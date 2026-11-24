from statistics import median
from typing import List, Optional, Tuple
from core.task import SplittableTask, TaskState
from policies.mitigation.base import MitigationStrategy
from policies.partitioners.capacity_weighted import CapacityWeightedPartitioner


class RecursiveSubdivisionStrategy(MitigationStrategy):
    """
    Recursive Dynamic Task Sub-Division (Multi-Round Hierarchical Work-Stealing):
    - Dynamic Recursion Depth: Does NOT stop at a fixed round count (e.g. Round 2).
      Continues subdividing sub-tasks of sub-tasks as long as idle healthy compute
      exists and remaining work exceeds the micro-task floor (2 * min_chunk_size).
    - Dynamic Health-Based Eviction: Compares the straggler node's effective CPU quota
      against the cluster median. If severely impaired (relative_health < threshold),
      the node is evicted from receiving new slices (0 items assigned). Otherwise,
      healthy slower nodes remain in the target pool with proportional slices.
    - Exact Work Conservation: Preempts parent at completed offset with 0.0s waste.
    """
    def __init__(
        self,
        tracker,
        min_chunk_size: int = 5000,
        health_eviction_threshold: float = 0.25,
        max_depth: Optional[int] = None
    ):
        super().__init__(tracker)
        self.min_chunk_size = min_chunk_size
        self.health_eviction_threshold = health_eviction_threshold
        self.max_depth = max_depth  # None = purely dynamic stopping via min_chunk_size
        self.partitioner = CapacityWeightedPartitioner()

    def _is_node_severely_impaired(self, straggler_node) -> bool:
        """
        Determines whether a node is crippled (e.g. C100 0.20c vs 1.93c peers)
        versus merely slower due to hardware diversity (e.g. D75 0.75c vs 1.50c median).
        """
        all_quotas = [n.effective_cpu_quota for n in self.tracker.nodes.values()]
        if not all_quotas:
            return False
        med_quota = median(all_quotas)
        if med_quota <= 0:
            return False
        relative_health = straggler_node.effective_cpu_quota / med_quota
        return relative_health < self.health_eviction_threshold

    def mitigate(self, straggler: SplittableTask) -> Tuple[bool, List[SplittableTask]]:
        # Optional safety cap if explicitly configured; otherwise dynamic stopping only
        if self.max_depth is not None and straggler.depth >= self.max_depth:
            return False, []

        # Gate 1: Check idle capacity across cluster
        total_idle_cores = self.tracker.get_total_idle_cores()
        idle_nodes = self.tracker.get_idle_nodes()
        if total_idle_cores < 1.0 or not idle_nodes:
            return False, []

        # Gate 2: Dynamic Work Granularity Floor (prevents micro-task thrashing)
        remaining_work = straggler.get_remaining_work()
        if remaining_work < 2 * self.min_chunk_size:
            return False, []

        straggler_node = self.tracker.nodes.get(straggler.assigned_node_id)
        if not straggler_node:
            return False, []

        # Gate 3: Dynamic Health-Based Eviction
        # If severely impaired (e.g., C100), evict straggler node from target pool.
        # If healthy/diverse (e.g., C0, C25, D50, D75), retain it with proportional share.
        if self._is_node_severely_impaired(straggler_node):
            target_nodes = list(idle_nodes)
        else:
            target_nodes = list(idle_nodes) + [straggler_node]

        if not target_nodes:
            return False, []

        # Calculate contiguous range splits starting from current completion point
        current_offset = straggler.start_idx + straggler.get_completed_work()
        ranges = self.partitioner.partition(
            start_offset=current_offset,
            total_remaining=remaining_work,
            nodes=target_nodes
        )

        if not ranges:
            return False, []

        # Freeze parent task at completed items (0 wasted compute)
        straggler.state = TaskState.SPLITTING

        # Create recursive child sub-tasks (depth = straggler.depth + 1)
        child_tasks = straggler.split(ranges)
        for child, target_node in zip(child_tasks, target_nodes):
            child.target_node_id = target_node.node_id

        return True, child_tasks
