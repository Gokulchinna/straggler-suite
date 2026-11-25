from statistics import median
from typing import Dict, List, Optional, Set, Tuple
from core.task import SplittableTask, TaskState
from policies.mitigation.base import MitigationStrategy
from policies.partitioners.capacity_weighted import CapacityWeightedPartitioner


class RecursiveSubdivisionV2Strategy(MitigationStrategy):
    """
    Option B — Recursive Sub-Division V2 (Proactive ETA Cutoff + Persistent Node Quarantine):
    - Dynamic Recursion Depth: Continues recursive sub-tasking as long as remaining work >= 2 * min_chunk_size.
    - Persistent Node Quarantine: Once a node's relative health drops below `health_eviction_threshold`,
      it is added to `quarantined_nodes` so NO future recursive slices (Round 1, 2, 3...) can be
      scheduled back onto it (prevents Slow-Node Ping-Pong).
    - ETA-Synchronized Cutoff: Supports `should_preempt_eta()` to prevent prematurely starving mild slow nodes
      while proactively evacuating crippled nodes before the final healthy wave ends.
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
        self.max_depth = max_depth
        self.partitioner = CapacityWeightedPartitioner()
        self.quarantined_nodes: Set[str] = set()

    def evaluate_node_health(self, node_id: str) -> bool:
        """
        Evaluates relative capacity against cluster median. Updates quarantine set if impaired.
        Returns True if the node is healthy (NOT quarantined), False if quarantined.
        """
        if node_id in self.quarantined_nodes:
            return False

        node = self.tracker.nodes.get(node_id)
        if not node:
            return False

        all_quotas = [n.effective_cpu_quota for n in self.tracker.nodes.values()]
        if not all_quotas:
            return True

        med_quota = median(all_quotas)
        if med_quota <= 0:
            return True

        relative_health = node.effective_cpu_quota / med_quota
        if relative_health < self.health_eviction_threshold:
            self.quarantined_nodes.add(node_id)
            return False
        return True

    def should_preempt_eta(
        self,
        task_remaining: int,
        task_rate: float,
        cluster_healthy_remaining_eta: float,
        stolen_eta: float,
        steal_overhead: float = 0.25
    ) -> bool:
        """
        ETA-Synchronized Cutoff Guard:
        Only preempt if remaining work exceeds micro-task floor AND the task will
        overshoot healthy cluster completion (or parallel stealing yields net speedup).
        """
        if task_remaining < 2 * self.min_chunk_size:
            return False
        if task_rate <= 0:
            return True

        task_eta = task_remaining / task_rate
        return task_eta > max(cluster_healthy_remaining_eta, stolen_eta + steal_overhead)

    def mitigate(self, straggler: SplittableTask) -> Tuple[bool, List[SplittableTask]]:
        if self.max_depth is not None and straggler.depth >= self.max_depth:
            return False, []

        # Gate 1: Healthy idle nodes must exist (exclude any quarantined nodes)
        idle_nodes = [
            n for n in self.tracker.get_idle_nodes()
            if n.node_id not in self.quarantined_nodes
        ]
        total_healthy_idle_cores = sum(n.available_cores for n in idle_nodes)
        if total_healthy_idle_cores < 1.0 or not idle_nodes:
            return False, []

        # Gate 2: Dynamic Work Granularity Floor
        remaining_work = straggler.get_remaining_work()
        if remaining_work < 2 * self.min_chunk_size:
            return False, []

        straggler_node = self.tracker.nodes.get(straggler.assigned_node_id)
        if not straggler_node:
            return False, []

        # Gate 3: Persistent Health Check & Node Quarantine
        is_healthy = self.evaluate_node_health(straggler_node.node_id)
        if is_healthy:
            target_nodes = list(idle_nodes) + [straggler_node]
        else:
            # Strict quarantine: zero slices returned to impaired host
            target_nodes = list(idle_nodes)

        if not target_nodes:
            return False, []

        current_offset = straggler.start_idx + straggler.get_completed_work()
        ranges = self.partitioner.partition(
            start_offset=current_offset,
            total_remaining=remaining_work,
            nodes=target_nodes
        )

        if not ranges:
            return False, []

        straggler.state = TaskState.SPLITTING
        child_tasks = straggler.split(ranges)
        for child, target_node in zip(child_tasks, target_nodes):
            child.target_node_id = target_node.node_id

        return True, child_tasks
