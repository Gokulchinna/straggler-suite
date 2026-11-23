from typing import List, Tuple
from core.task import SplittableTask, TaskState
from policies.mitigation.base import MitigationStrategy
from policies.partitioners.capacity_weighted import CapacityWeightedPartitioner


class AdaptiveHybridStrategy(MitigationStrategy):
    """
    Adaptive Hybrid Mitigation Policy:
    Unifies Dynamic Task Sub-Division and Selective Full-Cloning to eliminate the
    systems trade-off between severe single-node failure (C100) and multi-tier
    hardware diversity (D50-D100).

    Behavioral Modes:
    1. Dead-Node / Severe Impairment (C_effective < dead_node_threshold, e.g. < 0.35c):
       - If task progress < min_progress_to_slice (e.g. 20%): Employs Full-Clone to
         the highest-capacity idle node, immediately abandoning the dead worker.
       - If task progress >= min_progress_to_slice: Commits finished parent work and
         subdivides 100% of the residual uncompleted work across idle fast workers,
         completely evicting the dead node from the residual pool.
    2. Operational Heterogeneity (C_effective >= dead_node_threshold):
       - Employs Capacity-Weighted Dynamic Sub-Division with Residual Slicing,
         retaining a proportional slice on the straggler worker.
    3. Homogeneous / Low Skew Clusters:
       - Dormant mode: prevents coordination overhead and spurious splits.
    """
    def __init__(
        self,
        tracker,
        min_chunk_size: int = 5000,
        max_depth: int = 3,
        dead_node_threshold: float = 0.35,
        min_progress_to_slice: float = 0.20
    ):
        super().__init__(tracker)
        self.min_chunk_size = min_chunk_size
        self.max_depth = max_depth
        self.dead_node_threshold = dead_node_threshold
        self.min_progress_to_slice = min_progress_to_slice
        self.partitioner = CapacityWeightedPartitioner()

    def mitigate(self, straggler: SplittableTask) -> Tuple[bool, List[SplittableTask]]:
        # Gate 1: Check recursion depth limit
        if straggler.depth >= self.max_depth:
            return False, []

        # Gate 2: Check idle capacity across cluster
        total_idle_cores = self.tracker.get_total_idle_cores()
        idle_nodes = self.tracker.get_idle_nodes()
        if total_idle_cores < 1.0 or not idle_nodes:
            return False, []

        # Sort idle nodes by highest capacity first
        idle_nodes.sort(key=lambda n: n.effective_cpu_quota, reverse=True)

        straggler_node = self.tracker.nodes.get(straggler.assigned_node_id)
        if not straggler_node:
            return False, []

        is_dead_node = straggler_node.effective_cpu_quota < self.dead_node_threshold
        progress = straggler.get_progress_fraction()
        remaining_work = straggler.get_remaining_work()

        # Mode 1A: Dead Node with minimal progress -> Full Clone & Evict
        if is_dead_node and progress < self.min_progress_to_slice:
            chosen_node = idle_nodes[0]
            clones = straggler.split([(straggler.start_idx, straggler.end_idx)])
            if not clones:
                return False, []
            clone = clones[0]
            clone.task_id = f"{straggler.task_id}_hybrid_clone"
            clone.target_node_id = chosen_node.node_id
            return True, [clone]

        # Gate 3: Check remaining work threshold for sub-division
        if remaining_work < 2 * self.min_chunk_size:
            return False, []

        # Mode 1B & Mode 2: Dynamic Sub-Division
        if is_dead_node:
            # Evict dead node from residual pool; fast idle nodes absorb all remaining work
            target_nodes = list(idle_nodes)
        else:
            # Operational heterogeneity: retain residual slice on straggler
            target_nodes = idle_nodes + [straggler_node]

        current_offset = straggler.start_idx + straggler.get_completed_work()
        ranges = self.partitioner.partition(
            start_offset=current_offset,
            total_remaining=remaining_work,
            nodes=target_nodes
        )

        if not ranges:
            return False, []

        # Commit parent: Freeze parent task at completed items
        straggler.state = TaskState.SPLITTING

        # Create child sub-tasks for each slice
        child_tasks = straggler.split(ranges)

        # Assign corresponding target nodes
        for child, target_node in zip(child_tasks, target_nodes):
            child.target_node_id = target_node.node_id

        return True, child_tasks
