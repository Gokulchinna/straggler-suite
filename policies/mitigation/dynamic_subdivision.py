from typing import List, Tuple
from core.task import SplittableTask, TaskState
from policies.mitigation.base import MitigationStrategy
from policies.partitioners.capacity_weighted import CapacityWeightedPartitioner


class DynamicSubdivisionStrategy(MitigationStrategy):
    """
    Dynamic Task Sub-Division with Capacity-Aware Thresholding and Residual Slicing:
    - Gates splitting by idle compute (C_idle >= 1.0c) and minimum chunk size (W_rem >= 2 * W_min_chunk)
    - Retains a proportional slice on the original straggler worker (Residual Slicing)
    - Partitions remainder across idle workers weighted by effective cgroups quotas
    - Freezes completed parent work [start_idx, start_idx + completed) with 0.0s wasted compute
    """
    def __init__(self, tracker, min_chunk_size: int = 5000, max_depth: int = 3):
        super().__init__(tracker)
        self.min_chunk_size = min_chunk_size
        self.max_depth = max_depth
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

        # Gate 3: Check remaining work threshold
        remaining_work = straggler.get_remaining_work()
        if remaining_work < 2 * self.min_chunk_size:
            return False, []

        # Target pool: Idle nodes PLUS the original straggler node (Residual Slicing)
        straggler_node = self.tracker.nodes.get(straggler.assigned_node_id)
        if not straggler_node:
            return False, []

        target_nodes = idle_nodes + [straggler_node]

        # Calculate contiguous range splits starting from current completion point
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
