from typing import List, Tuple
from core.task import SplittableTask, TaskState
from policies.mitigation.base import MitigationStrategy
from policies.partitioners.capacity_weighted import CapacityWeightedPartitioner


class RacingTeamSpeculationStrategy(MitigationStrategy):
    """
    Racing Team Speculation (Parallel Team Speculation):
    - Original straggler task continues executing uninterrupted (does NOT stop, does NOT get a slice).
    - Target pool consists of IDLE WORKERS ONLY (strictly excludes the straggler node).
    - The remaining uncompleted work [start_idx + completed, end_idx) is partitioned
      exclusively across the idle workers proportional to their effective CPU quotas.
    - Slices form a parallel "Rescue Team" racing against the original worker.
    - Whichever side finishes first cancels/kills the other side!
    """
    def __init__(self, tracker, min_chunk_size: int = 5000, max_depth: int = 2):
        super().__init__(tracker)
        self.min_chunk_size = min_chunk_size
        self.max_depth = max_depth
        self.partitioner = CapacityWeightedPartitioner()

    def mitigate(self, straggler: SplittableTask) -> Tuple[bool, List[SplittableTask]]:
        # Gate 1: Check recursion depth limit
        if straggler.depth >= self.max_depth:
            return False, []

        # Gate 2: Check idle capacity across cluster (at least 1 idle node with >= 1.0 core)
        total_idle_cores = self.tracker.get_total_idle_cores()
        idle_nodes = self.tracker.get_idle_nodes()
        if total_idle_cores < 1.0 or not idle_nodes:
            return False, []

        # Gate 3: Check remaining work threshold
        remaining_work = straggler.get_remaining_work()
        if remaining_work < 2 * self.min_chunk_size:
            return False, []

        # Target pool: IDLE NODES ONLY! Exclude the straggler node completely!
        target_nodes = list(idle_nodes)
        target_nodes.sort(key=lambda n: n.effective_cpu_quota, reverse=True)

        current_offset = straggler.start_idx + straggler.get_completed_work()
        ranges = self.partitioner.partition(
            start_offset=current_offset,
            total_remaining=remaining_work,
            nodes=target_nodes
        )

        if not ranges:
            return False, []

        # Notice: Original straggler stays RUNNING! It is NOT frozen or marked SPLITTING.
        # It continues its original execution.

        # Create child rescue slices
        child_tasks = straggler.split(ranges)
        for child, target_node in zip(child_tasks, target_nodes):
            child.target_node_id = target_node.node_id
            child.task_id = f"{straggler.task_id}_team_{target_node.node_id}"

        return True, child_tasks
