from typing import List, Tuple
from core.task import SplittableTask, TaskState
from policies.mitigation.base import MitigationStrategy


class FullCloneStrategy(MitigationStrategy):
    """
    Standard Spark Speculation:
    Clones the entire task from scratch (item 0 to end) to 1 idle node.
    """
    def mitigate(self, straggler: SplittableTask) -> Tuple[bool, List[SplittableTask]]:
        idle_nodes = self.tracker.get_idle_nodes()
        if not idle_nodes:
            return False, []

        # Sort idle nodes by highest capacity first
        idle_nodes.sort(key=lambda n: n.effective_cpu_quota, reverse=True)
        chosen_node = idle_nodes[0]

        # Monolithic clone starting from item 0
        clones = straggler.split([(straggler.start_idx, straggler.end_idx)])
        if not clones:
            return False, []

        clone = clones[0]
        clone.task_id = f"{straggler.task_id}_clone"
        clone.target_node_id = chosen_node.node_id
        return True, [clone]
