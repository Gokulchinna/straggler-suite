from typing import Dict, List, Optional
from core.node import ClusterNode


class ClusterResourceTracker:
    """
    Authoritative state tracker for all cluster nodes and running tasks.
    Provides snapshot queries for dynamic threshold decision-making and capacity weighting.
    """
    def __init__(self, nodes: List[ClusterNode]):
        self.nodes: Dict[str, ClusterNode] = {n.node_id: n for n in nodes}

    def get_idle_nodes(self) -> List[ClusterNode]:
        """Returns all nodes currently executing zero active tasks."""
        return [n for n in self.nodes.values() if n.is_idle]

    def get_total_idle_cores(self) -> float:
        """Calculates aggregate idle compute capacity sum(C_idle) across cluster."""
        return sum(n.available_cores for n in self.nodes.values())

    def get_total_cluster_cores(self) -> float:
        """Returns total cluster compute capacity (conserved invariant: 6.0 cores)."""
        return sum(n.effective_cpu_quota for n in self.nodes.values())

    def assign_task(self, node_id: str, task_id: str) -> None:
        if node_id in self.nodes:
            self.nodes[node_id].assigned_tasks.append(task_id)

    def release_task(self, node_id: str, task_id: str) -> None:
        if node_id in self.nodes and task_id in self.nodes[node_id].assigned_tasks:
            self.nodes[node_id].assigned_tasks.remove(task_id)
