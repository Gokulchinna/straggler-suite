from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class ClusterNode:
    """
    Physical representation of a cluster worker node.
    Explicitly tracks the effective Linux cgroups quota (c_effective) rather than nominal cores.
    """
    node_id: str
    effective_cpu_quota: float         # Linux cgroups CFS throttled limit (e.g. 1.93, 0.50, 0.20)
    nominal_cores: int = 4             # Nominal worker vCPUs / slots
    assigned_tasks: List[str] = field(default_factory=list)
    is_throttled: bool = False

    @property
    def is_idle(self) -> bool:
        return len(self.assigned_tasks) == 0

    @property
    def available_cores(self) -> float:
        return self.effective_cpu_quota if self.is_idle else 0.0
