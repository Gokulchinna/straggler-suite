from dataclasses import dataclass
from typing import Optional


@dataclass
class SchedEvent:
    """Represents a discrete scheduler event for tracing and audit logging."""
    timestamp: float
    event_type: str        # TASK_START, TASK_PROGRESS, TASK_STRAGGLE, TASK_SPLIT, TASK_COMPLETE, TASK_KILLED
    task_id: str
    node_id: str
    partition_id: int
    details: str = ""
