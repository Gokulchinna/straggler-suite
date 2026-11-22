from abc import ABC, abstractmethod
from enum import Enum
from typing import List, Optional, Tuple
import uuid
import time


class TaskState(Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    STRAGGLING = "STRAGGLING"
    SPLITTING = "SPLITTING"
    COMPLETED = "COMPLETED"
    KILLED = "KILLED"


class SplittableTask(ABC):
    """
    Abstract Base Class establishing the contract for all schedulable and splittable work units.
    Implements Abstraction by hiding computational specifics from the scheduler.
    """
    def __init__(
        self,
        task_id: Optional[str] = None,
        parent_id: Optional[str] = None,
        depth: int = 0,
        start_idx: int = 0,
        end_idx: int = 200000,
        partition_id: int = 0
    ):
        self.task_id: str = task_id or str(uuid.uuid4())[:8]
        self.parent_id: Optional[str] = parent_id
        self.depth: int = depth
        self.start_idx: int = start_idx
        self.end_idx: int = end_idx
        self.partition_id: int = partition_id
        self.state: TaskState = TaskState.PENDING
        self.assigned_node_id: Optional[str] = None
        self.target_node_id: Optional[str] = None
        self.start_time: Optional[float] = None
        self.finish_time: Optional[float] = None
        self.completed_items: int = 0

    @property
    def total_work(self) -> int:
        return max(0, self.end_idx - self.start_idx)

    def get_total_work(self) -> int:
        return self.total_work

    def get_completed_work(self) -> int:
        return self.completed_items

    def get_remaining_work(self) -> int:
        return max(0, self.total_work - self.completed_items)

    def get_progress_fraction(self) -> float:
        total = self.total_work
        return (self.completed_items / total) if total > 0 else 0.0

    @abstractmethod
    def split(self, boundaries: List[Tuple[int, int]]) -> List['SplittableTask']:
        """
        Carves the uncompleted work range into discrete child SplittableTask instances.
        
        Args:
            boundaries: List of (start_idx, end_idx) ranges for each child.
            
        Returns:
            List of child SplittableTask instances with depth = self.depth + 1.
        """
        pass

    @abstractmethod
    def execute_slice(self, effective_cores: float, time_slice_sec: float, r_base: float) -> int:
        """
        Advances the computation for the duration of time_slice_sec given the CPU core quota.
        
        Returns:
            Number of items processed during this execution slice.
        """
        pass
