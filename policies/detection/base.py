from abc import ABC, abstractmethod
from typing import List
from core.task import SplittableTask
from core.cluster_state import ClusterResourceTracker


class StragglerDetector(ABC):
    """Abstract interface for straggler detection policies."""
    def __init__(self, tracker: ClusterResourceTracker):
        self.tracker = tracker

    @abstractmethod
    def is_straggler(self, task: SplittableTask, active_tasks: List[SplittableTask], current_time: float) -> bool:
        pass
