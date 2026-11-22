from abc import ABC, abstractmethod
from typing import List, Tuple
from core.task import SplittableTask
from core.cluster_state import ClusterResourceTracker


class MitigationStrategy(ABC):
    """
    Abstract Base Class for straggler mitigation policies.
    Enables Polymorphism: Schedulers interact with any strategy uniformly via mitigate().
    """
    def __init__(self, tracker: ClusterResourceTracker):
        self.tracker = tracker

    @abstractmethod
    def mitigate(self, straggler: SplittableTask) -> Tuple[bool, List[SplittableTask]]:
        """
        Executes mitigation logic.
        
        Returns:
            Tuple of (action_taken: bool, new_tasks_to_schedule: List[SplittableTask])
        """
        pass
