from abc import ABC, abstractmethod
from typing import List, Tuple
from core.node import ClusterNode


class WorkPartitioner(ABC):
    """Abstract interface for partitioning uncompleted work among nodes."""
    @abstractmethod
    def partition(self, start_offset: int, total_remaining: int, nodes: List[ClusterNode]) -> List[Tuple[int, int]]:
        pass
