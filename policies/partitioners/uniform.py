from typing import List, Tuple
from core.node import ClusterNode
from policies.partitioners.base import WorkPartitioner


class UniformPartitioner(WorkPartitioner):
    """Splits remaining work into equal-sized chunks regardless of node capacity."""
    def partition(self, start_offset: int, total_remaining: int, nodes: List[ClusterNode]) -> List[Tuple[int, int]]:
        k = len(nodes)
        if k == 0 or total_remaining <= 0:
            return []

        base_chunk = total_remaining // k
        remainder = total_remaining % k
        ranges = []
        cursor = start_offset

        for i in range(k):
            chunk = base_chunk + (1 if i < remainder else 0)
            ranges.append((cursor, cursor + chunk))
            cursor += chunk

        return ranges
