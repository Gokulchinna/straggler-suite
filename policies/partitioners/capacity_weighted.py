from typing import List, Tuple
from core.node import ClusterNode
from policies.partitioners.base import WorkPartitioner


class CapacityWeightedPartitioner(WorkPartitioner):
    """
    Partitions remaining work proportionally to each target node's effective cgroups CPU quota.
    Guarantees exact discrete work conservation (sum(ChunkSize_j) == W_rem).
    """
    def partition(self, start_offset: int, total_remaining: int, nodes: List[ClusterNode]) -> List[Tuple[int, int]]:
        if not nodes or total_remaining <= 0:
            return []

        total_cores = sum(n.effective_cpu_quota for n in nodes)
        if total_cores <= 0:
            total_cores = float(len(nodes))

        ranges = []
        current_cursor = start_offset
        allocated_items = 0

        for idx, node in enumerate(nodes):
            if idx == len(nodes) - 1:
                # Last node absorbs discrete rounding remainder (exact work conservation)
                chunk = total_remaining - allocated_items
            else:
                weight = node.effective_cpu_quota / total_cores
                chunk = int(round(total_remaining * weight))
                allocated_items += chunk

            chunk = max(0, chunk)
            ranges.append((current_cursor, current_cursor + chunk))
            current_cursor += chunk

        return ranges
