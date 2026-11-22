from typing import List, Tuple
from core.task import SplittableTask


class ArithmeticHashTask(SplittableTask):
    """
    Concrete task representing our deterministic 3.2-billion-operation CPU benchmark.
    Models processing items using the inner loop: val = (val * 31 + 7) % 1000000007.
    """
    def __init__(
        self,
        task_id: str = None,
        parent_id: str = None,
        depth: int = 0,
        start_idx: int = 0,
        end_idx: int = 200000,
        partition_id: int = 0
    ):
        super().__init__(
            task_id=task_id,
            parent_id=parent_id,
            depth=depth,
            start_idx=start_idx,
            end_idx=end_idx,
            partition_id=partition_id
        )

    def split(self, boundaries: List[Tuple[int, int]]) -> List['SplittableTask']:
        children = []
        for i, (b_start, b_end) in enumerate(boundaries):
            if b_end > b_start:
                child = ArithmeticHashTask(
                    task_id=f"{self.task_id}_sub{i}",
                    parent_id=self.task_id,
                    depth=self.depth + 1,
                    start_idx=b_start,
                    end_idx=b_end,
                    partition_id=self.partition_id
                )
                children.append(child)
        return children

    def execute_slice(self, effective_cores: float, time_slice_sec: float, r_base: float) -> int:
        if self.completed_items >= self.total_work:
            return 0

        # Items processed in this time slice = time * effective_cores * rate
        # R_base = ~9,010 items / (core * sec)
        capacity_rate = effective_cores * r_base
        delta_items = int(round(capacity_rate * time_slice_sec))
        
        remaining = self.get_remaining_work()
        actual_delta = min(remaining, delta_items)
        self.completed_items += actual_delta
        return actual_delta
