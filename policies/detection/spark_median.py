from typing import List
from core.task import SplittableTask
from policies.detection.base import StragglerDetector


class SparkMedianDetector(StragglerDetector):
    """
    Standard Apache Spark Speculation heuristic:
    Flags a task if its elapsed duration exceeds multiplier * median duration of successful tasks.
    """
    def __init__(self, tracker, multiplier: float = 1.5):
        super().__init__(tracker)
        self.multiplier = multiplier

    def is_straggler(self, task: SplittableTask, active_tasks: List[SplittableTask], current_time: float) -> bool:
        if task.get_progress_fraction() < 0.20 or task.start_time is None:
            return False

        elapsed = max(0.1, current_time - task.start_time)
        durations = []
        for t in active_tasks:
            if t.task_id != task.task_id and t.start_time is not None and t.get_progress_fraction() >= 0.20:
                durations.append(max(0.1, current_time - t.start_time))

        if not durations:
            return False

        durations.sort()
        median_dur = durations[len(durations) // 2]
        return elapsed > (self.multiplier * median_dur)
