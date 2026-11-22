from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class StageMetrics:
    mode: str
    condition: str
    profile: List[float]
    jct_seconds: float
    total_tasks_completed: int
    tasks_split: int
    subtasks_created: int
    total_cpu_seconds: float
    wasted_cpu_seconds: float
    gantt_records: List[dict] = field(default_factory=list)


class MetricsCollector:
    """Collects fine-grained execution metrics, Gantt timelines, and compute accounting."""
    def __init__(self):
        self.stage_results: List[StageMetrics] = []

    def record_stage(
        self,
        mode: str,
        condition: str,
        profile: List[float],
        jct_seconds: float,
        tasks_completed: int,
        tasks_split: int,
        subtasks_created: int,
        total_cpu_sec: float,
        wasted_cpu_sec: float,
        gantt_records: Optional[List[dict]] = None
    ) -> StageMetrics:
        m = StageMetrics(
            mode=mode,
            condition=condition,
            profile=profile,
            jct_seconds=round(jct_seconds, 2),
            total_tasks_completed=tasks_completed,
            tasks_split=tasks_split,
            subtasks_created=subtasks_created,
            total_cpu_seconds=round(total_cpu_sec, 2),
            wasted_cpu_seconds=round(wasted_cpu_sec, 2),
            gantt_records=gantt_records or []
        )
        self.stage_results.append(m)
        return m
