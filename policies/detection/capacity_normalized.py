from typing import List
from core.task import SplittableTask
from policies.detection.base import StragglerDetector


class CapacityNormalizedDetector(StragglerDetector):
    """
    Capacity-Normalized Progress Rate & Tail-Blocker Detector.
    Normalizes progress rates by assigned worker effective CPU quota:
        R_norm = items_completed / (elapsed * c_effective)
    Triggers when:
        1. Task is underperforming its hardware tier (norm_rate < threshold * reference_rate), OR
        2. Task is a stage-blocking straggler holding up idle cluster capacity.
    Includes fallback to calibrated baseline R_base when few or zero peer tasks remain active (Tail Straggler).
    """
    def __init__(self, tracker, calibrated_r_base: float = 9010.0, rate_ratio_threshold: float = 0.60):
        super().__init__(tracker)
        self.calibrated_r_base = calibrated_r_base
        self.rate_ratio_threshold = rate_ratio_threshold

    def is_straggler(self, task: SplittableTask, active_tasks: List[SplittableTask], current_time: float) -> bool:
        # Minimum progress gate: 20% to avoid transient JVM / thread warmup noise
        if task.get_progress_fraction() < 0.20 or task.start_time is None:
            return False

        elapsed = max(0.1, current_time - task.start_time)
        assigned_node = self.tracker.nodes.get(task.assigned_node_id)
        if not assigned_node:
            return False

        assigned_quota = assigned_node.effective_cpu_quota
        # Items / (sec * core)
        norm_rate = (task.get_completed_work() / elapsed) / assigned_quota

        # Extract active peer tasks running with >= 20% progress
        active_norm_rates = []
        for t in active_tasks:
            if t.task_id != task.task_id and t.start_time is not None and t.get_progress_fraction() >= 0.20:
                t_node = self.tracker.nodes.get(t.assigned_node_id)
                if t_node:
                    t_elapsed = max(0.1, current_time - t.start_time)
                    rate = (t.get_completed_work() / t_elapsed) / t_node.effective_cpu_quota
                    active_norm_rates.append(rate)

        # Refinement: Fallback to calibrated R_base if no peer tasks remain (Tail Straggler)
        if not active_norm_rates:
            reference_rate = self.calibrated_r_base
        else:
            active_norm_rates.sort()
            reference_rate = active_norm_rates[len(active_norm_rates) // 2]

        # Check 1: Hardware-relative degradation (e.g. noisy neighbor contention / stall)
        if norm_rate < (self.rate_ratio_threshold * reference_rate):
            return True

        # Check 2: Stage-blocking tail straggler (idle cores waiting while slow node crawls)
        total_idle_cores = self.tracker.get_total_idle_cores()
        remaining_work = task.get_remaining_work()
        if total_idle_cores >= 1.0 and remaining_work >= 10000:
            time_alone = remaining_work / (assigned_quota * self.calibrated_r_base)
            time_shared = remaining_work / ((assigned_quota + total_idle_cores) * self.calibrated_r_base)
            if time_alone > (1.5 * time_shared):
                return True

        return False
