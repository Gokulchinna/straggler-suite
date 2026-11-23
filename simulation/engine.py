import time
from typing import Dict, List, Optional, Tuple
from core.task import SplittableTask, TaskState
from core.node import ClusterNode
from core.cluster_state import ClusterResourceTracker
from simulation.workloads import ArithmeticHashTask
from simulation.metrics_collector import StageMetrics, MetricsCollector
from simulation.empirical_calibration import get_calibrated_cluster_parameters
from policies.detection.spark_median import SparkMedianDetector
from policies.detection.capacity_normalized import CapacityNormalizedDetector
from policies.mitigation.full_clone import FullCloneStrategy
from policies.mitigation.dynamic_subdivision import DynamicSubdivisionStrategy
from policies.mitigation.hybrid import AdaptiveHybridStrategy


class ClusterSimulator:
    """
    High-fidelity discrete-event simulator modeling Spark task execution on heterogeneous nodes.
    Supports Unmitigated, Spark Speculation, and Dynamic Sub-Division policies.
    """
    def __init__(
        self,
        node_quotas: List[float],
        num_partitions: int = 64,
        items_per_partition: int = 200000,
        calibrated_params: Optional[dict] = None
    ):
        self.node_quotas = node_quotas
        self.num_partitions = num_partitions
        self.items_per_partition = items_per_partition
        self.params = calibrated_params or get_calibrated_cluster_parameters()
        self.r_base = self.params.get("r_base", 9010.0)
        self.dt = self.params.get("time_step", 0.1)
        self.tau_dispatch = self.params.get("tau_dispatch", 0.035)
        self.tau_merge = self.params.get("tau_merge", 0.015)

    def run(self, mode: str, condition_label: str = "custom") -> StageMetrics:
        """
        Executes a single stage simulation run.
        Modes: 'UNMITIGATED', 'SPARK_SPECULATION', 'DYNAMIC_SUBDIVISION'
        """
        # 1. Initialize Nodes
        nodes = [
            ClusterNode(node_id=f"worker{i+1}", effective_cpu_quota=q)
            for i, q in enumerate(self.node_quotas)
        ]
        tracker = ClusterResourceTracker(nodes)

        # 2. Initialize Detectors & Strategies
        if mode == "SPARK_SPECULATION":
            detector = SparkMedianDetector(tracker)
            mitigation = FullCloneStrategy(tracker)
        elif mode == "DYNAMIC_SUBDIVISION":
            detector = CapacityNormalizedDetector(tracker, calibrated_r_base=self.r_base)
            mitigation = DynamicSubdivisionStrategy(tracker, min_chunk_size=5000, dead_node_threshold=0.35)
        elif mode == "HYBRID":
            detector = CapacityNormalizedDetector(tracker, calibrated_r_base=self.r_base)
            mitigation = AdaptiveHybridStrategy(tracker, min_chunk_size=5000, dead_node_threshold=0.35)
        else:
            detector = None
            mitigation = None

        # 3. Create 64 Partitions
        pending_queue: List[SplittableTask] = [
            ArithmeticHashTask(
                task_id=f"task_{p}",
                start_idx=0,
                end_idx=self.items_per_partition,
                partition_id=p
            )
            for p in range(self.num_partitions)
        ]

        active_tasks: List[SplittableTask] = []
        completed_partitions: set = set()
        partition_items_done: Dict[int, int] = {p: 0 for p in range(self.num_partitions)}

        # Speculation / cloning tracking
        clones_parent_map: Dict[str, str] = {}  # clone_id -> parent_id
        parent_clones_map: Dict[str, str] = {}  # parent_id -> clone_id

        # Metrics
        current_time = 0.0
        total_cpu_seconds = 0.0
        wasted_cpu_seconds = 0.0
        tasks_split_count = 0
        subtasks_created_count = 0
        gantt_records = []

        # Simulation Main Loop
        max_duration = 300.0  # safety timeout
        while len(completed_partitions) < self.num_partitions and current_time < max_duration:
            current_time += self.dt

            # Step A: Schedule pending tasks on idle nodes
            idle_nodes = tracker.get_idle_nodes()
            # Sort idle nodes: prioritize highest capacity nodes first
            idle_nodes.sort(key=lambda n: n.effective_cpu_quota, reverse=True)

            for node in idle_nodes:
                if not pending_queue:
                    break
                task = pending_queue.pop(0)
                task.state = TaskState.RUNNING
                task.assigned_node_id = node.node_id
                task.start_time = current_time
                tracker.assign_task(node.node_id, task.task_id)
                active_tasks.append(task)
                gantt_records.append({
                    "time": current_time,
                    "event": "START",
                    "task_id": task.task_id,
                    "partition_id": task.partition_id,
                    "node_id": node.node_id,
                    "range": (task.start_idx, task.end_idx)
                })

            # Step B: Advance execution for all active tasks
            finished_in_tick = []
            for task in list(active_tasks):
                node = tracker.nodes[task.assigned_node_id]
                effective_quota = node.effective_cpu_quota
                
                # Execute computation slice
                processed = task.execute_slice(effective_quota, self.dt, self.r_base)
                total_cpu_seconds += (effective_quota * self.dt)

                # Check task completion
                if task.completed_items >= task.total_work:
                    task.state = TaskState.COMPLETED
                    task.finish_time = current_time
                    finished_in_tick.append(task)

            # Step C: Handle completions & releases
            for task in finished_in_tick:
                active_tasks.remove(task)
                tracker.release_task(task.assigned_node_id, task.task_id)
                partition_items_done[task.partition_id] += task.total_work

                gantt_records.append({
                    "time": current_time,
                    "event": "COMPLETE",
                    "task_id": task.task_id,
                    "partition_id": task.partition_id,
                    "node_id": task.assigned_node_id,
                    "duration": current_time - task.start_time
                })

                # In Spark Speculation and Hybrid clone: kill slower copy when winner finishes
                if mode in ("SPARK_SPECULATION", "HYBRID"):
                    # If this was a clone that finished
                    if task.task_id in clones_parent_map:
                        parent_id = clones_parent_map[task.task_id]
                        parent_task = next((t for t in active_tasks if t.task_id == parent_id), None)
                        if parent_task:
                            # Kill parent, accumulate wasted compute
                            parent_task.state = TaskState.KILLED
                            active_tasks.remove(parent_task)
                            tracker.release_task(parent_task.assigned_node_id, parent_task.task_id)
                            p_node = tracker.nodes[parent_task.assigned_node_id]
                            wasted_time = current_time - parent_task.start_time
                            wasted_cpu_seconds += (p_node.effective_cpu_quota * wasted_time)
                            gantt_records.append({
                                "time": current_time,
                                "event": "KILLED_ORIGINAL",
                                "task_id": parent_task.task_id,
                                "partition_id": parent_task.partition_id,
                                "wasted_sec": wasted_time
                            })

                    # If this was a parent whose clone is still running
                    elif task.task_id in parent_clones_map:
                        clone_id = parent_clones_map[task.task_id]
                        clone_task = next((t for t in active_tasks if t.task_id == clone_id), None)
                        if clone_task:
                            clone_task.state = TaskState.KILLED
                            active_tasks.remove(clone_task)
                            tracker.release_task(clone_task.assigned_node_id, clone_task.task_id)
                            c_node = tracker.nodes[clone_task.assigned_node_id]
                            wasted_time = current_time - clone_task.start_time
                            wasted_cpu_seconds += (c_node.effective_cpu_quota * wasted_time)
                            gantt_records.append({
                                "time": current_time,
                                "event": "KILLED_CLONE",
                                "task_id": clone_task.task_id,
                                "partition_id": clone_task.partition_id,
                                "wasted_sec": wasted_time
                            })

                # Check if entire original 200,000 items of this partition are finished
                if partition_items_done[task.partition_id] >= self.items_per_partition:
                    completed_partitions.add(task.partition_id)

            # Step D: Straggler Detection & Mitigation
            if detector and mitigation:
                for task in list(active_tasks):
                    # Only check primary running tasks
                    if task.state != TaskState.RUNNING:
                        continue

                    if detector.is_straggler(task, active_tasks, current_time):
                        task.state = TaskState.STRAGGLING
                        action_taken, new_tasks = mitigation.mitigate(task)
                        
                        if action_taken and new_tasks:
                            is_clone = (mode == "SPARK_SPECULATION") or (mode == "HYBRID" and len(new_tasks) == 1 and "_clone" in new_tasks[0].task_id)

                            if is_clone:
                                # Add clone to pending or immediate dispatch
                                clone = new_tasks[0]
                                clones_parent_map[clone.task_id] = task.task_id
                                parent_clones_map[task.task_id] = clone.task_id
                                # Schedule clone on target idle node immediately
                                target_node = tracker.nodes[clone.target_node_id]
                                clone.state = TaskState.RUNNING
                                clone.assigned_node_id = target_node.node_id
                                clone.start_time = current_time + self.tau_dispatch
                                tracker.assign_task(target_node.node_id, clone.task_id)
                                active_tasks.append(clone)

                            elif mode in ("DYNAMIC_SUBDIVISION", "HYBRID"):
                                tasks_split_count += 1
                                subtasks_created_count += len(new_tasks)

                                # Release parent from active execution (it committed its progress)
                                active_tasks.remove(task)
                                tracker.release_task(task.assigned_node_id, task.task_id)
                                partition_items_done[task.partition_id] += task.get_completed_work()

                                gantt_records.append({
                                    "time": current_time,
                                    "event": "SPLIT",
                                    "parent_id": task.task_id,
                                    "partition_id": task.partition_id,
                                    "completed_work": task.get_completed_work(),
                                    "subtasks": len(new_tasks)
                                })

                                # Schedule all child sub-tasks on their target nodes
                                for child in new_tasks:
                                    target_node = tracker.nodes[child.target_node_id]
                                    child.state = TaskState.RUNNING
                                    child.assigned_node_id = target_node.node_id
                                    # Incorporate small RPC/thread setup latency
                                    child.start_time = current_time + self.tau_dispatch
                                    tracker.assign_task(target_node.node_id, child.task_id)
                                    active_tasks.append(child)

        # Stage finished
        metrics = StageMetrics(
            mode=mode,
            condition=condition_label,
            profile=self.node_quotas,
            jct_seconds=round(current_time, 2),
            total_tasks_completed=len(completed_partitions),
            tasks_split=tasks_split_count,
            subtasks_created=subtasks_created_count,
            total_cpu_seconds=round(total_cpu_seconds, 2),
            wasted_cpu_seconds=round(wasted_cpu_seconds, 2),
            gantt_records=gantt_records
        )
        return metrics
