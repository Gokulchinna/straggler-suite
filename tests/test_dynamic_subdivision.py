import unittest
from core.node import ClusterNode
from core.cluster_state import ClusterResourceTracker
from policies.partitioners.capacity_weighted import CapacityWeightedPartitioner
from policies.detection.capacity_normalized import CapacityNormalizedDetector
from policies.mitigation.dynamic_subdivision import DynamicSubdivisionStrategy
from policies.mitigation.hybrid import AdaptiveHybridStrategy
from simulation.workloads import ArithmeticHashTask
from simulation.engine import ClusterSimulator


class TestDynamicSubdivision(unittest.TestCase):
    def setUp(self):
        # Setup heterogeneous 4-node cluster (D75 profile)
        self.nodes = [
            ClusterNode(node_id="worker1", effective_cpu_quota=2.25),
            ClusterNode(node_id="worker2", effective_cpu_quota=1.875),
            ClusterNode(node_id="worker3", effective_cpu_quota=1.125),
            ClusterNode(node_id="worker4", effective_cpu_quota=0.75),
        ]
        self.tracker = ClusterResourceTracker(self.nodes)

    def test_exact_work_conservation(self):
        """Theorem 1: Discrete chunk allocations must strictly sum to total remaining work."""
        partitioner = CapacityWeightedPartitioner()
        remaining_work = 150000
        start_offset = 50000

        ranges = partitioner.partition(start_offset, remaining_work, self.nodes)
        self.assertEqual(len(ranges), 4)

        # Check total partitioned items
        total_partitioned = sum(end - start for start, end in ranges)
        self.assertEqual(total_partitioned, remaining_work)

        # Check contiguous boundaries
        for i in range(len(ranges) - 1):
            self.assertEqual(ranges[i][1], ranges[i+1][0])
        self.assertEqual(ranges[-1][1], start_offset + remaining_work)

    def test_capacity_weighted_proportionality(self):
        """Worker with 3x cores must receive approximately 3x larger work slice."""
        partitioner = CapacityWeightedPartitioner()
        remaining_work = 100000
        ranges = partitioner.partition(0, remaining_work, self.nodes)

        chunk_w1 = ranges[0][1] - ranges[0][0]  # 2.25c
        chunk_w4 = ranges[3][1] - ranges[3][0]  # 0.75c

        # Ratio should be 2.25 / 0.75 = 3.0
        ratio = chunk_w1 / chunk_w4
        self.assertAlmostEqual(ratio, 3.0, delta=0.05)

    def test_dynamic_gating_inhibits_splitting_when_no_idle_capacity(self):
        """If cluster is 100% busy (C_idle < 1.0), mitigation must NOT trigger."""
        strategy = DynamicSubdivisionStrategy(self.tracker)
        # Mark all nodes as busy
        for n in self.nodes:
            self.tracker.assign_task(n.node_id, f"dummy_task_{n.node_id}")

        task = ArithmeticHashTask(task_id="straggler1", start_idx=0, end_idx=200000)
        task.assigned_node_id = "worker4"
        task.completed_items = 40000

        action_taken, child_tasks = strategy.mitigate(task)
        self.assertFalse(action_taken)
        self.assertEqual(len(child_tasks), 0)

    def test_residual_slicing_includes_straggler_node(self):
        """Target pool must include idle nodes PLUS the original straggler worker."""
        strategy = DynamicSubdivisionStrategy(self.tracker)
        # Only worker 4 is busy with the straggler; workers 1-3 are idle
        self.tracker.assign_task("worker4", "straggler1")

        task = ArithmeticHashTask(task_id="straggler1", start_idx=0, end_idx=200000)
        task.assigned_node_id = "worker4"
        task.completed_items = 50000

        action_taken, child_tasks = strategy.mitigate(task)
        self.assertTrue(action_taken)
        # Target pool: 3 idle nodes + 1 straggler node = 4 children
        self.assertEqual(len(child_tasks), 4)

        # Confirm worker4 is assigned a slice
        assigned_targets = [c.target_node_id for c in child_tasks]
        self.assertIn("worker4", assigned_targets)
        self.assertIn("worker1", assigned_targets)

    def test_tail_straggler_detection_fallback(self):
        """When no peer tasks remain active, detector must use calibrated R_base fallback."""
        detector = CapacityNormalizedDetector(self.tracker, calibrated_r_base=9010.0, rate_ratio_threshold=0.60)
        task = ArithmeticHashTask(task_id="tail_task", start_idx=0, end_idx=200000)
        task.assigned_node_id = "worker4"  # 0.75c
        task.start_time = 0.0
        # Progress = 30%, but took 100 seconds (very slow: 600 items/sec vs expected ~6750)
        task.completed_items = 60000

        # Empty active_tasks list (it is the sole survivor)
        is_strag = detector.is_straggler(task, active_tasks=[], current_time=100.0)
        self.assertTrue(is_strag)

    def test_dead_node_eviction_threshold(self):
        """Worker throttled below dead_node_threshold (e.g. 0.20c) must NOT receive a residual slice."""
        c100_nodes = [
            ClusterNode(node_id="worker1", effective_cpu_quota=1.93),
            ClusterNode(node_id="worker2", effective_cpu_quota=1.93),
            ClusterNode(node_id="worker3", effective_cpu_quota=1.93),
            ClusterNode(node_id="worker4", effective_cpu_quota=0.20),
        ]
        tracker = ClusterResourceTracker(c100_nodes)
        strategy = DynamicSubdivisionStrategy(tracker, dead_node_threshold=0.35)

        # Worker 4 is busy with straggler, workers 1-3 are idle
        tracker.assign_task("worker4", "choked_task")
        task = ArithmeticHashTask(task_id="choked_task", start_idx=0, end_idx=200000)
        task.assigned_node_id = "worker4"
        task.completed_items = 60000

        action_taken, child_tasks = strategy.mitigate(task)
        self.assertTrue(action_taken)
        assigned_targets = [c.target_node_id for c in child_tasks]

        # Worker 4 (0.20c) must be evicted from the target pool
        self.assertNotIn("worker4", assigned_targets)
        # All 3 fast idle workers must receive slices
        self.assertEqual(len(child_tasks), 3)
        self.assertIn("worker1", assigned_targets)
        self.assertIn("worker2", assigned_targets)
        self.assertIn("worker3", assigned_targets)

    def test_homogeneous_cluster_dormancy(self):
        """On a balanced cluster (spread < 0.15c) with nominal progress, detector stays dormant."""
        homo_nodes = [
            ClusterNode(node_id=f"worker{i+1}", effective_cpu_quota=1.50)
            for i in range(4)
        ]
        tracker = ClusterResourceTracker(homo_nodes)
        detector = CapacityNormalizedDetector(tracker, cluster_skew_threshold=0.15)

        task = ArithmeticHashTask(task_id="task_norm", start_idx=0, end_idx=200000)
        task.assigned_node_id = "worker1"
        task.start_time = 0.0
        # Normal expected rate: 1.50 * 26733.5 = ~40,000 items/sec.
        # At 3 seconds, 100,000 items is 33,333 items/sec (> 50% nominal).
        task.completed_items = 100000

        is_strag = detector.is_straggler(task, active_tasks=[], current_time=3.0)
        self.assertFalse(is_strag)

    def test_adaptive_hybrid_strategy_modes(self):
        """Hybrid strategy must clone on dead node with low progress, and subdivide with high progress."""
        c100_nodes = [
            ClusterNode(node_id="worker1", effective_cpu_quota=1.93),
            ClusterNode(node_id="worker2", effective_cpu_quota=1.93),
            ClusterNode(node_id="worker3", effective_cpu_quota=1.93),
            ClusterNode(node_id="worker4", effective_cpu_quota=0.20),
        ]
        tracker = ClusterResourceTracker(c100_nodes)
        hybrid = AdaptiveHybridStrategy(tracker, dead_node_threshold=0.35, min_progress_to_slice=0.20)

        # Case A: Dead node with low progress (< 20%) -> Full Clone
        tracker.assign_task("worker4", "task_early")
        task_early = ArithmeticHashTask(task_id="task_early", start_idx=0, end_idx=200000)
        task_early.assigned_node_id = "worker4"
        task_early.completed_items = 20000  # 10% progress

        action, tasks = hybrid.mitigate(task_early)
        self.assertTrue(action)
        self.assertEqual(len(tasks), 1)
        self.assertTrue(tasks[0].task_id.endswith("_hybrid_clone"))
        self.assertEqual(tasks[0].target_node_id, "worker1")

        # Case B: Dead node with adequate progress (>= 20%) -> Subdivide without dead node
        tracker.assign_task("worker4", "task_late")
        task_late = ArithmeticHashTask(task_id="task_late", start_idx=0, end_idx=200000)
        task_late.assigned_node_id = "worker4"
        task_late.completed_items = 60000  # 30% progress

        action_b, tasks_b = hybrid.mitigate(task_late)
        self.assertTrue(action_b)
        self.assertEqual(len(tasks_b), 3)  # Only workers 1, 2, 3
        self.assertNotIn("worker4", [t.target_node_id for t in tasks_b])

    def test_simulation_c100_speedup_and_zero_waste(self):
        """In C100, Dynamic Sub-Division must beat Unmitigated with 0.0s wasted compute."""
        c100_profile = [1.93, 1.93, 1.93, 0.20]
        sim = ClusterSimulator(node_quotas=c100_profile, num_partitions=16, items_per_partition=50000)

        unmitigated_res = sim.run("UNMITIGATED", "C100")
        dynamic_res = sim.run("DYNAMIC_SUBDIVISION", "C100")

        # Dynamic sub-division must be faster
        self.assertLess(dynamic_res.jct_seconds, unmitigated_res.jct_seconds)
        # Compute wasted must be exactly 0.0s
        self.assertEqual(dynamic_res.wasted_cpu_seconds, 0.0)


if __name__ == "__main__":
    unittest.main()
