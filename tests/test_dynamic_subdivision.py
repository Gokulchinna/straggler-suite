import unittest
from core.node import ClusterNode
from core.cluster_state import ClusterResourceTracker
from policies.partitioners.capacity_weighted import CapacityWeightedPartitioner
from policies.detection.capacity_normalized import CapacityNormalizedDetector
from policies.mitigation.dynamic_subdivision import DynamicSubdivisionStrategy
from policies.mitigation.racing_team import RacingTeamSpeculationStrategy
from policies.mitigation.recursive_subdivision import RecursiveSubdivisionStrategy
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

    def test_racing_team_target_pool_excludes_straggler(self):
        """In Racing Team, the target pool must strictly exclude the slow straggler worker."""
        racing_strategy = RacingTeamSpeculationStrategy(self.tracker)
        # Only worker 4 is busy with the straggler; workers 1-3 are idle
        self.tracker.assign_task("worker4", "straggler1")

        task = ArithmeticHashTask(task_id="straggler1", start_idx=0, end_idx=200000)
        task.assigned_node_id = "worker4"
        task.completed_items = 50000

        action_taken, child_tasks = racing_strategy.mitigate(task)
        self.assertTrue(action_taken)
        # Target pool: strictly 3 idle nodes (Workers 1, 2, 3), Worker 4 excluded!
        self.assertEqual(len(child_tasks), 3)

        assigned_targets = [c.target_node_id for c in child_tasks]
        self.assertNotIn("worker4", assigned_targets)
        self.assertIn("worker1", assigned_targets)
        self.assertIn("worker2", assigned_targets)
        self.assertIn("worker3", assigned_targets)

        # Work conservation across idle workers
        total_stolen = sum(c.total_work for c in child_tasks)
        self.assertEqual(total_stolen, 150000)

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

    def test_simulation_racing_team_c100(self):
        """In C100, Racing Team must beat Unmitigated by rescuing the slow worker."""
        c100_profile = [1.93, 1.93, 1.93, 0.20]
        sim = ClusterSimulator(node_quotas=c100_profile, num_partitions=16, items_per_partition=50000)

        unmitigated_res = sim.run("UNMITIGATED", "C100")
        racing_res = sim.run("RACING_TEAM", "C100")

        # Racing Team must be faster than unmitigated
        self.assertLess(racing_res.jct_seconds, unmitigated_res.jct_seconds)
        self.assertGreater(racing_res.subtasks_created, 0)

    def test_recursive_subdivision_no_eviction_on_healthy_diversity(self):
        """In D75 (Worker 4 = 0.75c vs 1.50c median = 50% health), Worker 4 must NOT be evicted."""
        strategy = RecursiveSubdivisionStrategy(self.tracker, min_chunk_size=5000, health_eviction_threshold=0.25)
        self.tracker.assign_task("worker4", "straggler1")

        task = ArithmeticHashTask(task_id="straggler1", start_idx=0, end_idx=200000)
        task.assigned_node_id = "worker4"
        task.completed_items = 50000

        action_taken, child_tasks = strategy.mitigate(task)
        self.assertTrue(action_taken)
        self.assertEqual(len(child_tasks), 4)
        self.assertIn("worker4", [c.target_node_id for c in child_tasks])

    def test_recursive_subdivision_evicts_crippled_node(self):
        """In C100 (Worker 4 = 0.20c vs 1.93c median = 10.3% health), Worker 4 MUST be evicted."""
        c100_nodes = [
            ClusterNode(node_id="worker1", effective_cpu_quota=1.93),
            ClusterNode(node_id="worker2", effective_cpu_quota=1.93),
            ClusterNode(node_id="worker3", effective_cpu_quota=1.93),
            ClusterNode(node_id="worker4", effective_cpu_quota=0.20),
        ]
        tracker_c100 = ClusterResourceTracker(c100_nodes)
        tracker_c100.assign_task("worker4", "straggler_c100")

        strategy = RecursiveSubdivisionStrategy(tracker_c100, min_chunk_size=5000, health_eviction_threshold=0.25)
        task = ArithmeticHashTask(task_id="straggler_c100", start_idx=0, end_idx=200000)
        task.assigned_node_id = "worker4"
        task.completed_items = 20000

        action_taken, child_tasks = strategy.mitigate(task)
        self.assertTrue(action_taken)
        # Worker 4 evicted -> strictly 3 healthy idle nodes receive slices
        self.assertEqual(len(child_tasks), 3)
        self.assertNotIn("worker4", [c.target_node_id for c in child_tasks])
        self.assertEqual(sum(c.total_work for c in child_tasks), 180000)

    def test_recursive_subdivision_allows_multi_round_depth_and_stops_dynamically(self):
        """Must not hard-stop at Round 2; must recurse at depth >= 2 and stop when remaining < 2*min_chunk."""
        strategy = RecursiveSubdivisionStrategy(self.tracker, min_chunk_size=5000, max_depth=None)
        self.tracker.assign_task("worker4", "deep_subtask")

        # Simulate a Round 2 sub-task (depth=2) that still has large remaining work (60,000)
        deep_task = ArithmeticHashTask(task_id="deep_subtask", depth=2, start_idx=0, end_idx=80000)
        deep_task.assigned_node_id = "worker4"
        deep_task.completed_items = 20000

        action_taken, children = strategy.mitigate(deep_task)
        self.assertTrue(action_taken)
        self.assertEqual(children[0].depth, 3)

        # Now test dynamic stop when remaining work is below micro-task floor (< 10,000)
        tiny_task = ArithmeticHashTask(task_id="tiny_subtask", depth=3, start_idx=0, end_idx=15000)
        tiny_task.assigned_node_id = "worker4"
        tiny_task.completed_items = 8000  # remaining = 7,000 < 2 * 5000

        action_taken_tiny, children_tiny = strategy.mitigate(tiny_task)
        self.assertFalse(action_taken_tiny)
        self.assertEqual(len(children_tiny), 0)

    def test_simulation_recursive_subdivision_c100(self):
        """In C100, Recursive Sub-Division must beat Unmitigated with 0.0s wasted compute."""
        c100_profile = [1.93, 1.93, 1.93, 0.20]
        sim = ClusterSimulator(node_quotas=c100_profile, num_partitions=16, items_per_partition=50000)

        unmitigated_res = sim.run("UNMITIGATED", "C100")
        recursive_res = sim.run("RECURSIVE_SUBDIVISION", "C100")

        self.assertLess(recursive_res.jct_seconds, unmitigated_res.jct_seconds)
        self.assertEqual(recursive_res.wasted_cpu_seconds, 0.0)
        self.assertGreater(recursive_res.subtasks_created, 0)


if __name__ == "__main__":
    unittest.main()
