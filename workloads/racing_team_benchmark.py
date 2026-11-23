#!/usr/bin/env python3
"""PySpark Racing Team Speculation Benchmark Workload.

Implements Racing Team Speculation (Parallel Team Speculation):
- Executes identical deterministic 3.2-billion-operation CPU benchmark
  (64 partitions, 200,000 items each, 250 inner loop iterations: (val * 31 + 7) % 1000000007).
- Original slow worker continues executing its original task uninterrupted.
- An auxiliary rescue team of idle fast workers parallelizes remainder slices concurrently.
- Whichever side finishes first terminates/cancels the other side.
"""
import argparse
import socket
import time
from pyspark.sql import SparkSession
from pyspark import TaskContext


def parse_args():
    parser = argparse.ArgumentParser(description="Spark Racing Team Speculation Benchmark")
    parser.add_argument("--partitions", type=int, default=64, help="Number of partitions (default: 64)")
    parser.add_argument("--items-per-partition", type=int, default=200000, help="Items per partition (default: 200000)")
    parser.add_argument("--iterations", type=int, default=250, help="Computation iterations per item (default: 250)")
    return parser.parse_args()


def cpu_intensive_compute(partition_id, iterator, iters=250):
    start_time = time.time()
    hostname = socket.gethostname()
    ctx = TaskContext.get()
    attempt = ctx.attemptNumber() if ctx else 0

    total = 0
    count = 0
    for number in iterator:
        val = number
        for _ in range(iters):
            val = (val * 31 + 7) % 1000000007
        total += val
        count += 1

    duration = time.time() - start_time
    yield (partition_id, attempt, hostname, total, count, duration)


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("Straggler-Suite-Racing-Team").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    items = args.items_per_partition
    iters = args.iterations
    num_parts = args.partitions

    print(f"\n==================================================")
    print(f"STARTING RACING TEAM SPECULATION BENCHMARK")
    print(f"Partitions: {num_parts} | Items/Partition: {items} | Iterations: {iters}")
    print(f"Strategy: Racing Team Speculation (Parallel Team Rescue)")
    print(f"==================================================")

    data = sc.parallelize(range(num_parts * items), num_parts)

    start_wall = time.time()
    results = data.mapPartitionsWithIndex(lambda pid, it: cpu_intensive_compute(pid, it, iters)).collect()
    jct = time.time() - start_wall

    print(f"\n================ BENCHMARK RESULTS ================")
    print(f"Job Completion Time (JCT): {jct:.2f}s")
    print(f"Total Completed Tasks: {len(results)}")

    host_counts = {}
    host_durations = {}
    for part, attempt, host, _, count, dur in sorted(results):
        host_counts[host] = host_counts.get(host, 0) + 1
        host_durations.setdefault(host, []).append(dur)
        if attempt > 0:
            print(f"  [RESCUE TEAM SLICE] Partition {part} (Attempt {attempt}) on {host}: {count} items took {dur:.2f}s")

    print("\n--- Summary by Worker Node ---")
    for host in sorted(host_counts.keys()):
        durs = host_durations[host]
        avg_dur = sum(durs) / len(durs)
        print(f"  Worker {host:15}: {len(durs):2} tasks | Avg: {avg_dur:.2f}s | Min: {min(durs):.2f}s | Max: {max(durs):.2f}s")
    print(f"==================================================\n")

    spark.stop()


if __name__ == "__main__":
    main()
