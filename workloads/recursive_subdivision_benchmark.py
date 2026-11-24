#!/usr/bin/env python3
"""PySpark Recursive Dynamic Sub-Division Benchmark Workload.

Implements Multi-Round Hierarchical Residual Slicing (Sub-task of Sub-task):
- Executes the exact 3.2-billion-operation deterministic CPU benchmark
  (64 partitions * 200,000 items * 250 inner loop iterations).
- Uses lightweight cooperative checkpoints on shared /data/recursive_ckpt so
  the driver can measure real-time task progress, preempt lagging tasks with
  0.0s wasted work, and recursively slice remaining work across idle workers
  for as many rounds as needed until remaining work < 2 * min_chunk_size.
"""
import argparse
import concurrent.futures
import os
import shutil
import socket
import statistics
import time
from collections import deque
from pathlib import Path
from pyspark.sql import SparkSession

CKPT_DIR = Path("/data/recursive_ckpt")


def parse_args():
    parser = argparse.ArgumentParser(description="Spark Recursive Dynamic Sub-Division Benchmark")
    parser.add_argument("--partitions", type=int, default=64, help="Initial partitions (default: 64)")
    parser.add_argument("--items-per-partition", type=int, default=200000, help="Items per partition (default: 200000)")
    parser.add_argument("--iterations", type=int, default=250, help="Computation iterations per item (default: 250)")
    parser.add_argument("--min-chunk-size", type=int, default=5000, help="Minimum chunk size floor (default: 5000)")
    parser.add_argument("--max-slots", type=int, default=16, help="Total concurrent task slots (default: 16)")
    return parser.parse_args()


def execute_slice_worker(task_tuple, iters=250):
    """Executed on Spark workers. Periodically checkpoints progress and honors .stop."""
    task_id, start_idx, end_idx = task_tuple
    start_time = time.time()
    hostname = socket.gethostname()

    ckpt_file = CKPT_DIR / f"{task_id}.ckpt"
    stop_file = CKPT_DIR / f"{task_id}.stop"

    total = 0
    count = 0
    current_idx = start_idx
    status = "COMPLETED"

    ckpt_interval = 2000
    for number in range(start_idx, end_idx):
        val = number
        for _ in range(iters):
            val = (val * 31 + 7) % 1000000007
        total += val
        count += 1
        current_idx = number + 1

        if count % ckpt_interval == 0:
            try:
                tmp_file = CKPT_DIR / f"{task_id}.ckpt.tmp"
                with open(tmp_file, "w") as f:
                    f.write(f"{current_idx},{hostname}")
                os.replace(tmp_file, ckpt_file)
            except OSError:
                pass
            if stop_file.exists():
                status = "PREEMPTED"
                break

    duration = time.time() - start_time
    return (task_id, status, hostname, total, count, duration, current_idx)


def read_ckpt(task_id):
    ckpt_file = CKPT_DIR / f"{task_id}.ckpt"
    if not ckpt_file.exists():
        return None
    try:
        with open(ckpt_file, "r") as f:
            parts = f.read().strip().split(",")
        if len(parts) == 2:
            return int(parts[0]), parts[1]
    except (OSError, ValueError):
        pass
    return None


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("Straggler-Suite-Recursive-Subdivision").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    if CKPT_DIR.exists():
        shutil.rmtree(CKPT_DIR, ignore_errors=True)
    CKPT_DIR.mkdir(parents=True, exist_ok=True)
    try:
        os.chmod(CKPT_DIR, 0o777)
    except OSError:
        pass

    num_parts = args.partitions
    items_per_part = args.items_per_partition
    iters = args.iterations
    min_chunk = args.min_chunk_size
    max_slots = args.max_slots

    print("\n==================================================")
    print("STARTING RECURSIVE DYNAMIC SUB-DIVISION BENCHMARK")
    print(f"Partitions: {num_parts} | Items/Partition: {items_per_part} | Iterations: {iters}")
    print(f"Strategy: Multi-Round Recursive Sub-Division (Min Chunk: {min_chunk}, Dynamic Depth)")
    print("==================================================")

    # Initial 64 partition tasks covering [0, num_parts * items_per_part)
    task_queue = deque()
    for p in range(num_parts):
        s = p * items_per_part
        e = s + items_per_part
        task_queue.append({
            "task_id": f"p{p}_d0",
            "partition_id": p,
            "start_idx": s,
            "end_idx": e,
            "depth": 0,
        })

    active_futures = {}  # future -> task_meta
    stopping_tasks = set()
    completed_durations = []
    host_speeds = {}     # host -> list of items/sec
    host_counts = {}
    host_durations = {}

    total_checksum = 0
    total_items_processed = 0
    recursive_splits = 0
    max_depth_reached = 0
    subtasks_spawned = 0

    def submit_task(executor, meta):
        def _run(m):
            sc.setJobGroup(m["task_id"], f"RecursiveTask_d{m['depth']}")
            res = sc.parallelize([(m["task_id"], m["start_idx"], m["end_idx"])], 1) \
                    .map(lambda t: execute_slice_worker(t, iters)) \
                    .collect()[0]
            return res

        meta["launch_time"] = time.time()
        fut = executor.submit(_run, meta)
        active_futures[fut] = meta

    start_wall = time.time()

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_slots) as executor:
        while task_queue or active_futures:
            # 1. Fill idle slots from queue
            while task_queue and len(active_futures) < max_slots:
                meta = task_queue.popleft()
                submit_task(executor, meta)

            # 2. Check completed futures (short timeout to allow recursive monitoring)
            done, _ = concurrent.futures.wait(
                active_futures.keys(),
                timeout=0.4,
                return_when=concurrent.futures.FIRST_COMPLETED
            )

            for fut in done:
                meta = active_futures.pop(fut)
                task_id, status, host, partial_total, count, dur, stopped_idx = fut.result()

                total_checksum += partial_total
                total_items_processed += count
                host_counts[host] = host_counts.get(host, 0) + 1
                host_durations.setdefault(host, []).append(dur)

                if dur > 0.05 and count > 0:
                    host_speeds.setdefault(host, []).append(count / dur)

                if status == "COMPLETED":
                    if meta["depth"] == 0:
                        completed_durations.append(dur)
                elif status == "PREEMPTED":
                    rem = meta["end_idx"] - stopped_idx
                    if rem > 0:
                        idle_slots = max(2, max_slots - len(active_futures))
                        # Dynamic slice count bounded by micro-task threshold
                        max_possible_slices = max(1, rem // min_chunk)
                        num_slices = min(idle_slots, max_possible_slices, 4)

                        if num_slices <= 1:
                            # Too small to split further -> run as single remaining tail task
                            child_depth = meta["depth"] + 1
                            max_depth_reached = max(max_depth_reached, child_depth)
                            subtasks_spawned += 1
                            task_queue.appendleft({
                                "task_id": f"{meta['task_id']}_r{child_depth}_0",
                                "partition_id": meta["partition_id"],
                                "start_idx": stopped_idx,
                                "end_idx": meta["end_idx"],
                                "depth": child_depth,
                            })
                        else:
                            child_depth = meta["depth"] + 1
                            max_depth_reached = max(max_depth_reached, child_depth)
                            chunk_size = rem // num_slices
                            cur = stopped_idx
                            for i in range(num_slices):
                                nxt = meta["end_idx"] if i == num_slices - 1 else cur + chunk_size
                                subtasks_spawned += 1
                                task_queue.appendleft({
                                    "task_id": f"{meta['task_id']}_r{child_depth}_{i}",
                                    "partition_id": meta["partition_id"],
                                    "start_idx": cur,
                                    "end_idx": nxt,
                                    "depth": child_depth,
                                })
                                cur = nxt
                            print(
                                f"  [RECURSIVE SPLIT R{child_depth}] {task_id} on {host} preempted at "
                                f"{stopped_idx}/{meta['end_idx']} -> {num_slices} sub-tasks (rem={rem})"
                            )

            # 3. Recursive Tail Monitor: if queue is empty and idle capacity exists
            if not task_queue and active_futures:
                idle_slots = max_slots - len(active_futures)
                if idle_slots >= 2:
                    med_dur = statistics.median(completed_durations) if completed_durations else 12.0
                    med_speed = (
                        statistics.median([statistics.mean(v) for v in host_speeds.values()])
                        if host_speeds else 25000.0
                    )

                    now = time.time()
                    for fut, meta in list(active_futures.items()):
                        tid = meta["task_id"]
                        if tid in stopping_tasks:
                            continue

                        elapsed = now - meta["launch_time"]
                        if elapsed < max(2.0, 0.5 * med_dur):
                            continue

                        ckpt = read_ckpt(tid)
                        if not ckpt:
                            continue

                        cur_idx, host = ckpt
                        rem = meta["end_idx"] - cur_idx

                        # Dynamic granularity guard: do not split below 2 * min_chunk
                        if rem < 2 * min_chunk:
                            continue

                        # Check if lagging: either elapsed > 1.2 * med_dur or host is slow
                        host_avg_speed = (
                            statistics.mean(host_speeds[host]) if host in host_speeds else med_speed
                        )
                        is_slow_host = host_avg_speed < (0.65 * med_speed)
                        is_tail_straggler = elapsed > (1.15 * med_dur)

                        if is_slow_host or is_tail_straggler:
                            stopping_tasks.add(tid)
                            recursive_splits += 1
                            try:
                                (CKPT_DIR / f"{tid}.stop").touch()
                            except OSError:
                                pass

    jct = time.time() - start_wall

    print("\n================ BENCHMARK RESULTS ================")
    print(f"Job Completion Time (JCT): {jct:.2f}s")
    print(f"Total Items Verified: {total_items_processed} (Expected: {num_parts * items_per_part})")
    print(f"Recursive Splits Triggered: {recursive_splits} | Sub-Tasks Spawned: {subtasks_spawned} | Max Depth: {max_depth_reached}")

    print("\n--- Summary by Worker Node ---")
    for host in sorted(host_counts.keys()):
        durs = host_durations[host]
        avg_dur = sum(durs) / len(durs)
        print(f"  Worker {host:15}: {len(durs):2} tasks | Avg: {avg_dur:.2f}s | Min: {min(durs):.2f}s | Max: {max(durs):.2f}s")
    print("==================================================\n")

    shutil.rmtree(CKPT_DIR, ignore_errors=True)
    spark.stop()


if __name__ == "__main__":
    main()
