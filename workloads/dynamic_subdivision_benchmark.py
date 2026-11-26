#!/usr/bin/env python3
"""PySpark Dynamic Sub-Division Benchmark Workload (Original Design).

Implements Capacity-Aware Task Sub-Division with Residual Slicing (max_depth=3):
- Executes the 3.2-billion-operation deterministic CPU benchmark
  (64 partitions * 200,000 items * 250 inner loop hash iterations).
- Runs standard Spark jobs across a 16-slot ThreadPoolExecutor with cooperative
  checkpointing under /data/dynamic_ckpt (NO Recursive V2 TCP or quarantine logic).
- Matches DynamicSubdivisionStrategy: enforces depth < max_depth (default: 3),
  remaining >= 2 * min_chunk_size, and target_pool = idle hosts + [straggler host].
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

CKPT_DIR = Path("/data/dynamic_ckpt")


def parse_args():
    parser = argparse.ArgumentParser(description="Spark Dynamic Sub-Division Benchmark")
    parser.add_argument("--partitions", type=int, default=64, help="Initial partitions (default: 64)")
    parser.add_argument("--items-per-partition", type=int, default=200000, help="Items per partition (default: 200000)")
    parser.add_argument("--iterations", type=int, default=250, help="Hash iterations per item (default: 250)")
    parser.add_argument("--min-chunk-size", type=int, default=5000, help="Minimum chunk size floor (default: 5000)")
    parser.add_argument("--max-depth", type=int, default=3, help="Max sub-division depth (default: 3)")
    parser.add_argument("--max-slots", type=int, default=16, help="Total concurrent task slots (default: 16)")
    return parser.parse_args()


def _partition_by_capacity(start_offset: int, total_remaining: int, weights: list) -> list:
    """Discrete work-conserving partition proportional to host speeds."""
    total_w = sum(weights)
    if total_w <= 0 or not weights:
        n = len(weights)
        base = total_remaining // n
        rem = total_remaining % n
        out = []
        cur = start_offset
        for i in range(n):
            sz = base + (1 if i < rem else 0)
            if sz > 0:
                out.append((cur, cur + sz))
                cur += sz
        return out

    ranges = []
    allocated = 0
    for i, w in enumerate(weights):
        if i == len(weights) - 1:
            sz = total_remaining - allocated
        else:
            sz = int((total_remaining * w) / total_w)
            allocated += sz
        if sz > 0:
            ranges.append((start_offset, start_offset + sz))
            start_offset += sz
    return ranges


def execute_task_worker(task_tuple, iters=250):
    """Executed on Spark workers. Checkpoints progress and honors cooperative .stop."""
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
    from pyspark.sql import SparkSession
    args = parse_args()
    spark = SparkSession.builder.appName("Straggler-Suite-Dynamic-Subdivision").getOrCreate()
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
    max_depth = args.max_depth
    max_slots = args.max_slots

    print("\n==================================================")
    print("STARTING DYNAMIC SUB-DIVISION BENCHMARK")
    print(f"Partitions: {num_parts} | Items/Partition: {items_per_part} | Iterations: {iters}")
    print(f"Strategy: Capacity-Aware Sub-Division with Residual Slicing (Min Chunk: {min_chunk}, Max Depth: {max_depth})")
    print("==================================================")

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

    active_futures = {}
    stopping_tasks = set()
    completed_durations = []
    host_speeds = {}
    host_counts = {}
    host_durations = {}

    total_checksum = 0
    total_items_processed = 0
    splits_triggered = 0
    sub_slices_spawned = 0

    def submit_task(executor, meta):
        def _run(m):
            sc.setJobGroup(m["task_id"], f"DynamicSubDiv_d{m['depth']}")
            return sc.parallelize([(m["task_id"], m["start_idx"], m["end_idx"])], 1) \
                     .map(lambda t: execute_task_worker(t, iters)) \
                     .collect()[0]

        meta["launch_time"] = time.time()
        fut = executor.submit(_run, meta)
        active_futures[fut] = meta

    start_wall = time.time()

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_slots) as executor:
        while task_queue or active_futures:
            while task_queue and len(active_futures) < max_slots:
                meta = task_queue.popleft()
                submit_task(executor, meta)

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
                        splits_triggered += 1
                        idle_slots = max(1, max_slots - len(active_futures))
                        # Target pool: idle capacity + straggler node (Residual Slicing, max 4 slices)
                        other_hosts = [h for h in sorted(host_speeds.keys()) if h != host]
                        strag_speed = statistics.mean(host_speeds[host]) if host in host_speeds else 10000.0
                        weights = [statistics.mean(host_speeds[h]) for h in other_hosts] + [strag_speed]

                        # Cap slice count by micro-task threshold
                        max_slices = max(1, rem // min_chunk)
                        if len(weights) > max_slices:
                            weights = weights[:max_slices]

                        ranges = _partition_by_capacity(stopped_idx, rem, weights)
                        child_depth = meta["depth"] + 1
                        for idx, (s_idx, e_idx) in enumerate(ranges):
                            child_id = f"{task_id}_d{child_depth}_{idx}"
                            sub_slices_spawned += 1
                            task_queue.appendleft({
                                "task_id": child_id,
                                "partition_id": meta["partition_id"],
                                "start_idx": s_idx,
                                "end_idx": e_idx,
                                "depth": child_depth,
                            })
                            print(
                                f"  [SUB-TASK SLICE] Partition {meta['partition_id']} ({child_id}, depth={child_depth}) "
                                f"range [{s_idx}, {e_idx}) -> {e_idx - s_idx} items (split from {host})"
                            )

            # Tail detection (matches DynamicSubdivisionStrategy: depth < max_depth)
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
                        # Gate 1: Check max recursion depth limit (default: 3)
                        if meta["depth"] >= max_depth or tid in stopping_tasks:
                            continue

                        elapsed = now - meta["launch_time"]
                        if elapsed < max(2.0, 0.5 * med_dur):
                            continue

                        ckpt = read_ckpt(tid)
                        if not ckpt:
                            continue

                        cur_idx, host = ckpt
                        rem = meta["end_idx"] - cur_idx
                        if rem < 2 * min_chunk:
                            continue

                        host_avg_speed = (
                            statistics.mean(host_speeds[host]) if host in host_speeds else med_speed
                        )
                        if host_avg_speed < (0.65 * med_speed) or elapsed > (1.15 * med_dur):
                            stopping_tasks.add(tid)
                            try:
                                (CKPT_DIR / f"{tid}.stop").touch()
                            except OSError:
                                pass

    jct = time.time() - start_wall

    print("\n================ BENCHMARK RESULTS ================")
    print(f"Job Completion Time (JCT): {jct:.2f}s")
    print(f"Total Completed Tasks: {sum(host_counts.values())} | Total Items Verified: {total_items_processed}/{num_parts * items_per_part}")
    print(f"Splits Triggered: {splits_triggered} | Sub-Task Slices Spawned: {sub_slices_spawned}")

    print("\n--- Summary by Worker Node ---")
    for host in sorted(host_counts.keys()):
        durs = host_durations[host]
        avg_dur = sum(durs) / len(durs)
        print(
            f"  Worker {host:15}: {len(durs):2} tasks | Avg: {avg_dur:.2f}s | "
            f"Min: {min(durs):.2f}s | Max: {max(durs):.2f}s"
        )
    print("==================================================\n")

    shutil.rmtree(CKPT_DIR, ignore_errors=True)
    spark.stop()


if __name__ == "__main__":
    main()
