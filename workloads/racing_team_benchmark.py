#!/usr/bin/env python3
"""PySpark Racing Team Speculation Benchmark Workload (Original Design).

Implements Parallel Racing Team Speculation (matching RacingTeamSpeculationStrategy, max_depth=2):
- Executes the 3.2-billion-operation deterministic CPU benchmark
  (64 partitions * 200,000 items * 250 inner loop hash iterations).
- Runs standard Spark jobs across a 16-slot ThreadPoolExecutor with checkpoints under /data/racing_ckpt.
- Original slow worker continues executing UNINTERRUPTED (NEVER stopped early).
- In tail phase when idle workers exist, depth < max_depth (default: 2), and remaining >= 2 * min_chunk_size:
  snapshots exact completed prefix & checksum from checkpoint, partitions the remaining
  range [cur_idx, end_idx) across idle workers only (Racing Team), and launches parallel
  rescue jobs on Spark concurrently against the slow task.
- First side to complete wins cleanly, commits exact count & checksum up the race tree,
  and calls Spark sc.cancelJobGroup() to kill all tasks on the losing side.
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

CKPT_DIR = Path("/data/racing_ckpt")


def parse_args():
    parser = argparse.ArgumentParser(description="Spark Racing Team Speculation Benchmark")
    parser.add_argument("--partitions", type=int, default=64, help="Initial partitions (default: 64)")
    parser.add_argument("--items-per-partition", type=int, default=200000, help="Items per partition (default: 200000)")
    parser.add_argument("--iterations", type=int, default=250, help="Hash iterations per item (default: 250)")
    parser.add_argument("--min-chunk-size", type=int, default=5000, help="Minimum chunk size floor (default: 5000)")
    parser.add_argument("--max-depth", type=int, default=2, help="Max speculation race depth (default: 2)")
    parser.add_argument("--max-slots", type=int, default=16, help="Total concurrent task slots (default: 16)")
    return parser.parse_args()


def _partition_by_capacity(start_offset: int, total_remaining: int, weights: list) -> list:
    """Partitions remainder strictly across idle healthy workers proportional to throughput."""
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


def execute_racing_worker(task_tuple, iters=250):
    """Executed on Spark workers. Runs uninterrupted (killed only via Spark cancelJobGroup if it loses)."""
    task_id, start_idx, end_idx = task_tuple
    start_time = time.time()
    hostname = socket.gethostname()

    ckpt_file = CKPT_DIR / f"{task_id}.ckpt"
    total = 0
    count = 0
    current_idx = start_idx

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
                    f.write(f"{current_idx},{total},{hostname}")
                os.replace(tmp_file, ckpt_file)
            except OSError:
                pass

    duration = time.time() - start_time
    return (task_id, "COMPLETED", hostname, total, count, duration, current_idx)


def read_ckpt(task_id):
    ckpt_file = CKPT_DIR / f"{task_id}.ckpt"
    if not ckpt_file.exists():
        return None
    try:
        with open(ckpt_file, "r") as f:
            parts = f.read().strip().split(",")
        if len(parts) == 3:
            return int(parts[0]), int(parts[1]), parts[2]
    except (OSError, ValueError):
        pass
    return None


def main():
    from pyspark.sql import SparkSession
    args = parse_args()
    spark = SparkSession.builder.appName("Straggler-Suite-Racing-Team").getOrCreate()
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
    print("STARTING RACING TEAM SPECULATION BENCHMARK")
    print(f"Partitions: {num_parts} | Items/Partition: {items_per_part} | Iterations: {iters}")
    print(f"Strategy: Parallel Rescue Team Race vs. Uninterrupted Straggler (Min Chunk: {min_chunk}, Max Depth: {max_depth})")
    print("==================================================")

    task_queue = deque()
    for p in range(num_parts):
        s = p * items_per_part
        e = s + items_per_part
        task_queue.append({
            "task_id": f"p{p}",
            "partition_id": p,
            "parent_task_id": None,
            "start_idx": s,
            "end_idx": e,
            "depth": 0,
        })

    active_futures = {}          # future -> task_meta
    races_by_parent = {}         # parent_task_id -> race state
    committed_partitions = set()
    completed_durations = []
    host_speeds = {}
    host_counts = {}
    host_durations = {}

    total_checksum = 0
    total_items_processed = 0
    races_triggered = 0
    rescue_slices_spawned = 0
    team_wins = 0
    parent_wins = 0

    def submit_task(executor, meta):
        def _run(m):
            sc.setJobGroup(m["task_id"], f"RacingTeam_p{m['partition_id']}_d{m['depth']}")
            return sc.parallelize([(m["task_id"], m["start_idx"], m["end_idx"])], 1) \
                     .map(lambda t: execute_racing_worker(t, iters)) \
                     .collect()[0]

        meta["launch_time"] = time.time()
        fut = executor.submit(_run, meta)
        active_futures[fut] = meta

    def cancel_job_safe(job_group_id):
        try:
            sc.cancelJobGroup(job_group_id)
        except Exception:
            pass

    def cancel_race_subtree(parent_tid):
        race = races_by_parent.get(parent_tid)
        if not race:
            return
        if race["winner"] is None:
            race["winner"] = "CANCELLED"
        for cid in list(race["remaining_children"]):
            cancel_job_safe(cid)
            cancel_race_subtree(cid)

    def resolve_task_success(meta, t_sum, t_count, host):
        nonlocal total_checksum, total_items_processed, parent_wins, team_wins
        pid = meta["partition_id"]
        tid = meta["task_id"]
        if pid in committed_partitions:
            return

        p_tid = meta["parent_task_id"]
        if p_tid is None:
            # Root partition completed
            if tid in races_by_parent and races_by_parent[tid]["winner"] is None:
                races_by_parent[tid]["winner"] = "PARENT"
                parent_wins += 1
                cancel_race_subtree(tid)
                print(f"  [RACE WON BY PARENT] Partition {pid}: {tid} on {host} finished first; cancelled rescue team")
            committed_partitions.add(pid)
            total_checksum += t_sum
            total_items_processed += t_count
        else:
            race = races_by_parent.get(p_tid)
            if race and race["winner"] is None and tid in race["remaining_children"]:
                race["remaining_children"].discard(tid)
                race["team_sum"] += t_sum
                race["team_count"] += t_count

                if not race["remaining_children"]:
                    race["winner"] = "TEAM"
                    team_wins += 1
                    cancel_job_safe(p_tid)
                    print(
                        f"  [RACE WON BY TEAM] Partition {pid} (depth={meta['depth']}): "
                        f"Rescue slices finished first; killed {p_tid}"
                    )
                    resolve_task_success(
                        race["parent_meta"],
                        race["base_sum"] + race["team_sum"],
                        race["base_count"] + race["team_count"],
                        host,
                    )

    start_wall = time.time()

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_slots) as executor:
        while len(committed_partitions) < num_parts and (task_queue or active_futures):
            while task_queue and len(active_futures) < max_slots:
                meta = task_queue.popleft()
                if meta["partition_id"] in committed_partitions:
                    continue
                submit_task(executor, meta)

            if not active_futures:
                break

            done, _ = concurrent.futures.wait(
                active_futures.keys(),
                timeout=0.4,
                return_when=concurrent.futures.FIRST_COMPLETED
            )

            for fut in done:
                meta = active_futures.pop(fut)
                try:
                    task_id, status, host, partial_total, count, dur, _ = fut.result()
                except Exception:
                    continue

                host_counts[host] = host_counts.get(host, 0) + 1
                host_durations.setdefault(host, []).append(dur)
                if dur > 0.05 and count > 0:
                    host_speeds.setdefault(host, []).append(count / dur)

                if meta["depth"] == 0:
                    completed_durations.append(dur)

                resolve_task_success(meta, partial_total, count, host)

            # Launch Parallel Rescue Teams when initial queue empties & idle fast slots exist (depth < max_depth)
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
                        pid = meta["partition_id"]
                        if meta["depth"] >= max_depth or tid in races_by_parent or pid in committed_partitions:
                            continue

                        elapsed = now - meta["launch_time"]
                        if elapsed < max(2.0, 0.5 * med_dur):
                            continue

                        ckpt = read_ckpt(tid)
                        if not ckpt:
                            continue

                        cur_idx, base_sum, straggler_host = ckpt
                        rem = meta["end_idx"] - cur_idx
                        if rem < 2 * min_chunk:
                            continue

                        host_avg_speed = (
                            statistics.mean(host_speeds[straggler_host])
                            if straggler_host in host_speeds else med_speed
                        )
                        if host_avg_speed < (0.65 * med_speed) or elapsed > (1.15 * med_dur):
                            idle_healthy = [h for h in sorted(host_speeds.keys()) if h != straggler_host]
                            if not idle_healthy:
                                idle_healthy = ["idle_pool"]
                            weights = [
                                statistics.mean(host_speeds[h]) if h in host_speeds else med_speed
                                for h in idle_healthy
                            ]
                            max_slices = min(len(weights), max(1, rem // min_chunk), idle_slots)
                            ranges = _partition_by_capacity(cur_idx, rem, weights[:max_slices])

                            if ranges:
                                races_triggered += 1
                                child_depth = meta["depth"] + 1
                                child_ids = set()
                                for idx, (s_idx, e_idx) in enumerate(ranges):
                                    cid = f"{tid}_t{child_depth}_{idx}"
                                    child_ids.add(cid)
                                    rescue_slices_spawned += 1
                                    task_queue.append({
                                        "task_id": cid,
                                        "partition_id": pid,
                                        "parent_task_id": tid,
                                        "start_idx": s_idx,
                                        "end_idx": e_idx,
                                        "depth": child_depth,
                                    })
                                    print(
                                        f"  [RESCUE TEAM SLICE] Partition {pid} ({cid}, depth={child_depth}): "
                                        f"{e_idx - s_idx} items racing uninterrupted {straggler_host} from {cur_idx}/{meta['end_idx']}"
                                    )

                                races_by_parent[tid] = {
                                    "parent_meta": meta,
                                    "base_sum": base_sum,
                                    "base_count": cur_idx - meta["start_idx"],
                                    "remaining_children": child_ids,
                                    "team_sum": 0,
                                    "team_count": 0,
                                    "winner": None,
                                }
                                idle_slots -= len(ranges)
                                if idle_slots <= 0:
                                    break

    jct = time.time() - start_wall

    print("\n================ BENCHMARK RESULTS ================")
    print(f"Job Completion Time (JCT): {jct:.2f}s")
    print(f"Total Completed Tasks: {sum(host_counts.values())} | Total Items Verified: {total_items_processed}/{num_parts * items_per_part}")
    print(f"Races Triggered: {races_triggered} | Rescue Slices Spawned: {rescue_slices_spawned} | Team Wins: {team_wins} | Parent Wins: {parent_wins}")

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
