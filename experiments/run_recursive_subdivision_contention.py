#!/usr/bin/env python3
"""Real Docker Cluster Runner: Recursive Dynamic Sub-Division across Contention (Axis 1).

Executes real Spark jobs inside the 6-container Docker network.
Sweeps Worker 4 through 5 contention levels (C0, C25, C50, C75, C100) x reps
using real Linux cgroups quotas and real wall-clock execution on Spark.
"""
import argparse
import csv
import glob
import os
import re
import shutil
import subprocess
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
EVENTS_DIR = ROOT / "events"
RESULTS_DIR = ROOT / "results" / "recursive_subdivision" / "contention"
LOGS_DIR = RESULTS_DIR / "logs"
SAVED_EVENTS_DIR = RESULTS_DIR / "eventlogs"

CONTENTION_LEVELS = [0, 25, 50, 75, 100]


def parse_args():
    parser = argparse.ArgumentParser(description="Docker Recursive Sub-Division Contention Sweep Runner")
    parser.add_argument("--reps", type=int, default=5, help="Number of repetitions per level (default: 5)")
    parser.add_argument("--cooldown", type=int, default=20, help="Cooldown sleep in seconds between runs (default: 20)")
    return parser.parse_args()


def run_cmd(cmd, check=True, capture=True):
    res = subprocess.run(cmd, shell=True, text=True, capture_output=capture)
    if check and res.returncode != 0:
        err = res.stderr.strip() or res.stdout.strip()
        raise RuntimeError(f"Command failed ({res.returncode}): {cmd}\n{err}")
    return res


def get_latest_event_log(before_files):
    current_files = set(glob.glob(str(EVENTS_DIR / "app-*")))
    new_files = current_files - before_files
    if new_files:
        return max(new_files, key=os.path.getmtime)
    if current_files:
        return max(current_files, key=os.path.getmtime)
    return None


def run_single_experiment(label, app_name, level_pct):
    output_log = LOGS_DIR / f"{label}.log"
    before_files = set(glob.glob(str(EVENTS_DIR / "app-*")))

    cmd = (
        f"docker exec spark-driver /opt/spark/bin/spark-submit "
        f"--master spark://master:7077 "
        f"--conf spark.app.name={app_name} "
        f"--conf spark.speculation=false "
        f"--conf spark.scheduler.mode=FAIR "
        f"--conf spark.eventLog.enabled=true "
        f"--conf spark.eventLog.dir=file:///events "
        f"--conf spark.eventLog.compress=false "
        f"/workloads/recursive_subdivision_benchmark.py "
    )

    print(f"  >>> [DOCKER] Executing {label} (Contention={level_pct}%, Recursive Sub-Division)...")
    start_time = time.time()
    res = run_cmd(cmd, check=False, capture=True)
    wall_duration = time.time() - start_time

    with open(output_log, "w", encoding="utf-8") as f:
        f.write(res.stdout)
        if res.stderr:
            f.write("\n--- STDERR ---\n")
            f.write(res.stderr)

    if res.returncode != 0:
        print(f"      [ERROR] Docker run failed (exit {res.returncode}). See {output_log}")
        return None

    jct_match = re.search(r"Job Completion Time \(JCT\):\s+([\d\.]+)s", res.stdout)
    jct = float(jct_match.group(1)) if jct_match else wall_duration

    splits_match = re.search(r"Recursive Splits Triggered:\s+(\d+)\s+\|\s+Sub-Tasks Spawned:\s+(\d+)\s+\|\s+Max Depth:\s+(\d+)", res.stdout)
    splits = int(splits_match.group(1)) if splits_match else 0
    subtasks = int(splits_match.group(2)) if splits_match else 0
    max_depth = int(splits_match.group(3)) if splits_match else 0

    run_cmd("docker exec -u 0 spark-driver chmod -R 777 /events", check=False)
    time.sleep(1)

    ev_log = get_latest_event_log(before_files)
    if ev_log:
        saved_ev = SAVED_EVENTS_DIR / f"{label}.eventlog"
        shutil.copyfile(ev_log, saved_ev)
        print(f"      [DONE] JCT: {jct:.2f}s | Splits: {splits} | Sub-Tasks: {subtasks} | MaxDepth: {max_depth} | Saved: {saved_ev.name}")
    else:
        print(f"      [DONE] JCT: {jct:.2f}s | Splits: {splits} | Sub-Tasks: {subtasks} | MaxDepth: {max_depth}")

    return {
        "run_label": label,
        "level_pct": level_pct,
        "strategy": "RECURSIVE_SUBDIVISION",
        "jct_s": jct,
        "recursive_splits": splits,
        "subtasks_spawned": subtasks,
        "max_depth": max_depth,
        "wall_duration_s": wall_duration,
        "log_file": str(output_log)
    }


def main():
    args = parse_args()
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    SAVED_EVENTS_DIR.mkdir(parents=True, exist_ok=True)

    summary_csv = RESULTS_DIR / "run_summary.csv"
    results = []

    print("=" * 80)
    print("AXIS 1: DOCKER CLUSTER RECURSIVE SUB-DIVISION CONTENTION SWEEP")
    print(f"Levels: {CONTENTION_LEVELS} | Reps: {args.reps} | Cooldown: {args.cooldown}s")
    print(f"Results Directory: {RESULTS_DIR}")
    print("=" * 80)

    for lvl in CONTENTION_LEVELS:
        print(f"\n==================================================")
        print(f"APPLYING CONTENTION LEVEL: {lvl}% ON WORKER 4 (DOCKER CGROUPS)")
        print(f"==================================================")
        run_cmd(f"python3 controllers/set_contention.py {lvl}", capture=False)

        for rep in range(1, args.reps + 1):
            label = f"recursive_c{lvl}_rep{rep}"
            app_name = f"RecursiveSubdiv_C{lvl}_Rep{rep}"
            row = run_single_experiment(label, app_name, lvl)
            if row:
                results.append(row)

            if rep < args.reps or lvl != CONTENTION_LEVELS[-1]:
                print(f"      [Cooldown] Pausing {args.cooldown}s for thermal stabilization...")
                time.sleep(args.cooldown)

    with open(summary_csv, "w", newline="", encoding="utf-8") as f:
        fieldnames = ["run_label", "level_pct", "strategy", "jct_s", "recursive_splits", "subtasks_spawned", "max_depth", "wall_duration_s", "log_file"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    print(f"\n[SUCCESS] Completed {len(results)} Docker runs. Summary saved to: {summary_csv}")


if __name__ == "__main__":
    main()
