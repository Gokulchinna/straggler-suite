#!/usr/bin/env python3
"""Automated Runner for Phase 1: Baseline Proof-of-Concept.

Executes alternating rounds of E0 (Speculation OFF) and E1 (Speculation ON)
with an automated cooldown period between runs to prevent thermal bias.
"""
import argparse
import csv
import glob
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
EVENTS_DIR = ROOT / "events"
RESULTS_DIR = ROOT / "results" / "phase1_baseline"
LOGS_DIR = RESULTS_DIR / "logs"
SAVED_EVENTS_DIR = RESULTS_DIR / "eventlogs"


def parse_args():
    parser = argparse.ArgumentParser(description="Phase 1: Baseline Benchmark Runner")
    parser.add_argument("--reps", type=int, default=5, help="Number of repetitions per condition (default: 5)")
    parser.add_argument("--cooldown", type=int, default=20, help="Cooldown sleep in seconds between runs (default: 20)")
    parser.add_argument("--skip-warmup", action="store_true", help="Skip the initial warm-up run")
    return parser.parse_args()


def run_cmd(cmd, check=True, capture=True):
    res = subprocess.run(cmd, shell=True, text=True, capture_output=capture)
    if check and res.returncode != 0:
        raise RuntimeError(f"Command failed ({res.returncode}): {cmd}\n{res.stderr}")
    return res


def get_latest_event_log(before_files):
    current_files = set(glob.glob(str(EVENTS_DIR / "app-*")))
    new_files = current_files - before_files
    if new_files:
        return max(new_files, key=os.path.getmtime)
    # Fallback to newest overall
    if current_files:
        return max(current_files, key=os.path.getmtime)
    return None


def run_single_experiment(label, speculation, app_name):
    output_log = LOGS_DIR / f"{label}.log"
    before_files = set(glob.glob(str(EVENTS_DIR / "app-*")))

    spec_flag = "true" if speculation else "false"

    cmd = (
        f"docker exec spark-driver /opt/spark/bin/spark-submit "
        f"--master spark://master:7077 "
        f"--conf spark.app.name={app_name} "
        f"--conf spark.speculation={spec_flag} "
        f"--conf spark.speculation.quantile=0.75 "
        f"--conf spark.speculation.multiplier=1.5 "
        f"--conf spark.speculation.minTaskRuntime=1000ms "
        f"--conf spark.speculation.interval=1000ms "
        f"--conf spark.eventLog.enabled=true "
        f"--conf spark.eventLog.dir=file:///events "
        f"--conf spark.eventLog.compress=false "
        f"/workloads/straggler_benchmark.py "
    )

    print(f"\n>>> Running: {label} (Speculation={spec_flag})")
    start_time = time.time()
    
    res = run_cmd(cmd, check=False, capture=True)
    wall_duration = time.time() - start_time

    # Save output log
    with open(output_log, "w", encoding="utf-8") as f:
        f.write(res.stdout)
        if res.stderr:
            f.write("\n--- STDERR ---\n")
            f.write(res.stderr)

    if res.returncode != 0:
        print(f"  [ERROR] Job failed with exit code {res.returncode}. See {output_log}")
        return None

    # Parse JCT from output
    jct_match = re.search(r"Job Completion Time \(JCT\):\s+([\d\.]+)s", res.stdout)
    jct = float(jct_match.group(1)) if jct_match else wall_duration

    # Fix permissions using container so host user can read event logs
    run_cmd("docker exec -u 0 spark-driver chmod -R 777 /events", check=False)
    time.sleep(1)
    ev_log = get_latest_event_log(before_files)
    if ev_log:
        saved_ev = SAVED_EVENTS_DIR / f"{label}.eventlog"
        shutil.copyfile(ev_log, saved_ev)
        print(f"  [DONE] JCT: {jct:.2f}s | Saved EventLog: {saved_ev.name}")
    else:
        print(f"  [DONE] JCT: {jct:.2f}s | (Warning: No new event log found)")

    return {
        "run_label": label,
        "speculation": speculation,
        "jct_s": jct,
        "wall_duration_s": wall_duration,
        "log_file": str(output_log)
    }


def main():
    args = parse_args()

    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    SAVED_EVENTS_DIR.mkdir(parents=True, exist_ok=True)
    run_cmd("docker exec -u 0 spark-driver chmod -R 777 /events", check=False)

    # Set baseline CPU quotas (Worker 4 throttled to 0.5 CPU)
    print("Setting Phase 1 baseline CPU quotas: [1.5, 1.5, 1.5, 0.5]...")
    run_cmd("python3 controllers/cluster.py set-cpus 1.5 1.5 1.5 0.5", check=True)

    summary_file = RESULTS_DIR / "run_summary.csv"
    results = []

    # Optional Warmup
    if not args.skip_warmup:
        print("\n=== Executing Warm-Up Run (JIT/Cache Warm) ===")
        run_single_experiment("warmup", False, "Straggler-Warmup")
        print(f"Cooling down for {args.cooldown}s...")
        time.sleep(args.cooldown)

    print(f"\n========================================================")
    print(f"STARTING PHASE 1 BENCHMARK: {args.reps} REPETITIONS (Alternating)")
    print(f"Cooldown between runs: {args.cooldown}s")
    print(f"========================================================")

    for r in range(1, args.reps + 1):
        print(f"\n----------------- ROUND {r} / {args.reps} -----------------")

        # 1. Speculation OFF
        res_off = run_single_experiment(f"E0_off_rep{r}", False, f"Straggler-E0-rep{r}")
        if res_off:
            results.append({"round": r, **res_off})

        print(f"Thermal cooldown pause: {args.cooldown}s...")
        time.sleep(args.cooldown)

        # 2. Speculation ON
        res_on = run_single_experiment(f"E1_on_rep{r}", True, f"Straggler-E1-rep{r}")
        if res_on:
            results.append({"round": r, **res_on})

        if r < args.reps:
            print(f"Thermal cooldown pause: {args.cooldown}s...")
            time.sleep(args.cooldown)

    # Write summary CSV
    if results:
        fields = ["round", "run_label", "speculation", "jct_s", "wall_duration_s", "log_file"]
        with open(summary_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            writer.writerows(results)

        print(f"\n========================================================")
        print(f"ALL PHASE 1 RUNS COMPLETED SUCCESSFULLY!")
        print(f"Summary saved to: {summary_file}")
        print(f"To analyze and plot results, run:")
        print(f"  python3 analysis/parse_phase1.py")
        print(f"========================================================\n")


if __name__ == "__main__":
    main()
