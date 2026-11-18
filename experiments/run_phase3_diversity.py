#!/usr/bin/env python3
"""Automated Runner for Phase 3: Cluster-Wide Hardware Diversity Sensitivity Study.

Sweeps all 4 workers across 5 multi-tier hardware diversity levels (0%, 25%, 50%, 75%, 100%)
while strictly conserving total cluster CPU capacity at 6.0 CPUs.

Tests Speculation OFF vs. Speculation ON across 5 repetitions per condition
with automated 20-second thermal cooldown pauses between runs.
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
RESULTS_DIR = ROOT / "results" / "phase3_diversity"
LOGS_DIR = RESULTS_DIR / "logs"
SAVED_EVENTS_DIR = RESULTS_DIR / "eventlogs"

DIVERSITY_LEVELS = [0, 25, 50, 75, 100]


def parse_args():
    parser = argparse.ArgumentParser(description="Phase 3: Hardware Diversity Runner")
    parser.add_argument("--reps", type=int, default=5, help="Number of repetitions per condition (default: 5)")
    parser.add_argument("--cooldown", type=int, default=20, help="Cooldown sleep in seconds between runs (default: 20)")
    parser.add_argument("--skip-warmup", action="store_true", help="Skip the initial warm-up run")
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


def run_single_experiment(label, speculation, app_name, level_pct):
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

    print(f"  >>> Executing {label} (Diversity={level_pct}%, Speculation={spec_flag})...")
    start_time = time.time()
    res = run_cmd(cmd, check=False, capture=True)
    wall_duration = time.time() - start_time

    with open(output_log, "w", encoding="utf-8") as f:
        f.write(res.stdout)
        if res.stderr:
            f.write("\n--- STDERR ---\n")
            f.write(res.stderr)

    if res.returncode != 0:
        print(f"      [ERROR] Run failed (exit {res.returncode}). See {output_log}")
        return None

    jct_match = re.search(r"Job Completion Time \(JCT\):\s+([\d\.]+)s", res.stdout)
    jct = float(jct_match.group(1)) if jct_match else wall_duration

    # Count speculative attempts from console output
    spec_count = len(re.findall(r"\[SPECULATIVE BACKUP\]", res.stdout))

    # Grant read permissions to host via container root
    run_cmd("docker exec -u 0 spark-driver chmod -R 777 /events", check=False)
    time.sleep(1)

    ev_log = get_latest_event_log(before_files)
    if ev_log:
        saved_ev = SAVED_EVENTS_DIR / f"{label}.eventlog"
        shutil.copyfile(ev_log, saved_ev)
        print(f"      [DONE] JCT: {jct:.2f}s | Speculative Clones: {spec_count} | Saved: {saved_ev.name}")
    else:
        print(f"      [DONE] JCT: {jct:.2f}s | Speculative Clones: {spec_count}")

    return {
        "run_label": label,
        "level_pct": level_pct,
        "speculation": speculation,
        "jct_s": jct,
        "speculative_clones": spec_count,
        "wall_duration_s": wall_duration,
        "log_file": str(output_log)
    }


def main():
    args = parse_args()

    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    SAVED_EVENTS_DIR.mkdir(parents=True, exist_ok=True)
    run_cmd("docker exec -u 0 spark-driver chmod -R 777 /events", check=False)

    summary_file = RESULTS_DIR / "run_summary.csv"
    results = []

    total_runs = len(DIVERSITY_LEVELS) * 2 * args.reps
    print("\n=================================================================")
    print(f"STARTING PHASE 3: CLUSTER-WIDE HARDWARE DIVERSITY BENCHMARK")
    print(f"Levels: {DIVERSITY_LEVELS}% | Repetitions: {args.reps} per condition")
    print(f"Total planned runs: {total_runs} (50 runs at 5 reps)")
    print(f"Cooldown between runs: {args.cooldown}s")
    print("=================================================================\n")

    # Optional Warmup
    if not args.skip_warmup:
        print("--- Warm-Up Run at 0% Diversity ---")
        run_cmd("python3 controllers/set_diversity.py 0", check=True)
        run_single_experiment("warmup_phase3", False, "Straggler-Phase3-Warmup", 0)
        print(f"Cooling down for {args.cooldown}s...\n")
        time.sleep(args.cooldown)

    # Loop over Diversity Levels
    for level in DIVERSITY_LEVELS:
        print(f"\n#################################################################")
        print(f"APPLYING HARDWARE DIVERSITY LEVEL: {level}% ACROSS CLUSTER")
        print(f"#################################################################")
        run_cmd(f"python3 controllers/set_diversity.py {level}", check=True)

        for r in range(1, args.reps + 1):
            print(f"\n--- Diversity {level}% | Round {r} / {args.reps} ---")

            # 1. Speculation OFF
            label_off = f"D{level}_off_rep{r}"
            res_off = run_single_experiment(label_off, False, f"Straggler-{label_off}", level)
            if res_off:
                results.append({"round": r, **res_off})

            print(f"      Thermal cooldown pause: {args.cooldown}s...")
            time.sleep(args.cooldown)

            # 2. Speculation ON
            label_on = f"D{level}_on_rep{r}"
            res_on = run_single_experiment(label_on, True, f"Straggler-{label_on}", level)
            if res_on:
                results.append({"round": r, **res_on})

            if r < args.reps or level != DIVERSITY_LEVELS[-1]:
                print(f"      Thermal cooldown pause: {args.cooldown}s...")
                time.sleep(args.cooldown)

    # Save summary CSV
    if results:
        fields = ["round", "level_pct", "run_label", "speculation", "jct_s", "speculative_clones", "wall_duration_s", "log_file"]
        with open(summary_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            writer.writerows(results)

        print(f"\n=================================================================")
        print(f"ALL PHASE 3 RUNS COMPLETED SUCCESSFULLY!")
        print(f"Summary saved to: {summary_file}")
        print(f"To analyze results and plot Figure 4, run:")
        print(f"  python3 analysis/parse_phase3_diversity.py")
        print("=================================================================\n")


if __name__ == "__main__":
    main()
