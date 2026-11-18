#!/usr/bin/env python3
"""Event Log Parser and Graph Plotter for Phase 1 (Baseline).

Analyzes Phase 1 event logs, generates metrics, and creates publication-ready charts.
"""
import csv
import json
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

ROOT = Path(__file__).resolve().parent.parent
PHASE1_DIR = ROOT / "results" / "phase1_baseline"
EVENTLOGS_DIR = PHASE1_DIR / "eventlogs"


def parse_eventlog(path: Path):
    events = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError:
                continue

    app_start = app_end = None
    stage_durations = []
    task_attempts = []
    task_starts = {}

    for ev in events:
        name = ev.get("Event")
        if name == "SparkListenerApplicationStart":
            app_start = ev.get("Timestamp")
        elif name == "SparkListenerApplicationEnd":
            app_end = ev.get("Timestamp")
        elif name == "SparkListenerStageCompleted":
            info = ev.get("Stage Info", {})
            st = info.get("Submission Time")
            ct = info.get("Completion Time")
            if st and ct:
                stage_durations.append((ct - st) / 1000.0)
        elif name == "SparkListenerTaskStart":
            t_info = ev.get("Task Info", {})
            task_starts[t_info.get("Task ID")] = t_info
        elif name == "SparkListenerTaskEnd":
            t_info = ev.get("Task Info", {})
            tid = t_info.get("Task ID")
            st_info = task_starts.get(tid, {})
            start_ts = t_info.get("Launch Time") or st_info.get("Launch Time")
            finish_ts = t_info.get("Finish Time")
            dur = (finish_ts - start_ts) if (start_ts and finish_ts) else None

            task_attempts.append({
                "task_id": tid,
                "stage_id": ev.get("Stage ID"),
                "partition": t_info.get("Index"),
                "attempt": t_info.get("Attempt"),
                "host": t_info.get("Host"),
                "executor_id": t_info.get("Executor ID"),
                "speculative": t_info.get("Speculative", False),
                "successful": t_info.get("Successful", False),
                "duration_ms": dur
            })

    jct_s = ((app_end - app_start) / 1000.0) if (app_start and app_end) else None
    return jct_s, stage_durations, task_attempts


def main():
    event_files = sorted(EVENTLOGS_DIR.glob("*.eventlog"))
    if not event_files:
        print(f"No event logs found in {EVENTLOGS_DIR}. Run experiments/run_phase1_baseline.py first.")
        return

    print(f"Parsing {len(event_files)} event logs from Phase 1...")

    all_attempts = []
    run_records = []

    for ef in event_files:
        if ef.stem == "warmup":
            continue
        label = ef.stem
        config = "E0_OFF" if "off" in label else "E1_ON"
        jct, stages, attempts = parse_eventlog(ef)

        spec_attempts = sum(1 for a in attempts if a["speculative"])
        for a in attempts:
            all_attempts.append({"run_label": label, "config": config, **a})

        run_records.append({
            "run_label": label,
            "config": config,
            "jct_s": jct,
            "stage_time_s": stages[0] if stages else None,
            "total_attempts": len(attempts),
            "speculative_attempts": spec_attempts
        })

    # Save attempts CSV
    attempts_csv = PHASE1_DIR / "task_attempts.csv"
    if all_attempts:
        with open(attempts_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(all_attempts[0].keys()))
            writer.writeheader()
            writer.writerows(all_attempts)
        print(f"Saved: {attempts_csv}")

    # Load Workload collect() JCT from run_summary.csv
    run_summary_file = PHASE1_DIR / "run_summary.csv"
    workload_jcts = {}
    if run_summary_file.exists():
        with open(run_summary_file, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if "warmup" in row["run_label"]:
                    continue
                workload_jcts[row["run_label"]] = float(row["jct_s"])

    # Compute Aggregate Stats using Workload collect() JCT
    e0_jcts = [workload_jcts[r["run_label"]] for r in run_records if r["config"] == "E0_OFF" and r["run_label"] in workload_jcts]
    e1_jcts = [workload_jcts[r["run_label"]] for r in run_records if r["config"] == "E1_ON" and r["run_label"] in workload_jcts]

    mean_e0 = float(np.mean(e0_jcts)) if e0_jcts else 0
    std_e0 = float(np.std(e0_jcts, ddof=1)) if len(e0_jcts) > 1 else 0
    mean_e1 = float(np.mean(e1_jcts)) if e1_jcts else 0
    std_e1 = float(np.std(e1_jcts, ddof=1)) if len(e1_jcts) > 1 else 0

    speedup = ((mean_e0 - mean_e1) / mean_e0 * 100) if mean_e0 > 0 else 0

    print("\n================ PHASE 1 STATISTICAL SUMMARY (WORKLOAD JCT) ================")
    print(f"E0 (Speculation OFF): Mean JCT = {mean_e0:.2f}s (±{std_e0:.2f}s) across {len(e0_jcts)} runs")
    print(f"E1 (Speculation ON) : Mean JCT = {mean_e1:.2f}s (±{std_e1:.2f}s) across {len(e1_jcts)} runs")
    print(f"Improvement / Speedup: {speedup:.2f}%")
    print("============================================================================\n")

    summary_csv = PHASE1_DIR / "aggregate_summary.csv"
    with open(summary_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["config", "mean_jct_s", "std_jct_s", "runs", "speedup_pct"])
        writer.writeheader()
        writer.writerow({"config": "E0_OFF", "mean_jct_s": round(mean_e0, 2), "std_jct_s": round(std_e0, 2), "runs": len(e0_jcts), "speedup_pct": "-"})
        writer.writerow({"config": "E1_ON", "mean_jct_s": round(mean_e1, 2), "std_jct_s": round(std_e1, 2), "runs": len(e1_jcts), "speedup_pct": f"{speedup:.2f}%"})
    print(f"Saved: {summary_csv}")

    # Generate Publication Plots
    plot_file = PHASE1_DIR / "graph_phase1_comparison.png"
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 4.5))

    # Bar Chart with error bars
    configs = ["E0 (Spec OFF)", "E1 (Spec ON)"]
    means = [mean_e0, mean_e1]
    stds = [std_e0, std_e1]
    colors = ["#e74c3c", "#2ecc71"]

    ax1.bar(configs, means, yerr=stds, capsize=6, color=colors, alpha=0.85, edgecolor="black", width=0.5)
    ax1.set_ylabel("Job Completion Time (seconds)", fontsize=11)
    ax1.set_title("Mean JCT Comparison (±1 Std Dev)", fontsize=12, fontweight="bold")
    ax1.grid(axis="y", linestyle="--", alpha=0.6)
    for i, v in enumerate(means):
        ax1.text(i, v / 2, f"{v:.1f}s", ha="center", va="center", color="white", fontweight="bold", fontsize=12)

    # Box Plot
    box_data = [e0_jcts, e1_jcts]
    bp = ax2.boxplot(box_data, patch_artist=True,
                     boxprops=dict(facecolor="#3498db", alpha=0.6),
                     medianprops=dict(color="red", linewidth=2))
    ax2.set_xticks([1, 2])
    ax2.set_xticklabels(configs)
    ax2.set_ylabel("Job Completion Time (seconds)", fontsize=11)
    ax2.set_title("JCT Distribution across Repetitions", fontsize=12, fontweight="bold")
    ax2.grid(axis="y", linestyle="--", alpha=0.6)

    fig.suptitle(f"Phase 1: Baseline Straggler Mitigation ({speedup:.1f}% Speedup)", fontsize=14, fontweight="bold")
    plt.tight_layout()
    plt.savefig(plot_file, dpi=300)
    plt.close()
    print(f"Chart generated: {plot_file}")


if __name__ == "__main__":
    main()
