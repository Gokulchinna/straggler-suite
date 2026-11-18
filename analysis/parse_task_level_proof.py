#!/usr/bin/env python3
"""
parse_task_level_proof.py
-------------------------
Deep-dive task-level analysis of Spark event logs across all phases.

Accurately reconciles:
1. Task starts (SparkListenerTaskStart) vs. Task ends (SparkListenerTaskEnd).
2. Distinguishes between completed attempt durations and successful task durations.
3. Quantifies task duration percentiles (Min, Median, P75, P95, Max).
4. Verifies straggler identification (> 1.5x median, Worker 4 bottleneck).
5. Uses portable relative paths.

Outputs:
- results/task_level_distribution_summary.csv
- results/task_level_speculation_mechanics.csv
"""

import json
import statistics
import math
from pathlib import Path
import csv

ROOT = Path(__file__).resolve().parent.parent
RESULTS_DIR = ROOT / "results"

def parse_eventlog_tasks(eventlog_path):
    """
    Parses a single Spark eventlog and extracts task-level metrics,
    tracking all task starts, ends, and speculative clone mechanics.
    """
    tasks_start = {}
    tasks_end = {}
    speculative_starts = []
    speculative_ends = []
    
    with open(eventlog_path, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                data = json.loads(line)
            except json.JSONDecodeError:
                continue
                
            event = data.get("Event")
            tinfo = data.get("Task Info", {})
            
            if event == "SparkListenerTaskStart":
                tid = tinfo.get("Task ID")
                t_record = {
                    "task_id": tid,
                    "index": tinfo.get("Index"),
                    "attempt": tinfo.get("Attempt"),
                    "host": tinfo.get("Host"),
                    "launch_time": tinfo.get("Launch Time"),
                    "speculative": tinfo.get("Speculative", False)
                }
                tasks_start[tid] = t_record
                if t_record["speculative"]:
                    speculative_starts.append(t_record)
                
            elif event == "SparkListenerTaskEnd":
                tid = tinfo.get("Task ID")
                dur = (tinfo.get("Finish Time", 0) - tinfo.get("Launch Time", 0)) / 1000.0
                reason = data.get("Task End Reason", {}).get("Reason", "Unknown")
                t_end_record = {
                    "task_id": tid,
                    "index": tinfo.get("Index"),
                    "attempt": tinfo.get("Attempt"),
                    "host": tinfo.get("Host"),
                    "speculative": tinfo.get("Speculative", False),
                    "duration": dur,
                    "reason": reason,
                    "killed": tinfo.get("Killed", False)
                }
                tasks_end[tid] = t_end_record
                if t_end_record["speculative"]:
                    speculative_ends.append(t_end_record)

    if not tasks_end:
        return None

    # 1. Completed Attempts (all attempts with a TaskEnd event)
    completed_durations = [t["duration"] for t in tasks_end.values()]
    sorted_durs = sorted(completed_durations)
    median_attempt_dur = float(statistics.median(completed_durations))
    p95_idx = min(len(sorted_durs) - 1, int(len(sorted_durs) * 0.95))
    p95_attempt_dur = float(sorted_durs[p95_idx])
    max_attempt_dur = float(max(completed_durations))
    min_attempt_dur = float(min(completed_durations))
    
    # 2. Successful Attempts (only attempts that ended with Success)
    success_durations = [t["duration"] for t in tasks_end.values() if t["reason"] == "Success"]
    mean_success_dur = float(statistics.mean(success_durations)) if success_durations else 0.0
    median_success_dur = float(statistics.median(success_durations)) if success_durations else 0.0
    
    # Identify completed stragglers (> 1.5 * median completed duration)
    stragglers = [t for t in tasks_end.values() if t["duration"] > 1.5 * median_attempt_dur]
    
    # Completed tasks on worker4 vs other workers
    w4_completed = [t["duration"] for t in tasks_end.values() if "worker4" in t["host"]]
    other_completed = [t["duration"] for t in tasks_end.values() if "worker4" not in t["host"]]
    
    # Speculative accounting: Launches vs Ends
    spec_launched_count = len(speculative_starts)
    spec_success_count = sum(1 for t in speculative_ends if t["reason"] == "Success")
    spec_killed_count = sum(1 for t in speculative_ends if t["reason"] in ["TaskKilled", "Killed"])
    spec_in_flight_count = spec_launched_count - len(speculative_ends)
    
    spec_durations = [t["duration"] for t in speculative_ends if t["reason"] == "Success"]
    spec_mean_dur = float(statistics.mean(spec_durations)) if spec_durations else 0.0

    # Host placement for speculative starts
    spec_host_counts = {}
    for st in speculative_starts:
        h = st["host"]
        spec_host_counts[h] = spec_host_counts.get(h, 0) + 1

    return {
        "eventlog": eventlog_path.name,
        "total_task_starts": len(tasks_start),
        "total_task_ends": len(tasks_end),
        "starts_without_end": len(tasks_start) - len(tasks_end),
        "min_attempt_s": round(min_attempt_dur, 2),
        "median_attempt_s": round(median_attempt_dur, 2),
        "p95_attempt_s": round(p95_attempt_dur, 2),
        "max_attempt_s": round(max_attempt_dur, 2),
        "median_success_s": round(median_success_dur, 2),
        "mean_success_s": round(mean_success_dur, 2),
        "straggler_factor": round(max_attempt_dur / median_attempt_dur, 2) if median_attempt_dur > 0 else 1.0,
        "straggler_count": len(stragglers),
        "w4_completed_count": len(w4_completed),
        "w4_mean_duration_s": round(float(statistics.mean(w4_completed)), 2) if w4_completed else 0.0,
        "other_mean_duration_s": round(float(statistics.mean(other_completed)), 2) if other_completed else 0.0,
        "spec_launched": spec_launched_count,
        "spec_success": spec_success_count,
        "spec_killed": spec_killed_count,
        "spec_in_flight": spec_in_flight_count,
        "spec_mean_duration_s": round(spec_mean_dur, 2),
        "spec_host_placement": spec_host_counts
    }

def main():
    print("=" * 80)
    print("RECONCILED TASK-LEVEL STRAGGLER AND SPECULATION MECHANICS AUDIT")
    print("=" * 80)
    
    phases = [
        ("Phase 1: Baseline", RESULTS_DIR / "phase1_baseline" / "eventlogs"),
        ("Phase 2: Contention", RESULTS_DIR / "phase2_contention" / "eventlogs"),
        ("Phase 3: Hardware Diversity", RESULTS_DIR / "phase3_diversity" / "eventlogs")
    ]
    
    all_runs = []
    
    for phase_name, elog_dir in phases:
        if not elog_dir.exists():
            continue
            
        logs = sorted(elog_dir.glob("*.eventlog"))
        for log_path in logs:
            if "warmup" in log_path.name:
                continue
                
            res = parse_eventlog_tasks(log_path)
            if res:
                res["phase"] = phase_name
                stem = log_path.stem
                if "_off_" in stem or stem.startswith("E0_"):
                    res["speculation"] = "OFF"
                elif "_on_" in stem or stem.startswith("E1_"):
                    res["speculation"] = "ON"
                else:
                    res["speculation"] = "UNKNOWN"
                    
                parts = stem.split("_")
                res["condition"] = parts[0]
                all_runs.append(res)
                
    # Group by (Phase, Condition, Speculation)
    grouped = {}
    for r in all_runs:
        key = (r["phase"], r["condition"], r["speculation"])
        grouped.setdefault(key, []).append(r)
        
    summary_rows = []
    mechanics_rows = []
    
    for (phase, cond, spec), items in sorted(grouped.items()):
        total_runs = len(items)
        
        # Aggregate attempt distribution metrics
        mean_min_att = statistics.mean([x["min_attempt_s"] for x in items])
        mean_med_att = statistics.mean([x["median_attempt_s"] for x in items])
        mean_p95_att = statistics.mean([x["p95_attempt_s"] for x in items])
        mean_max_att = statistics.mean([x["max_attempt_s"] for x in items])
        mean_med_succ = statistics.mean([x["median_success_s"] for x in items])
        mean_succ = statistics.mean([x["mean_success_s"] for x in items])
        mean_factor = statistics.mean([x["straggler_factor"] for x in items])
        mean_stragglers = statistics.mean([x["straggler_count"] for x in items])
        
        w4_means = [x["w4_mean_duration_s"] for x in items if x["w4_mean_duration_s"] > 0]
        mean_w4 = statistics.mean(w4_means) if w4_means else 0.0
        
        other_means = [x["other_mean_duration_s"] for x in items if x["other_mean_duration_s"] > 0]
        mean_other = statistics.mean(other_means) if other_means else 0.0
        
        # Speculative attempt accounting
        total_launched = sum(x["spec_launched"] for x in items)
        avg_launched = total_launched / total_runs
        total_success = sum(x["spec_success"] for x in items)
        avg_success = total_success / total_runs
        total_killed = sum(x["spec_killed"] for x in items)
        avg_killed = total_killed / total_runs
        total_in_flight = sum(x["spec_in_flight"] for x in items)
        avg_in_flight = total_in_flight / total_runs
        
        # Host placements aggregated
        combined_hosts = {}
        for x in items:
            for h, c in x["spec_host_placement"].items():
                combined_hosts[h] = combined_hosts.get(h, 0) + c
        host_summary_str = "; ".join(f"{h}:{c}" for h, c in sorted(combined_hosts.items()))
        
        summary_rows.append({
            "phase": phase,
            "condition": cond,
            "speculation": spec,
            "runs_analyzed": total_runs,
            "mean_min_attempt_s": round(mean_min_att, 2),
            "mean_median_attempt_s": round(mean_med_att, 2),
            "mean_p95_attempt_s": round(mean_p95_att, 2),
            "mean_max_attempt_s": round(mean_max_att, 2),
            "mean_successful_task_s": round(mean_succ, 2),
            "straggler_factor": round(mean_factor, 2),
            "straggler_tasks_count": round(mean_stragglers, 1),
            "w4_mean_attempt_s": round(mean_w4, 2),
            "other_workers_mean_attempt_s": round(mean_other, 2),
            "spec_clones_launched_avg": round(avg_launched, 1),
            "spec_clones_successful_avg": round(avg_success, 1),
            "spec_clones_killed_avg": round(avg_killed, 1),
            "spec_clones_in_flight_avg": round(avg_in_flight, 1)
        })
        
        mechanics_rows.append({
            "phase": phase,
            "condition": cond,
            "speculation": spec,
            "runs_analyzed": total_runs,
            "total_spec_starts": total_launched,
            "total_spec_success": total_success,
            "total_spec_killed": total_killed,
            "total_spec_in_flight": total_in_flight,
            "host_placements_total": host_summary_str or "None"
        })

    # Write task_level_distribution_summary.csv
    dist_csv = RESULTS_DIR / "task_level_distribution_summary.csv"
    with open(dist_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(summary_rows[0].keys()))
        writer.writeheader()
        writer.writerows(summary_rows)
    print(f"[+] Saved reconciled distribution summary: {dist_csv}")

    # Write task_level_speculation_mechanics.csv
    mech_csv = RESULTS_DIR / "task_level_speculation_mechanics.csv"
    with open(mech_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(mechanics_rows[0].keys()))
        writer.writeheader()
        writer.writerows(mechanics_rows)
    print(f"[+] Saved reconciled speculation mechanics: {mech_csv}\n")
    
    # Print formatted presentation table
    print(f"{'Condition':<15} | {'Spec':<4} | {'Med Att(s)':<10} | {'P95 Att(s)':<10} | {'Max Att(s)':<10} | {'Mean Succ(s)':<12} | {'Factor':<6} | {'Launches':<8} | {'Success':<7} | {'Killed':<6} | {'In-Flight'}")
    print("-" * 130)
    for r in summary_rows:
        print(f"{r['condition']:<15} | {r['speculation']:<4} | {r['mean_median_attempt_s']:<10.2f} | {r['mean_p95_attempt_s']:<10.2f} | {r['mean_max_attempt_s']:<10.2f} | {r['mean_successful_task_s']:<12.2f} | {r['straggler_factor']:<6.2f} | {r['spec_clones_launched_avg']:<8.1f} | {r['spec_clones_successful_avg']:<7.1f} | {r['spec_clones_killed_avg']:<6.1f} | {r['spec_clones_in_flight_avg']:.1f}")

if __name__ == "__main__":
    main()
