#!/usr/bin/env python3
"""Plotting script for Adaptive Hybrid and Enhanced Dynamic Sub-Division."""
import csv
from pathlib import Path
import matplotlib.pyplot as plt
import numpy as np

ROOT = Path(__file__).resolve().parent.parent
CSV_PATH = ROOT / "results" / "dynamic_subdivision" / "simulation_master_comparison.csv"
OUTPUT_PNG = ROOT / "results" / "dynamic_subdivision" / "graph_hybrid_master_comparison.png"


def main():
    if not CSV_PATH.exists():
        print(f"File not found: {CSV_PATH}")
        return

    data = {"Contention": [], "Diversity": []}
    with open(CSV_PATH, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            axis = row["axis"]
            if axis in data:
                data[axis].append(row)

    fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(18, 5.2))
    plt.subplots_adjust(wspace=0.28)

    # Style
    plt.rcParams["font.sans-serif"] = "DejaVu Sans"
    palette = {
        "unm": "#7f8c8d",      # Gray
        "spec": "#e74c3c",     # Red
        "dyn": "#2980b9",      # Blue
        "hyb": "#27ae60",      # Green
    }

    # 1. Axis 1: Contention
    c_rows = data["Contention"]
    c_labels = [r["condition"] for r in c_rows]
    x_c = np.arange(len(c_labels))
    w = 0.20

    ax1.bar(x_c - 1.5*w, [float(r["unmitigated_jct_s"]) for r in c_rows], width=w, label="Unmitigated (OFF)", color=palette["unm"], alpha=0.85)
    ax1.bar(x_c - 0.5*w, [float(r["spark_spec_jct_s"]) for r in c_rows], width=w, label="Spark Speculation (ON)", color=palette["spec"], alpha=0.85)
    ax1.bar(x_c + 0.5*w, [float(r["dynamic_jct_s"]) for r in c_rows], width=w, label="Dynamic Sub-Division", color=palette["dyn"], alpha=0.85)
    ax1.bar(x_c + 1.5*w, [float(r["hybrid_jct_s"]) for r in c_rows], width=w, label="Adaptive Hybrid", color=palette["hyb"], alpha=0.85)

    ax1.set_title("Axis 1: Single-Node Contention (C0 - C100)", fontsize=12, fontweight="bold", pad=10)
    ax1.set_xlabel("Contention Profile (Worker 4 Throttled)", fontweight="bold")
    ax1.set_ylabel("Job Completion Time (seconds)", fontweight="bold")
    ax1.set_xticks(x_c)
    ax1.set_xticklabels(c_labels)
    ax1.grid(axis="y", linestyle="--", alpha=0.5)
    ax1.legend(loc="upper left", framealpha=0.9, fontsize=9)

    # 2. Axis 2: Hardware Diversity
    d_rows = data["Diversity"]
    d_labels = [r["condition"] for r in d_rows]
    x_d = np.arange(len(d_labels))

    ax2.bar(x_d - 1.5*w, [float(r["unmitigated_jct_s"]) for r in d_rows], width=w, label="Unmitigated (OFF)", color=palette["unm"], alpha=0.85)
    ax2.bar(x_d - 0.5*w, [float(r["spark_spec_jct_s"]) for r in d_rows], width=w, label="Spark Speculation (ON)", color=palette["spec"], alpha=0.85)
    ax2.bar(x_d + 0.5*w, [float(r["dynamic_jct_s"]) for r in d_rows], width=w, label="Dynamic Sub-Division", color=palette["dyn"], alpha=0.85)
    ax2.bar(x_d + 1.5*w, [float(r["hybrid_jct_s"]) for r in d_rows], width=w, label="Adaptive Hybrid", color=palette["hyb"], alpha=0.85)

    ax2.set_title("Axis 2: Multi-Tier Hardware Diversity (D0 - D100)", fontsize=12, fontweight="bold", pad=10)
    ax2.set_xlabel("Hardware Diversity Level (Tier Spread)", fontweight="bold")
    ax2.set_ylabel("Job Completion Time (seconds)", fontweight="bold")
    ax2.set_xticks(x_d)
    ax2.set_xticklabels(d_labels)
    ax2.grid(axis="y", linestyle="--", alpha=0.5)
    ax2.legend(loc="upper left", framealpha=0.9, fontsize=9)

    # 3. Wasted Compute (CPU-Seconds) in Diversity
    w_spec = [float(r["spark_wasted_cpu_s"]) for r in d_rows]
    w_dyn = [float(r["dynamic_wasted_cpu_s"]) for r in d_rows]

    ax3.bar(x_d - 0.2, w_spec, width=0.38, label="Spark Speculation (Killed Clones)", color=palette["spec"], alpha=0.85)
    ax3.bar(x_d + 0.2, w_dyn, width=0.38, label="Hybrid & Dynamic Sub-Division", color=palette["hyb"], alpha=0.85)

    ax3.set_title("Wasted Compute under Hardware Diversity", fontsize=12, fontweight="bold", pad=10)
    ax3.set_xlabel("Diversity Level", fontweight="bold")
    ax3.set_ylabel("Wasted CPU-Seconds (Killed Tasks)", fontweight="bold")
    ax3.set_xticks(x_d)
    ax3.set_xticklabels(d_labels)
    ax3.grid(axis="y", linestyle="--", alpha=0.5)
    ax3.legend(loc="upper left", framealpha=0.9, fontsize=9)

    plt.suptitle("Adaptive Hybrid & Capacity-Aware Sub-Division: Overcoming Distributed Stragglers", fontsize=14, fontweight="bold", y=1.02)

    OUTPUT_PNG.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(OUTPUT_PNG, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"[SUCCESS] Plot saved to: {OUTPUT_PNG}")


if __name__ == "__main__":
    main()
