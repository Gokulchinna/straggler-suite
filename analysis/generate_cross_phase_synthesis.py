#!/usr/bin/env python3
"""
generate_cross_phase_synthesis.py
---------------------------------
Synthesizes data across Phase 1, Phase 2, and Phase 3 to generate:
  - Figure 4: Grand Cross-Phase JCT Performance Comparison (Seconds)
  - Figure 5: Master Cost-Benefit Trade-Off Frontier
  - Master Summary Table (results/master_summary_table.csv)
"""
import csv
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

ROOT = Path(__file__).resolve().parent.parent
RESULTS_DIR = ROOT / "results"
P1_CSV = RESULTS_DIR / "phase1_baseline" / "run_summary.csv"
P2_CSV = RESULTS_DIR / "phase2_contention" / "run_summary.csv"
P3_CSV = RESULTS_DIR / "phase3_diversity" / "run_summary.csv"
MECH_CSV = RESULTS_DIR / "task_level_distribution_summary.csv"


def load_runs(csv_path: Path):
    if not csv_path.exists():
        return {}
    runs = {}
    with open(csv_path, "r", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if "warmup" in r["run_label"]:
                continue
            lvl = r.get("level_pct", r.get("config", "0"))
            spec = r["speculation"]
            runs.setdefault((str(lvl), spec), []).append(float(r["jct_s"]))
    return runs


def load_clones(csv_path: Path):
    clones = {}
    if csv_path.exists():
        with open(csv_path, "r", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                if r.get("speculation") == "ON":
                    phase = r.get("phase")
                    cond = r.get("condition")
                    clones[(phase, cond)] = float(r.get("spec_clones_launched_avg", 0.0))
    return clones


def main():
    p1_runs = load_runs(P1_CSV)
    p2_runs = load_runs(P2_CSV)
    p3_runs = load_runs(P3_CSV)
    clones_map = load_clones(MECH_CSV)

    if not p1_runs or not p2_runs or not p3_runs:
        print("Error: Run summary CSVs missing.")
        return

    # Master Table Compilation
    master_rows = []

    # Phase 1: Unrounded calculation
    p1_off_list = [v for k, v in p1_runs.items() if k[1] == "False"][0]
    p1_on_list = [v for k, v in p1_runs.items() if k[1] == "True"][0]
    m_p1_off = sum(p1_off_list) / len(p1_off_list)
    m_p1_on = sum(p1_on_list) / len(p1_on_list)
    sp_p1 = ((m_p1_off - m_p1_on) / m_p1_off) * 100.0

    master_rows.append({
        "phase": "Phase 1: Baseline",
        "condition": "Baseline (Fixed Worker 4 at 0.5c)",
        "mean_off_s": round(m_p1_off, 2),
        "mean_on_s": round(m_p1_on, 2),
        "speedup_pct": f"{sp_p1:.2f}%",
        "avg_speculative_clones": clones_map.get(("Phase 1: Baseline", "E1"), 4.0)
    })

    # Phase 2
    for lvl in [0, 25, 50, 75, 100]:
        off_l = p2_runs[(str(lvl), "False")]
        on_l = p2_runs[(str(lvl), "True")]
        m_off = sum(off_l) / len(off_l)
        m_on = sum(on_l) / len(on_l)
        sp = ((m_off - m_on) / m_off) * 100.0
        c_count = clones_map.get(("Phase 2: Contention", f"C{lvl}"), 0.0)
        master_rows.append({
            "phase": "Phase 2: Contention",
            "condition": f"{lvl}% Contention",
            "mean_off_s": round(m_off, 2),
            "mean_on_s": round(m_on, 2),
            "speedup_pct": f"{sp:.2f}%",
            "avg_speculative_clones": c_count
        })

    # Phase 3
    for lvl in [0, 25, 50, 75, 100]:
        off_l = p3_runs[(str(lvl), "False")]
        on_l = p3_runs[(str(lvl), "True")]
        m_off = sum(off_l) / len(off_l)
        m_on = sum(on_l) / len(on_l)
        sp = ((m_off - m_on) / m_off) * 100.0
        c_count = clones_map.get(("Phase 3: Hardware Diversity", f"D{lvl}"), 0.0)
        master_rows.append({
            "phase": "Phase 3: Hardware Diversity",
            "condition": f"{lvl}% Diversity",
            "mean_off_s": round(m_off, 2),
            "mean_on_s": round(m_on, 2),
            "speedup_pct": f"{sp:.2f}%",
            "avg_speculative_clones": c_count
        })

    master_table_file = RESULTS_DIR / "master_summary_table.csv"
    with open(master_table_file, "w", newline="", encoding="utf-8") as f:
        fields = ["phase", "condition", "mean_off_s", "mean_on_s", "speedup_pct", "avg_speculative_clones"]
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(master_rows)
    print(f"Master Table saved: {master_table_file}")

    # Generate Figure 4: Grand Cross-Phase JCT Performance Comparison
    labels = []
    off_vals = []
    on_vals = []

    for r in master_rows:
        c_name = r["condition"].replace(" (Fixed Worker 4 at 0.5c)", "").replace(" Contention", "%\nContention").replace(" Diversity", "%\nDiversity")
        labels.append(c_name)
        off_vals.append(float(r["mean_off_s"]))
        on_vals.append(float(r["mean_on_s"]))

    x = np.arange(len(labels))
    width = 0.38

    fig4, ax = plt.subplots(figsize=(max(12, len(labels) * 1.3), 5.5))
    bars1 = ax.bar(x - width/2, off_vals, width, label='Speculation OFF (Unmitigated)', color='#e74c3c', alpha=0.85, edgecolor='black')
    bars2 = ax.bar(x + width/2, on_vals, width, label='Speculation ON (Mitigated)', color='#2ecc71', alpha=0.85, edgecolor='black')

    ax.set_ylabel("Workload JCT (seconds)", fontsize=11, fontweight="bold")
    ax.set_title("Figure 4: Grand Cross-Phase JCT Performance Comparison", fontsize=13, fontweight="bold")
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=9)
    ax.legend(fontsize=10, loc="upper right")
    ax.grid(axis="y", linestyle="--", alpha=0.6)

    fig4_path = RESULTS_DIR / "graph_master_jct_comparison.png"
    plt.tight_layout()
    plt.savefig(fig4_path, dpi=300)
    plt.close()
    print(f"Figure 4 generated: {fig4_path}")

    # Generate Figure 5: Master Cost-Benefit Trade-Off Frontier
    speedups = []
    clones = []
    tradeoff_labels = []

    for r in master_rows:
        sp_float = float(r["speedup_pct"].replace("%", ""))
        c_float = float(r["avg_speculative_clones"])
        cond_label = r["condition"].replace(" (Fixed Worker 4 at 0.5c)", "").replace(" Contention", "% Cont").replace(" Diversity", "% Div")
        tradeoff_labels.append(cond_label)
        speedups.append(sp_float)
        clones.append(c_float)

    fig5, ax = plt.subplots(figsize=(10, 6))

    # Scatter with bubble size proportional to clones launched
    bubble_sizes = [(c + 0.5) * 80 for c in clones]
    scatter = ax.scatter(clones, speedups, s=bubble_sizes, c=speedups, cmap="RdYlGn", edgecolors="black", alpha=0.85, zorder=3)

    for i, txt in enumerate(tradeoff_labels):
        offset_y = 1.8 if speedups[i] >= 0 else -2.5
        ax.annotate(txt, (clones[i], speedups[i]), textcoords="offset points", xytext=(0, offset_y), ha='center', fontsize=8.5, fontweight='bold')

    ax.axhline(0, color='gray', linestyle='--', linewidth=1.2, zorder=2)
    ax.set_xlabel("Average Speculative Clones Launched per Run", fontsize=11, fontweight="bold")
    ax.set_ylabel("Workload Speedup (%) [collect() JCT]", fontsize=11, fontweight="bold")
    ax.set_title("Figure 5: Master Cost-Benefit Trade-Off Frontier", fontsize=13, fontweight="bold")
    ax.grid(True, linestyle=":", alpha=0.6, zorder=1)

    cbar = plt.colorbar(scatter, ax=ax)
    cbar.set_label("Speedup (%)", fontsize=10)

    fig5_path = RESULTS_DIR / "graph_master_cost_benefit.png"
    plt.tight_layout()
    plt.savefig(fig5_path, dpi=300)
    plt.close()
    print(f"Figure 5 generated: {fig5_path}")


if __name__ == "__main__":
    main()
