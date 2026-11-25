#!/usr/bin/env python3
"""Grand 6-Way Comparative Synthesis across Real Docker Cluster Runs.

Contrasts all 6 physical Docker cluster execution strategies:
1. Speculation OFF (Unmitigated)
2. Spark Speculation ON (Legacy Speculation)
3. Dynamic Sub-Division (Single-Round Residual Slicing)
4. Racing Team Speculation (Parallel Team Speculation)
5. Recursive Sub-Division V1 (Disk Checkpoint Multi-Job Stealing)
6. Recursive Sub-Division V2 / Option B (Zero-Disk TCP + ETA Cutoff + Quarantine)

Evaluates:
- Axis 1: Contention (C0, C25, C50, C75, C100)
- Axis 2: Hardware Diversity (D0, D25, D50, D75, D100)

Generates:
- results/recursive_v2/master_6way_comparison.csv
- results/recursive_v2/graph_6way_grand_comparison.png
"""
import csv
import math
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RESULTS_DIR = ROOT / "results"
DYNAMIC_DIR = RESULTS_DIR / "dynamic_subdivision"
RACING_DIR = RESULTS_DIR / "racing_team"
REC_V1_DIR = RESULTS_DIR / "recursive_subdivision"
REC_V2_DIR = RESULTS_DIR / "recursive_v2"


def sample_stats(vals):
    n = len(vals)
    if n == 0:
        return 0.0, 0.0
    m = sum(vals) / n
    var = sum((x - m) ** 2 for x in vals) / (n - 1) if n > 1 else 0.0
    return m, math.sqrt(var)


def load_phase_data(csv_path):
    data = {}
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if "warmup" in row["run_label"]:
                continue
            lvl = int(row.get("level_pct", 0))
            spec = row.get("speculation", "")
            jct = float(row["jct_s"])
            data.setdefault((lvl, spec), []).append(jct)
    return data


def load_strategy_data(csv_path):
    data = {}
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            lvl = int(row.get("level_pct", 0))
            jct = float(row["jct_s"])
            data.setdefault(lvl, []).append(jct)
    return data


def main():
    print("=" * 145)
    print("6-WAY GRAND REAL DOCKER SYNTHESIS: OFF vs. SPARK vs. DYNAMIC vs. RACING vs. REC V1 vs. REC V2 (OPTION B)")
    print("=" * 145)

    p2_data = load_phase_data(RESULTS_DIR / "phase2_contention" / "run_summary.csv")
    p3_data = load_phase_data(RESULTS_DIR / "phase3_diversity" / "run_summary.csv")
    dyn_cont_data = load_strategy_data(DYNAMIC_DIR / "contention" / "run_summary.csv")
    dyn_div_data = load_strategy_data(DYNAMIC_DIR / "diversity" / "run_summary.csv")
    race_cont_data = load_strategy_data(RACING_DIR / "contention" / "run_summary.csv")
    race_div_data = load_strategy_data(RACING_DIR / "diversity" / "run_summary.csv")
    v1_cont_data = load_strategy_data(REC_V1_DIR / "contention" / "run_summary.csv")
    v1_div_data = load_strategy_data(REC_V1_DIR / "diversity" / "run_summary.csv")
    v2_cont_data = load_strategy_data(REC_V2_DIR / "contention" / "run_summary.csv")
    v2_div_data = load_strategy_data(REC_V2_DIR / "diversity" / "run_summary.csv")

    master_rows = []

    axes_cfg = [
        (
            "Contention", "C", p2_data, dyn_cont_data, race_cont_data, v1_cont_data, v2_cont_data,
            {
                0: "[1.50, 1.50, 1.50, 1.50]",
                25: "[1.61, 1.61, 1.61, 1.18]",
                50: "[1.72, 1.72, 1.72, 0.85]",
                75: "[1.83, 1.83, 1.83, 0.53]",
                100: "[1.93, 1.93, 1.93, 0.20]"
            }
        ),
        (
            "Diversity", "D", p3_data, dyn_div_data, race_div_data, v1_div_data, v2_div_data,
            {
                0: "[1.50, 1.50, 1.50, 1.50]",
                25: "[1.75, 1.63, 1.38, 1.25]",
                50: "[2.00, 1.75, 1.25, 1.00]",
                75: "[2.25, 1.88, 1.13, 0.75]",
                100: "[2.50, 2.00, 1.00, 0.50]"
            }
        ),
    ]

    for axis_name, prefix, phase_d, dyn_d, race_d, v1_d, v2_d, profiles in axes_cfg:
        for lvl in [0, 25, 50, 75, 100]:
            m_off, s_off = sample_stats(phase_d[(lvl, "False")])
            m_on, s_on = sample_stats(phase_d[(lvl, "True")])
            m_dyn, s_dyn = sample_stats(dyn_d[lvl])
            m_race, s_race = sample_stats(race_d[lvl])
            m_v1, s_v1 = sample_stats(v1_d[lvl])
            m_v2, s_v2 = sample_stats(v2_d[lvl])

            candidates = {
                "Spec OFF": m_off,
                "Spark ON": m_on,
                "Dynamic Sub-Div": m_dyn,
                "Racing Team": m_race,
                "Recursive V1": m_v1,
                "Recursive V2": m_v2,
            }
            winner = min(candidates, key=candidates.get)

            master_rows.append({
                "axis": axis_name,
                "condition": f"{prefix}{lvl}",
                "profile": profiles[lvl],
                "spec_off_mean": round(m_off, 2),
                "spec_off_std": round(s_off, 2),
                "spark_on_mean": round(m_on, 2),
                "spark_on_std": round(s_on, 2),
                "dynamic_subdiv_mean": round(m_dyn, 2),
                "dynamic_subdiv_std": round(s_dyn, 2),
                "racing_team_mean": round(m_race, 2),
                "racing_team_std": round(s_race, 2),
                "recursive_v1_mean": round(m_v1, 2),
                "recursive_v1_std": round(s_v1, 2),
                "recursive_v2_mean": round(m_v2, 2),
                "recursive_v2_std": round(s_v2, 2),
                "v2_vs_spark_pct": round(((m_on - m_v2) / m_on) * 100.0, 2),
                "v2_vs_v1_pct": round(((m_v1 - m_v2) / m_v1) * 100.0, 2),
                "best_strategy": winner,
            })

    out_csv = REC_V2_DIR / "master_6way_comparison.csv"
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(master_rows[0].keys()))
        writer.writeheader()
        writer.writerows(master_rows)

    print(f"\n[+] Saved 6-way master comparison table to: {out_csv}\n")

    header = (
        f"{'Axis':<10} | {'Cond':<4} | {'Spec OFF':<11} | {'Spark ON':<11} | "
        f"{'Dyn SubDv':<11} | {'Racing Tm':<11} | {'Rec V1':<11} | {'Rec V2 (Opt B)':<14} | {'V2 vs Spk':<9} | {'Winner'}"
    )
    print(header)
    print("-" * len(header))
    for r in master_rows:
        print(
            f"{r['axis']:<10} | {r['condition']:<4} | "
            f"{r['spec_off_mean']:>5.2f}±{r['spec_off_std']:<4.2f} | "
            f"{r['spark_on_mean']:>5.2f}±{r['spark_on_std']:<4.2f} | "
            f"{r['dynamic_subdiv_mean']:>5.2f}±{r['dynamic_subdiv_std']:<4.2f} | "
            f"{r['racing_team_mean']:>5.2f}±{r['racing_team_std']:<4.2f} | "
            f"{r['recursive_v1_mean']:>5.2f}±{r['recursive_v1_std']:<4.2f} | "
            f"{r['recursive_v2_mean']:>6.2f}±{r['recursive_v2_std']:<5.2f}  | "
            f"{r['v2_vs_spark_pct']:>+7.2f}% | {r['best_strategy']}"
        )

    # Generate 6-Bar Grand Figure
    os.environ["MPLCONFIGDIR"] = "/tmp"
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(19, 6.8))

    c_off = "#94a3b8"   # Slate Gray
    c_spk = "#f97316"   # Spark Orange
    c_dyn = "#10b981"   # Emerald Green
    c_race = "#0284c7"  # Sky Blue
    c_v1 = "#8b5cf6"    # Violet
    c_v2 = "#e11d48"    # Crimson Red (Champion Highlight)

    w = 0.135

    for ax, axis_name, title, xlabel in [
        (ax1, "Contention", "Axis 1: Single-Node Contention (C0 - C100)", "Contention Condition (Worker 4 Throttled)"),
        (ax2, "Diversity", "Axis 2: Hardware Diversity (D0 - D100)", "Hardware Diversity Condition (4-Tier Spread)")
    ]:
        rows = [r for r in master_rows if r["axis"] == axis_name]
        levels = [r["condition"] for r in rows]
        x = range(len(levels))

        ax.bar([i - 2.5*w for i in x], [r["spec_off_mean"] for r in rows], width=w, label="Speculation OFF", color=c_off)
        ax.bar([i - 1.5*w for i in x], [r["spark_on_mean"] for r in rows], width=w, label="Spark Speculation ON", color=c_spk)
        ax.bar([i - 0.5*w for i in x], [r["dynamic_subdiv_mean"] for r in rows], width=w, label="Dynamic Sub-Division", color=c_dyn)
        ax.bar([i + 0.5*w for i in x], [r["racing_team_mean"] for r in rows], width=w, label="Racing Team", color=c_race)
        ax.bar([i + 1.5*w for i in x], [r["recursive_v1_mean"] for r in rows], width=w, label="Recursive V1 (Disk)", color=c_v1)
        b6 = ax.bar([i + 2.5*w for i in x], [r["recursive_v2_mean"] for r in rows], width=w, label="Recursive V2 / Option B (Ours)", color=c_v2, edgecolor="#881337", linewidth=0.8)

        for bar in b6:
            h = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2, h + 1.5, f"{h:.1f}", ha="center", va="bottom", fontsize=7.5, fontweight="bold", color=c_v2)

        ax.set_title(title, fontsize=13, fontweight="bold")
        ax.set_xlabel(xlabel, fontsize=11, fontweight="bold")
        ax.set_ylabel("Job Completion Time (Seconds)", fontsize=11, fontweight="bold")
        ax.set_xticks(list(x))
        ax.set_xticklabels(levels, fontsize=10, fontweight="bold")
        ax.set_ylim(0, 168)
        ax.grid(axis="y", linestyle="--", alpha=0.4)
        ax.legend(fontsize=8.2, loc="upper left", ncol=2)

    fig.suptitle(
        "Grand 6-Way Real Docker Comparison (310 Runs): Spec OFF vs. Spark ON vs. Dynamic Sub-Div vs. Racing Team vs. Recursive V1 vs. Recursive V2 (Option B)",
        fontsize=13.5, fontweight="bold", y=0.98
    )
    plt.tight_layout(rect=[0, 0, 1, 0.94])

    out_png = REC_V2_DIR / "graph_6way_grand_comparison.png"
    plt.savefig(out_png, dpi=300)
    print(f"\n[+] Saved 6-way grand comparison chart to: {out_png}")


if __name__ == "__main__":
    main()
