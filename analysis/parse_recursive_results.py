#!/usr/bin/env python3
"""Grand 5-Way Comparative Synthesis across Real Docker Cluster Runs.

Contrasts all 5 physical Docker cluster execution strategies:
1. Speculation OFF (Unmitigated)
2. Spark Speculation ON (Legacy Speculation)
3. Dynamic Sub-Division (Single-Round Residual Slicing)
4. Racing Team Speculation (Parallel Team Speculation)
5. Recursive Sub-Division (Multi-Round Hierarchical Work-Stealing)

Evaluates:
- Axis 1: Contention (C0, C25, C50, C75, C100)
- Axis 2: Hardware Diversity (D0, D25, D50, D75, D100)

Generates:
- results/recursive_subdivision/master_5way_comparison.csv
- results/recursive_subdivision/graph_5way_grand_comparison.png
"""
import csv
import math
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from controllers.set_contention import CONTENTION_PROFILES
from controllers.set_diversity import DIVERSITY_PROFILES


def _fmt_profiles(profiles_dict):
    return {k: "[" + ", ".join(f"{v:.2f}" for v in vals) + "]" for k, vals in profiles_dict.items()}
RESULTS_DIR = ROOT / "results"
DYNAMIC_DIR = RESULTS_DIR / "dynamic_subdivision"
RACING_DIR = RESULTS_DIR / "racing_team"
RECURSIVE_DIR = RESULTS_DIR / "recursive_subdivision"


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
    print("=" * 130)
    print("5-WAY GRAND REAL DOCKER SYNTHESIS: OFF vs. SPARK vs. DYNAMIC SUB-DIV vs. RACING TEAM vs. RECURSIVE SUB-DIV")
    print("=" * 130)

    p2_data = load_phase_data(RESULTS_DIR / "phase2_contention" / "run_summary.csv")
    p3_data = load_phase_data(RESULTS_DIR / "phase3_diversity" / "run_summary.csv")
    dyn_cont_data = load_strategy_data(DYNAMIC_DIR / "contention" / "run_summary.csv")
    dyn_div_data = load_strategy_data(DYNAMIC_DIR / "diversity" / "run_summary.csv")
    race_cont_data = load_strategy_data(RACING_DIR / "contention" / "run_summary.csv")
    race_div_data = load_strategy_data(RACING_DIR / "diversity" / "run_summary.csv")
    rec_cont_data = load_strategy_data(RECURSIVE_DIR / "contention" / "run_summary.csv")
    rec_div_data = load_strategy_data(RECURSIVE_DIR / "diversity" / "run_summary.csv")

    master_rows = []

    c_profiles = _fmt_profiles(CONTENTION_PROFILES)

    for lvl in [0, 25, 50, 75, 100]:
        m_off, s_off = sample_stats(p2_data[(lvl, "False")])
        m_on, s_on = sample_stats(p2_data[(lvl, "True")])
        m_dyn, s_dyn = sample_stats(dyn_cont_data[lvl])
        m_race, s_race = sample_stats(race_cont_data[lvl])
        m_rec, s_rec = sample_stats(rec_cont_data[lvl])

        master_rows.append({
            "axis": "Contention",
            "condition": f"C{lvl}",
            "profile": c_profiles[lvl],
            "spec_off_mean": round(m_off, 2),
            "spec_off_std": round(s_off, 2),
            "spark_on_mean": round(m_on, 2),
            "spark_on_std": round(s_on, 2),
            "dynamic_subdiv_mean": round(m_dyn, 2),
            "dynamic_subdiv_std": round(s_dyn, 2),
            "racing_team_mean": round(m_race, 2),
            "racing_team_std": round(s_race, 2),
            "recursive_subdiv_mean": round(m_rec, 2),
            "recursive_subdiv_std": round(s_rec, 2),
            "rec_vs_spark_pct": round(((m_on - m_rec) / m_on) * 100.0, 2),
            "rec_vs_dynamic_pct": round(((m_dyn - m_rec) / m_dyn) * 100.0, 2),
        })

    d_profiles = _fmt_profiles(DIVERSITY_PROFILES)

    for lvl in [0, 25, 50, 75, 100]:
        m_off, s_off = sample_stats(p3_data[(lvl, "False")])
        m_on, s_on = sample_stats(p3_data[(lvl, "True")])
        m_dyn, s_dyn = sample_stats(dyn_div_data[lvl])
        m_race, s_race = sample_stats(race_div_data[lvl])
        m_rec, s_rec = sample_stats(rec_div_data[lvl])

        master_rows.append({
            "axis": "Diversity",
            "condition": f"D{lvl}",
            "profile": d_profiles[lvl],
            "spec_off_mean": round(m_off, 2),
            "spec_off_std": round(s_off, 2),
            "spark_on_mean": round(m_on, 2),
            "spark_on_std": round(s_on, 2),
            "dynamic_subdiv_mean": round(m_dyn, 2),
            "dynamic_subdiv_std": round(s_dyn, 2),
            "racing_team_mean": round(m_race, 2),
            "racing_team_std": round(s_race, 2),
            "recursive_subdiv_mean": round(m_rec, 2),
            "recursive_subdiv_std": round(s_rec, 2),
            "rec_vs_spark_pct": round(((m_on - m_rec) / m_on) * 100.0, 2),
            "rec_vs_dynamic_pct": round(((m_dyn - m_rec) / m_dyn) * 100.0, 2),
        })

    out_csv = RECURSIVE_DIR / "master_5way_comparison.csv"
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(master_rows[0].keys()))
        writer.writeheader()
        writer.writerows(master_rows)

    print(f"\n[+] Saved 5-way master comparison table to: {out_csv}\n")

    header = (
        f"{'Axis':<10} | {'Cond':<4} | {'Spec OFF':<12} | {'Spark ON':<12} | "
        f"{'Dyn SubDiv':<12} | {'Racing Tm':<12} | {'Recursive':<12} | {'Rec vs Spark':<12} | {'Rec vs Dyn'}"
    )
    print(header)
    print("-" * len(header))
    for r in master_rows:
        print(
            f"{r['axis']:<10} | {r['condition']:<4} | "
            f"{r['spec_off_mean']:>5.2f}±{r['spec_off_std']:<5.2f} | "
            f"{r['spark_on_mean']:>5.2f}±{r['spark_on_std']:<5.2f} | "
            f"{r['dynamic_subdiv_mean']:>5.2f}±{r['dynamic_subdiv_std']:<5.2f} | "
            f"{r['racing_team_mean']:>5.2f}±{r['racing_team_std']:<5.2f} | "
            f"{r['recursive_subdiv_mean']:>5.2f}±{r['recursive_subdiv_std']:<5.2f} | "
            f"{r['rec_vs_spark_pct']:>+10.2f}% | {r['rec_vs_dynamic_pct']:>+9.2f}%"
        )

    # Generate 5-Bar Figure
    os.environ["MPLCONFIGDIR"] = "/tmp"
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 6.5))

    c_off = "#94a3b8"   # Slate Gray
    c_spk = "#f97316"   # Spark Orange
    c_dyn = "#10b981"   # Emerald Green
    c_race = "#0284c7"  # Sky Blue
    c_rec = "#7c3aed"   # Royal Violet

    w = 0.16

    for ax, axis_name, title, xlabel in [
        (ax1, "Contention", "Axis 1: Single-Node Contention (C0 - C100)", "Contention Condition (Worker 4 Throttled)"),
        (ax2, "Diversity", "Axis 2: Hardware Diversity (D0 - D100)", "Hardware Diversity Condition (4-Tier Spread)")
    ]:
        rows = [r for r in master_rows if r["axis"] == axis_name]
        levels = [r["condition"] for r in rows]
        x = range(len(levels))

        b1 = ax.bar([i - 2.0*w for i in x], [r["spec_off_mean"] for r in rows], width=w, label="Speculation OFF", color=c_off)
        b2 = ax.bar([i - 1.0*w for i in x], [r["spark_on_mean"] for r in rows], width=w, label="Spark Speculation ON", color=c_spk)
        b3 = ax.bar([i + 0.0*w for i in x], [r["dynamic_subdiv_mean"] for r in rows], width=w, label="Dynamic Sub-Division", color=c_dyn)
        b4 = ax.bar([i + 1.0*w for i in x], [r["racing_team_mean"] for r in rows], width=w, label="Racing Team Speculation", color=c_race)
        b5 = ax.bar([i + 2.0*w for i in x], [r["recursive_subdiv_mean"] for r in rows], width=w, label="Recursive Sub-Division (Ours)", color=c_rec)

        for bar in b5:
            h = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2, h + 1.5, f"{h:.1f}", ha="center", va="bottom", fontsize=7.5, fontweight="bold", color=c_rec)

        ax.set_title(title, fontsize=13, fontweight="bold")
        ax.set_xlabel(xlabel, fontsize=11, fontweight="bold")
        ax.set_ylabel("Job Completion Time (Seconds)", fontsize=11, fontweight="bold")
        ax.set_xticks(list(x))
        ax.set_xticklabels(levels, fontsize=10, fontweight="bold")
        ax.set_ylim(0, 165)
        ax.grid(axis="y", linestyle="--", alpha=0.4)
        ax.legend(fontsize=8.5, loc="upper left")

    fig.suptitle(
        "Grand 5-Way Real Docker Comparison: Spark Speculation vs. Dynamic Sub-Division vs. Racing Team vs. Recursive Sub-Division",
        fontsize=14.5, fontweight="bold", y=0.98
    )
    plt.tight_layout(rect=[0, 0, 1, 0.94])

    out_png = RECURSIVE_DIR / "graph_5way_grand_comparison.png"
    plt.savefig(out_png, dpi=300)
    print(f"\n[+] Saved 5-way grand comparison chart to: {out_png}")


if __name__ == "__main__":
    main()
