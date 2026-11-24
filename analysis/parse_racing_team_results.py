#!/usr/bin/env python3
"""Grand 4-Way Comparative Synthesis across Real Docker Cluster Runs.

Contrasts all 4 physical Docker cluster execution strategies:
1. Speculation OFF (Unmitigated)
2. Spark Speculation ON (Legacy Speculation)
3. Dynamic Sub-Division (Residual Slicing Strategy)
4. Racing Team Speculation (Parallel Team Speculation)

Evaluates:
- Axis 1: Contention (C0, C25, C50, C75, C100)
- Axis 2: Hardware Diversity (D0, D25, D50, D75, D100)

Generates:
- results/racing_team/master_4way_comparison.csv
- results/racing_team/graph_4way_grand_comparison.png
"""
import csv
import math
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RESULTS_DIR = ROOT / "results"
DYNAMIC_DIR = RESULTS_DIR / "dynamic_subdivision"
RACING_DIR = RESULTS_DIR / "racing_team"


def sample_stats(vals):
    n = len(vals)
    if n == 0: return 0.0, 0.0
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
    print("=" * 115)
    print("4-WAY GRAND REAL DOCKER SYNTHESIS: OFF vs. SPARK vs. DYNAMIC SUB-DIV vs. RACING TEAM")
    print("=" * 115)

    p2_csv = RESULTS_DIR / "phase2_contention" / "run_summary.csv"
    p3_csv = RESULTS_DIR / "phase3_diversity" / "run_summary.csv"
    dyn_cont_csv = DYNAMIC_DIR / "contention" / "run_summary.csv"
    dyn_div_csv = DYNAMIC_DIR / "diversity" / "run_summary.csv"
    race_cont_csv = RACING_DIR / "contention" / "run_summary.csv"
    race_div_csv = RACING_DIR / "diversity" / "run_summary.csv"

    p2_data = load_phase_data(p2_csv)
    p3_data = load_phase_data(p3_csv)
    dyn_cont_data = load_strategy_data(dyn_cont_csv)
    dyn_div_data = load_strategy_data(dyn_div_csv)
    race_cont_data = load_strategy_data(race_cont_csv)
    race_div_data = load_strategy_data(race_div_csv)

    master_rows = []

    c_profiles = {
        0: "[1.50, 1.50, 1.50, 1.50]",
        25: "[1.61, 1.61, 1.61, 1.18]",
        50: "[1.72, 1.72, 1.72, 0.85]",
        75: "[1.83, 1.83, 1.83, 0.53]",
        100: "[1.93, 1.93, 1.93, 0.20]"
    }

    for lvl in [0, 25, 50, 75, 100]:
        off_vals = p2_data[(lvl, "False")]
        on_vals = p2_data[(lvl, "True")]
        dyn_vals = dyn_cont_data[lvl]
        race_vals = race_cont_data[lvl]

        m_off, s_off = sample_stats(off_vals)
        m_on, s_on = sample_stats(on_vals)
        m_dyn, s_dyn = sample_stats(dyn_vals)
        m_race, s_race = sample_stats(race_vals)

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
            "racing_vs_spark_pct": round(((m_on - m_race) / m_on) * 100.0, 2),
            "racing_vs_dynamic_pct": round(((m_dyn - m_race) / m_dyn) * 100.0, 2)
        })

    d_profiles = {
        0: "[1.50, 1.50, 1.50, 1.50]",
        25: "[1.75, 1.63, 1.38, 1.25]",
        50: "[2.00, 1.75, 1.25, 1.00]",
        75: "[2.25, 1.88, 1.13, 0.75]",
        100: "[2.50, 2.00, 1.00, 0.50]"
    }

    for lvl in [0, 25, 50, 75, 100]:
        off_vals = p3_data[(lvl, "False")]
        on_vals = p3_data[(lvl, "True")]
        dyn_vals = dyn_div_data[lvl]
        race_vals = race_div_data[lvl]

        m_off, s_off = sample_stats(off_vals)
        m_on, s_on = sample_stats(on_vals)
        m_dyn, s_dyn = sample_stats(dyn_vals)
        m_race, s_race = sample_stats(race_vals)

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
            "racing_vs_spark_pct": round(((m_on - m_race) / m_on) * 100.0, 2),
            "racing_vs_dynamic_pct": round(((m_dyn - m_race) / m_dyn) * 100.0, 2)
        })

    out_csv = RACING_DIR / "master_4way_comparison.csv"
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(master_rows[0].keys()))
        writer.writeheader()
        writer.writerows(master_rows)

    print(f"\n[+] Saved 4-way master comparison table to: {out_csv}\n")

    header = f"{'Axis':<11} | {'Cond':<5} | {'Profile':<24} | {'Spec OFF (s)':<13} | {'Spark ON (s)':<13} | {'Dyn Sub-Div (s)':<15} | {'Racing Team (s)':<15} | {'Race vs Spark':<14} | {'Race vs Dyn'}"
    print(header)
    print("-" * len(header))
    for r in master_rows:
        off_s = f"{r['spec_off_mean']:.2f} ± {r['spec_off_std']:.2f}"
        on_s = f"{r['spark_on_mean']:.2f} ± {r['spark_on_std']:.2f}"
        dyn_s = f"{r['dynamic_subdiv_mean']:.2f} ± {r['dynamic_subdiv_std']:.2f}"
        race_s = f"{r['racing_team_mean']:.2f} ± {r['racing_team_std']:.2f}"
        spk_s = f"{r['racing_vs_spark_pct']:+.2f}%"
        dyn_pct_s = f"{r['racing_vs_dynamic_pct']:+.2f}%"
        print(f"{r['axis']:<11} | {r['condition']:<5} | {r['profile']:<24} | {off_s:<13} | {on_s:<13} | {dyn_s:<15} | {race_s:<15} | {spk_s:<14} | {dyn_pct_s}")

    # Generate 4-Bar Figure
    try:
        os.environ["MPLCONFIGDIR"] = "/tmp"
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(17, 6.2))

        # Colors: Slate Gray, Spark Orange, Emerald Green, Electric Sky Blue
        c_off = "#94a3b8"
        c_spk = "#f97316"
        c_dyn = "#10b981"
        c_race = "#0284c7"

        # Axis 1: Contention Plot
        c_rows = [r for r in master_rows if r["axis"] == "Contention"]
        c_levels = [r["condition"] for r in c_rows]
        x1 = range(len(c_levels))
        w = 0.20

        ax1.bar([i - 1.5*w for i in x1], [r["spec_off_mean"] for r in c_rows], width=w, label="Speculation OFF", color=c_off)
        ax1.bar([i - 0.5*w for i in x1], [r["spark_on_mean"] for r in c_rows], width=w, label="Spark Speculation ON", color=c_spk)
        ax1.bar([i + 0.5*w for i in x1], [r["dynamic_subdiv_mean"] for r in c_rows], width=w, label="Dynamic Sub-Division", color=c_dyn)
        ax1.bar([i + 1.5*w for i in x1], [r["racing_team_mean"] for r in c_rows], width=w, label="Racing Team (Ours)", color=c_race)

        ax1.set_title("Axis 1: Single-Node Contention (C0 - C100)", fontsize=13, fontweight="bold")
        ax1.set_xlabel("Contention Condition (Worker 4 Throttled)", fontsize=11, fontweight="bold")
        ax1.set_ylabel("Job Completion Time (seconds)", fontsize=11, fontweight="bold")
        ax1.set_xticks(x1)
        ax1.set_xticklabels(c_levels, fontweight="bold")
        ax1.grid(axis="y", linestyle="--", alpha=0.5)
        ax1.legend(fontsize=9.5)

        # Axis 2: Diversity Plot
        d_rows = [r for r in master_rows if r["axis"] == "Diversity"]
        d_levels = [r["condition"] for r in d_rows]
        x2 = range(len(d_levels))

        ax2.bar([i - 1.5*w for i in x2], [r["spec_off_mean"] for r in d_rows], width=w, label="Speculation OFF", color=c_off)
        ax2.bar([i - 0.5*w for i in x2], [r["spark_on_mean"] for r in d_rows], width=w, label="Spark Speculation ON", color=c_spk)
        ax2.bar([i + 0.5*w for i in x2], [r["dynamic_subdiv_mean"] for r in d_rows], width=w, label="Dynamic Sub-Division", color=c_dyn)
        ax2.bar([i + 1.5*w for i in x2], [r["racing_team_mean"] for r in d_rows], width=w, label="Racing Team (Ours)", color=c_race)

        ax2.set_title("Axis 2: Multi-Tier Hardware Diversity (D0 - D100)", fontsize=13, fontweight="bold")
        ax2.set_xlabel("Hardware Diversity Level (Worker Tier Spread)", fontsize=11, fontweight="bold")
        ax2.set_ylabel("Job Completion Time (seconds)", fontsize=11, fontweight="bold")
        ax2.set_xticks(x2)
        ax2.set_xticklabels(d_levels, fontweight="bold")
        ax2.grid(axis="y", linestyle="--", alpha=0.5)
        ax2.legend(fontsize=9.5)

        plt.suptitle("Grand Synthesis: Spark Speculation vs. Dynamic Sub-Division vs. Racing Team (Real Docker Runs)", fontsize=14, fontweight="bold")
        plt.tight_layout()

        plot_path = RACING_DIR / "graph_4way_grand_comparison.png"
        plt.savefig(plot_path, dpi=300)
        plt.close()
        print(f"[+] 4-Bar publication figure saved to: {plot_path}")

    except Exception as e:
        print(f"[!] Plot generation note: {e}")


if __name__ == "__main__":
    main()
