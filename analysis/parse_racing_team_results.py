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
- results/racing_team/master_racing_team_comparison.csv
- results/racing_team/graph_racing_team_vs_spark.png
- results/racing_team/master_4way_comparison.csv
- results/racing_team/graph_4way_grand_comparison.png
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
from analysis.run_statistical_tests import welch_t_test


def _fmt_profiles(profiles_dict):
    return {k: "[" + ", ".join(f"{v:.2f}" for v in vals) + "]" for k, vals in profiles_dict.items()}
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

    c_profiles = _fmt_profiles(CONTENTION_PROFILES)

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

    d_profiles = _fmt_profiles(DIVERSITY_PROFILES)

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

    # Also regenerate 3-way Racing Team vs. Spark CSV with Welch's t-test
    rows_3way = []
    for axis_name, prefix, phase_d, race_d, profiles in [
        ("Contention", "C", p2_data, race_cont_data, c_profiles),
        ("Diversity", "D", p3_data, race_div_data, d_profiles),
    ]:
        for lvl in [0, 25, 50, 75, 100]:
            off_vals = phase_d[(lvl, "False")]
            on_vals = phase_d[(lvl, "True")]
            race_vals = race_d[lvl]
            m_off, s_off = sample_stats(off_vals)
            m_on, s_on = sample_stats(on_vals)
            m_race, s_race = sample_stats(race_vals)
            t_stat, _, p_val = welch_t_test(off_vals, race_vals)
            rows_3way.append({
                "axis": axis_name,
                "condition": f"{prefix}{lvl}",
                "profile": profiles[lvl],
                "spec_off_mean": round(m_off, 2),
                "spec_off_std": round(s_off, 2),
                "spec_on_mean": round(m_on, 2),
                "spec_on_std": round(s_on, 2),
                "racing_team_mean": round(m_race, 2),
                "racing_team_std": round(s_race, 2),
                "speedup_vs_off_pct": round(((m_off - m_race) / m_off) * 100.0, 2),
                "speedup_vs_spark_on_pct": round(((m_on - m_race) / m_on) * 100.0, 2),
                "welch_t_vs_off": round(t_stat, 2),
                "welch_p_vs_off": f"{p_val:.6f}" if p_val >= 1e-6 else f"{p_val:.2e}",
                "significant_05": "YES" if p_val < 0.05 else "NO",
            })

    out_3way_csv = RACING_DIR / "master_racing_team_comparison.csv"
    with open(out_3way_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows_3way[0].keys()))
        writer.writeheader()
        writer.writerows(rows_3way)

    print(f"\n[+] Saved 3-way master comparison table to: {out_3way_csv}")
    print(f"[+] Saved 4-way master comparison table to: {out_csv}\n")

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

    # Generate 3-Bar & 4-Bar Figures via shared helper
    try:
        os.environ["MPLCONFIGDIR"] = "/tmp"
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        def save_bar_plot(series_spec, title, out_path):
            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(17, 6.2))
            n_bars = len(series_spec)
            w = 0.80 / n_bars
            for ax, axis_name, sub_title, xlabel in [
                (ax1, "Contention", "Axis 1: Single-Node Contention (C0 - C100)", "Contention Condition"),
                (ax2, "Diversity", "Axis 2: Multi-Tier Hardware Diversity (D0 - D100)", "Hardware Diversity Condition"),
            ]:
                axis_rows = [r for r in master_rows if r["axis"] == axis_name]
                levels = [r["condition"] for r in axis_rows]
                x = range(len(levels))
                for idx, (field, label, color) in enumerate(series_spec):
                    offset = (idx - (n_bars - 1) / 2.0) * w
                    ax.bar([i + offset for i in x], [r[field] for r in axis_rows], width=w, label=label, color=color)
                ax.set_title(sub_title, fontsize=13, fontweight="bold")
                ax.set_xlabel(xlabel, fontsize=11, fontweight="bold")
                ax.set_ylabel("Job Completion Time (s)", fontsize=11, fontweight="bold")
                ax.set_xticks(list(x))
                ax.set_xticklabels(levels, fontweight="bold")
                ax.grid(axis="y", linestyle="--", alpha=0.5)
                ax.legend(fontsize=9.5)
            plt.suptitle(title, fontsize=14, fontweight="bold")
            plt.tight_layout()
            plt.savefig(out_path, dpi=300)
            plt.close()
            print(f"[+] Publication figure saved to: {out_path}")

        save_bar_plot(
            [
                ("spec_off_mean", "Speculation OFF", "#94a3b8"),
                ("spark_on_mean", "Spark Speculation ON", "#f97316"),
                ("racing_team_mean", "Racing Team Speculation", "#0284c7"),
            ],
            "Grand Synthesis: Spark Speculation vs. Racing Team Speculation (Real Docker Runs)",
            RACING_DIR / "graph_racing_team_vs_spark.png",
        )
        save_bar_plot(
            [
                ("spec_off_mean", "Speculation OFF", "#94a3b8"),
                ("spark_on_mean", "Spark Speculation ON", "#f97316"),
                ("dynamic_subdiv_mean", "Dynamic Sub-Division", "#10b981"),
                ("racing_team_mean", "Racing Team (Ours)", "#0284c7"),
            ],
            "Grand Synthesis: Spark Speculation vs. Dynamic Sub-Division vs. Racing Team (Real Docker Runs)",
            RACING_DIR / "graph_4way_grand_comparison.png",
        )

    except Exception as e:
        print(f"[!] Plot generation note: {e}")


if __name__ == "__main__":
    main()
