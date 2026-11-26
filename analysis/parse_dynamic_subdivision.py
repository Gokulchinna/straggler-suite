#!/usr/bin/env python3
"""Comparative Analysis and Statistical Synthesis for Dynamic Sub-Division.

Contrasts:
1. Speculation OFF (Unmitigated)
2. Spark Speculation ON (Legacy Full Clone)
3. Dynamic Sub-Division (Capacity-Aware & Residual Slicing)

Evaluates:
- Axis 1: Contention (C0, C25, C50, C75, C100)
- Axis 2: Hardware Diversity (D0, D25, D50, D75, D100)

Generates:
- results/dynamic_subdivision/master_dynamic_comparison.csv
- results/dynamic_subdivision/graph_dynamic_vs_speculation.png
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

# Pure-Python statistical routines (from run_statistical_tests.py)
def betacf(a, b, x):
    MAXIT = 100
    EPS = 3.0e-7
    FPMIN = 1.0e-30
    qab = a + b
    qap = a + 1.0
    qam = a - 1.0
    c = 1.0
    d = 1.0 - qab * x / qap
    if abs(d) < FPMIN: d = FPMIN
    d = 1.0 / d
    h = d
    for m in range(1, MAXIT + 1):
        m2 = 2 * m
        aa = m * (b - m) * x / ((qam + m2) * (a + m2))
        d = 1.0 + aa * d
        if abs(d) < FPMIN: d = FPMIN
        c = 1.0 + aa / c
        if abs(c) < FPMIN: c = FPMIN
        d = 1.0 / d
        h *= d * c
        aa = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2))
        d = 1.0 + aa * d
        if abs(d) < FPMIN: d = FPMIN
        c = 1.0 + aa / c
        if abs(c) < FPMIN: c = FPMIN
        d = 1.0 / d
        del_val = d * c
        h *= del_val
        if abs(del_val - 1.0) < EPS: break
    return h


def ibeta(a, b, x):
    if x <= 0.0: return 0.0
    if x >= 1.0: return 1.0
    bt = math.exp(math.lgamma(a + b) - math.lgamma(a) - math.lgamma(b) + a * math.log(x) + b * math.log(1.0 - x))
    if x < (a + 1.0) / (a + b + 2.0):
        return bt * betacf(a, b, x) / a
    else:
        return 1.0 - bt * betacf(b, a, 1.0 - x) / b


def t_pvalue(t_val, df):
    if df <= 0: return 1.0
    x = df / (df + t_val * t_val)
    return ibeta(0.5 * df, 0.5, x)


def sample_stats(vals):
    n = len(vals)
    if n == 0: return 0.0, 0.0
    m = sum(vals) / n
    var = sum((x - m) ** 2 for x in vals) / (n - 1) if n > 1 else 0.0
    return m, math.sqrt(var)


def welch_t_test(x, y):
    n1, n2 = len(x), len(y)
    m1, s1 = sample_stats(x)
    m2, s2 = sample_stats(y)
    v1, v2 = s1 * s1, s2 * s2
    se = math.sqrt(v1 / n1 + v2 / n2)
    t = (m1 - m2) / se if se > 0 else 0.0
    denom = ((v1 / n1) ** 2 / (n1 - 1)) + ((v2 / n2) ** 2 / (n2 - 1))
    df = ((v1 / n1 + v2 / n2) ** 2) / denom if denom > 0 else 1.0
    p = t_pvalue(t, df)
    return t, df, p


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


def load_dynamic_data(csv_path):
    data = {}
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            lvl = int(row.get("level_pct", 0))
            jct = float(row["jct_s"])
            data.setdefault(lvl, []).append(jct)
    return data


def main():
    print("=" * 100)
    print("MASTER COMPARATIVE SYNTHESIS: SPEC OFF vs. SPARK SPEC ON vs. DYNAMIC SUB-DIVISION")
    print("=" * 100)

    # File paths
    p2_csv = RESULTS_DIR / "phase2_contention" / "run_summary.csv"
    p3_csv = RESULTS_DIR / "phase3_diversity" / "run_summary.csv"
    dyn_cont_csv = DYNAMIC_DIR / "contention" / "run_summary.csv"
    dyn_div_csv = DYNAMIC_DIR / "diversity" / "run_summary.csv"

    p2_data = load_phase_data(p2_csv)
    p3_data = load_phase_data(p3_csv)
    dyn_cont_data = load_dynamic_data(dyn_cont_csv)
    dyn_div_data = load_dynamic_data(dyn_div_csv)

    master_rows = []

    # Process Axis 1: Contention
    print("\n--- AXIS 1: SINGLE-NODE CONTENTION (Worker 4 Throttled) ---")
    c_profiles = _fmt_profiles(CONTENTION_PROFILES)

    for lvl in [0, 25, 50, 75, 100]:
        off_vals = p2_data[(lvl, "False")]
        on_vals = p2_data[(lvl, "True")]
        dyn_vals = dyn_cont_data[lvl]

        m_off, s_off = sample_stats(off_vals)
        m_on, s_on = sample_stats(on_vals)
        m_dyn, s_dyn = sample_stats(dyn_vals)

        # Speedups
        speedup_vs_off = ((m_off - m_dyn) / m_off) * 100.0
        speedup_vs_on = ((m_on - m_dyn) / m_on) * 100.0

        t_stat, df, p_val = welch_t_test(off_vals, dyn_vals)
        sig = "YES" if p_val < 0.05 else "NO"

        master_rows.append({
            "axis": "Contention",
            "condition": f"C{lvl}",
            "profile": c_profiles[lvl],
            "spec_off_mean": round(m_off, 2),
            "spec_off_std": round(s_off, 2),
            "spec_on_mean": round(m_on, 2),
            "spec_on_std": round(s_on, 2),
            "dynamic_mean": round(m_dyn, 2),
            "dynamic_std": round(s_dyn, 2),
            "speedup_vs_off_pct": round(speedup_vs_off, 2),
            "speedup_vs_spark_on_pct": round(speedup_vs_on, 2),
            "welch_t_vs_off": round(t_stat, 2),
            "welch_p_vs_off": f"{p_val:.6f}" if p_val >= 1e-6 else f"{p_val:.2e}",
            "significant_05": sig
        })

    # Process Axis 2: Hardware Diversity
    print("\n--- AXIS 2: MULTI-TIER HARDWARE DIVERSITY ---")
    d_profiles = _fmt_profiles(DIVERSITY_PROFILES)

    for lvl in [0, 25, 50, 75, 100]:
        off_vals = p3_data[(lvl, "False")]
        on_vals = p3_data[(lvl, "True")]
        dyn_vals = dyn_div_data[lvl]

        m_off, s_off = sample_stats(off_vals)
        m_on, s_on = sample_stats(on_vals)
        m_dyn, s_dyn = sample_stats(dyn_vals)

        speedup_vs_off = ((m_off - m_dyn) / m_off) * 100.0
        speedup_vs_on = ((m_on - m_dyn) / m_on) * 100.0

        t_stat, df, p_val = welch_t_test(off_vals, dyn_vals)
        sig = "YES" if p_val < 0.05 else "NO"

        master_rows.append({
            "axis": "Diversity",
            "condition": f"D{lvl}",
            "profile": d_profiles[lvl],
            "spec_off_mean": round(m_off, 2),
            "spec_off_std": round(s_off, 2),
            "spec_on_mean": round(m_on, 2),
            "spec_on_std": round(s_on, 2),
            "dynamic_mean": round(m_dyn, 2),
            "dynamic_std": round(s_dyn, 2),
            "speedup_vs_off_pct": round(speedup_vs_off, 2),
            "speedup_vs_spark_on_pct": round(speedup_vs_on, 2),
            "welch_t_vs_off": round(t_stat, 2),
            "welch_p_vs_off": f"{p_val:.6f}" if p_val >= 1e-6 else f"{p_val:.2e}",
            "significant_05": sig
        })

    # Save to master CSV
    out_csv = DYNAMIC_DIR / "master_dynamic_comparison.csv"
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(master_rows[0].keys()))
        writer.writeheader()
        writer.writerows(master_rows)

    print(f"\n[+] Saved master comparison table to: {out_csv}\n")

    # Print Table
    header = f"{'Axis':<11} | {'Cond':<5} | {'Profile':<24} | {'Spec OFF (s)':<14} | {'Spark ON (s)':<14} | {'Dynamic (s)':<14} | {'Speedup vs OFF':<15} | {'Speedup vs ON':<14} | {'Sig'}"
    print(header)
    print("-" * len(header))
    for r in master_rows:
        off_str = f"{r['spec_off_mean']:.2f} ± {r['spec_off_std']:.2f}"
        on_str = f"{r['spec_on_mean']:.2f} ± {r['spec_on_std']:.2f}"
        dyn_str = f"{r['dynamic_mean']:.2f} ± {r['dynamic_std']:.2f}"
        sp_off = f"{r['speedup_vs_off_pct']:+.2f}%"
        sp_on = f"{r['speedup_vs_spark_on_pct']:+.2f}%"
        print(f"{r['axis']:<11} | {r['condition']:<5} | {r['profile']:<24} | {off_str:<14} | {on_str:<14} | {dyn_str:<14} | {sp_off:<15} | {sp_on:<14} | {r['significant_05']}")

    # Generate Publication Chart with Matplotlib
    try:
        os.environ["MPLCONFIGDIR"] = "/tmp"
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

        # Axis 1: Contention Plot
        c_rows = [r for r in master_rows if r["axis"] == "Contention"]
        c_levels = [r["condition"] for r in c_rows]
        c_off = [r["spec_off_mean"] for r in c_rows]
        c_on = [r["spec_on_mean"] for r in c_rows]
        c_dyn = [r["dynamic_mean"] for r in c_rows]

        x = range(len(c_levels))
        width = 0.25

        ax1.bar([i - width for i in x], c_off, width=width, label="Speculation OFF", color="#94a3b8")
        ax1.bar(x, c_on, width=width, label="Spark Speculation ON", color="#f97316")
        ax1.bar([i + width for i in x], c_dyn, width=width, label="Dynamic Sub-Division (Ours)", color="#22c55e")
        ax1.set_title("Axis 1: Single-Node Contention (C0 - C100)", fontsize=13, fontweight="bold")
        ax1.set_xlabel("Contention Condition", fontsize=11)
        ax1.set_ylabel("Job Completion Time (s)", fontsize=11)
        ax1.set_xticks(x)
        ax1.set_xticklabels(c_levels)
        ax1.grid(axis="y", linestyle="--", alpha=0.5)
        ax1.legend()

        # Axis 2: Diversity Plot
        d_rows = [r for r in master_rows if r["axis"] == "Diversity"]
        d_levels = [r["condition"] for r in d_rows]
        d_off = [r["spec_off_mean"] for r in d_rows]
        d_on = [r["spec_on_mean"] for r in d_rows]
        d_dyn = [r["dynamic_mean"] for r in d_rows]

        ax2.bar([i - width for i in x], d_off, width=width, label="Speculation OFF", color="#94a3b8")
        ax2.bar(x, d_on, width=width, label="Spark Speculation ON", color="#f97316")
        ax2.bar([i + width for i in x], d_dyn, width=width, label="Dynamic Sub-Division (Ours)", color="#22c55e")
        ax2.set_title("Axis 2: Multi-Tier Hardware Diversity (D0 - D100)", fontsize=13, fontweight="bold")
        ax2.set_xlabel("Hardware Diversity Condition", fontsize=11)
        ax2.set_ylabel("Job Completion Time (s)", fontsize=11)
        ax2.set_xticks(x)
        ax2.set_xticklabels(d_levels)
        ax2.grid(axis="y", linestyle="--", alpha=0.5)
        ax2.legend()

        plt.suptitle("Grand Synthesis: Spark Speculation vs. Dynamic Sub-Division Framework", fontsize=15, fontweight="bold")
        plt.tight_layout()

        plot_path = DYNAMIC_DIR / "graph_dynamic_vs_speculation.png"
        plt.savefig(plot_path, dpi=300)
        plt.close()
        print(f"[+] Publication figure saved to: {plot_path}")

    except Exception as e:
        print(f"[!] Chart generation note: {e}")


if __name__ == "__main__":
    main()
