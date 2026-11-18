#!/usr/bin/env python3
"""
run_statistical_tests.py
------------------------
Performs formal inferential statistical tests across all 11 experimental conditions:
1. Welch's Independent Two-Sample t-test (unequal variances)
2. Paired t-test (sequential repetition pairing)

Generates:
- results/statistical_significance_tests.csv
"""

import csv
import math
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RESULTS_DIR = ROOT / "results"

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

def paired_t_test(x, y):
    n = min(len(x), len(y))
    diffs = [x[i] - y[i] for i in range(n)]
    m_d, s_d = sample_stats(diffs)
    se = s_d / math.sqrt(n) if n > 0 else 0.0
    t = m_d / se if se > 0 else 0.0
    df = n - 1
    p = t_pvalue(t, df)
    return t, df, p

def load_phase_runs(summary_csv):
    data = {}
    with open(summary_csv, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if "warmup" in row["run_label"]:
                continue
            lvl = row.get("level_pct", row.get("config", "0"))
            spec = row["speculation"]
            jct = float(row["jct_s"])
            data.setdefault((lvl, spec), []).append(jct)
    return data

def main():
    print("=" * 85)
    print("INFERENTIAL STATISTICAL SIGNIFICANCE TESTS (WORKLOAD JCT)")
    print("=" * 85)

    test_results = []

    # Phase 1
    p1_file = RESULTS_DIR / "phase1_baseline" / "run_summary.csv"
    p1_off, p1_on = [], []
    with open(p1_file) as f:
        for r in csv.DictReader(f):
            if "warmup" in r["run_label"]: continue
            if r["speculation"] == "False": p1_off.append(float(r["jct_s"]))
            else: p1_on.append(float(r["jct_s"]))

    wt, wdf, wp = welch_t_test(p1_off, p1_on)
    pt, pdf, pp = paired_t_test(p1_off, p1_on)
    test_results.append({
        "phase": "Phase 1: Baseline",
        "condition": "Fixed Worker 4 (0.5c)",
        "welch_t": round(wt, 3),
        "welch_df": round(wdf, 3),
        "welch_p": round(wp, 5),
        "paired_t": round(pt, 3),
        "paired_df": pdf,
        "paired_p": round(pp, 5),
        "statistically_significant_05": "YES" if wp < 0.05 else "NO"
    })

    # Phase 2
    p2_file = RESULTS_DIR / "phase2_contention" / "run_summary.csv"
    p2_data = load_phase_runs(p2_file)
    for lvl in [0, 25, 50, 75, 100]:
        off_vals = p2_data[(str(lvl), "False")]
        on_vals = p2_data[(str(lvl), "True")]
        wt, wdf, wp = welch_t_test(off_vals, on_vals)
        pt, pdf, pp = paired_t_test(off_vals, on_vals)
        test_results.append({
            "phase": "Phase 2: Contention",
            "condition": f"{lvl}% Contention",
            "welch_t": round(wt, 3),
            "welch_df": round(wdf, 3),
            "welch_p": f"{wp:.8f}" if wp < 0.0001 else f"{wp:.5f}",
            "paired_t": round(pt, 3),
            "paired_df": pdf,
            "paired_p": f"{pp:.8f}" if pp < 0.0001 else f"{pp:.5f}",
            "statistically_significant_05": "YES" if wp < 0.05 else "NO"
        })

    # Phase 3
    p3_file = RESULTS_DIR / "phase3_diversity" / "run_summary.csv"
    p3_data = load_phase_runs(p3_file)
    for lvl in [0, 25, 50, 75, 100]:
        off_vals = p3_data[(str(lvl), "False")]
        on_vals = p3_data[(str(lvl), "True")]
        wt, wdf, wp = welch_t_test(off_vals, on_vals)
        pt, pdf, pp = paired_t_test(off_vals, on_vals)
        test_results.append({
            "phase": "Phase 3: Hardware Diversity",
            "condition": f"{lvl}% Diversity",
            "welch_t": round(wt, 3),
            "welch_df": round(wdf, 3),
            "welch_p": round(wp, 5),
            "paired_t": round(pt, 3),
            "paired_df": pdf,
            "paired_p": round(pp, 5),
            "statistically_significant_05": "YES" if wp < 0.05 else "NO"
        })

    # Save to CSV
    out_csv = RESULTS_DIR / "statistical_significance_tests.csv"
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(test_results[0].keys()))
        writer.writeheader()
        writer.writerows(test_results)
    print(f"[+] Saved statistical test results to: {out_csv}\n")

    # Print Table
    print(f"{'Condition':<25} | {'Welch t':<8} | {'df':<6} | {'Welch p':<12} | {'Paired t':<9} | {'Paired p':<10} | {'Sig (p<0.05)'}")
    print("-" * 92)
    for r in test_results:
        print(f"{r['condition']:<25} | {r['welch_t']:<8} | {r['welch_df']:<6} | {str(r['welch_p']):<12} | {r['paired_t']:<9} | {str(r['paired_p']):<10} | {r['statistically_significant_05']}")

if __name__ == "__main__":
    main()
