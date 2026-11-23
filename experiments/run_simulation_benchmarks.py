#!/usr/bin/env python3
"""Simulation Benchmark Runner for Comparative Mitigation Analysis.

Evaluates 4 policies across all 11 experimental conditions:
1. UNMITIGATED (Speculation OFF)
2. SPARK_SPECULATION (Legacy Median Clone & Kill)
3. DYNAMIC_SUBDIVISION (Capacity-Aware Sub-Division with Dead-Node Eviction)
4. HYBRID (Adaptive Hybrid Strategy)

Generates:
- results/dynamic_subdivision/simulation_master_comparison.csv
"""
import csv
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from simulation.engine import ClusterSimulator
OUTPUT_CSV = ROOT / "results" / "dynamic_subdivision" / "simulation_master_comparison.csv"

CONDITIONS = [
    # Axis, Label, Profile
    ("Baseline", "Phase 1 Reference", [1.50, 1.50, 1.50, 0.50]),
    ("Contention", "C0", [1.50, 1.50, 1.50, 1.50]),
    ("Contention", "C25", [1.61, 1.61, 1.61, 1.18]),
    ("Contention", "C50", [1.72, 1.72, 1.72, 0.85]),
    ("Contention", "C75", [1.83, 1.83, 1.83, 0.53]),
    ("Contention", "C100", [1.93, 1.93, 1.93, 0.20]),
    ("Diversity", "D0", [1.50, 1.50, 1.50, 1.50]),
    ("Diversity", "D25", [1.75, 1.63, 1.38, 1.25]),
    ("Diversity", "D50", [2.00, 1.75, 1.25, 1.00]),
    ("Diversity", "D75", [2.25, 1.88, 1.13, 0.75]),
    ("Diversity", "D100", [2.50, 2.00, 1.00, 0.50]),
]


def main():
    print("=" * 90)
    print("RUNNING FULL CLUSTER SIMULATION BENCHMARK (11 CONDITIONS x 4 POLICIES)")
    print("=" * 90)

    rows = []
    for axis, label, profile in CONDITIONS:
        print(f"\nCondition: {label:20} Profile: {profile}")
        sim = ClusterSimulator(node_quotas=profile)

        unm = sim.run("UNMITIGATED", label)
        spec = sim.run("SPARK_SPECULATION", label)
        dyn = sim.run("DYNAMIC_SUBDIVISION", label)
        hyb = sim.run("HYBRID", label)

        dyn_speedup_vs_unm = round(((unm.jct_seconds - dyn.jct_seconds) / unm.jct_seconds) * 100, 2)
        dyn_speedup_vs_spec = round(((spec.jct_seconds - dyn.jct_seconds) / spec.jct_seconds) * 100, 2)

        hyb_speedup_vs_unm = round(((unm.jct_seconds - hyb.jct_seconds) / unm.jct_seconds) * 100, 2)
        hyb_speedup_vs_spec = round(((spec.jct_seconds - hyb.jct_seconds) / spec.jct_seconds) * 100, 2)

        print(f"  Unmitigated: {unm.jct_seconds:6.1f}s | Wasted: {unm.wasted_cpu_seconds:5.1f}s")
        print(f"  Spark Spec : {spec.jct_seconds:6.1f}s | Wasted: {spec.wasted_cpu_seconds:5.1f}s")
        print(f"  Dynamic Sub: {dyn.jct_seconds:6.1f}s | Speedup vs Spec: {dyn_speedup_vs_spec:+6.2f}% | Wasted: {dyn.wasted_cpu_seconds:5.1f}s")
        print(f"  Hybrid Mode: {hyb.jct_seconds:6.1f}s | Speedup vs Spec: {hyb_speedup_vs_spec:+6.2f}% | Wasted: {hyb.wasted_cpu_seconds:5.1f}s")

        rows.append({
            "axis": axis,
            "condition": label,
            "profile": str(profile),
            "unmitigated_jct_s": unm.jct_seconds,
            "spark_spec_jct_s": spec.jct_seconds,
            "dynamic_jct_s": dyn.jct_seconds,
            "hybrid_jct_s": hyb.jct_seconds,
            "dynamic_speedup_vs_unm_pct": dyn_speedup_vs_unm,
            "dynamic_speedup_vs_spec_pct": dyn_speedup_vs_spec,
            "hybrid_speedup_vs_unm_pct": hyb_speedup_vs_unm,
            "hybrid_speedup_vs_spec_pct": hyb_speedup_vs_spec,
            "dynamic_wasted_cpu_s": dyn.wasted_cpu_seconds,
            "spark_wasted_cpu_s": spec.wasted_cpu_seconds,
        })

    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        fieldnames = list(rows[0].keys())
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\n[SUCCESS] Simulation results saved to: {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
