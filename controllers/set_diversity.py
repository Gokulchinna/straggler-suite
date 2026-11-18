#!/usr/bin/env python3
"""Cluster-Wide Hardware Diversity Controller for Straggler Suite (Phase 3).

Configures all 4 workers into distinct multi-tier hardware performance classes
across 5 diversity levels (0%, 25%, 50%, 75%, 100%) while strictly conserving
total cluster CPU capacity at 6.0 CPUs.

Diversity Levels:
   0% Diversity : [1.50, 1.50, 1.50, 1.50] CPUs (Pure Homogeneous, CV=0.00)
  25% Diversity : [1.88, 1.63, 1.38, 1.11] CPUs (Mild Hardware Spread, CV=0.19)
  50% Diversity : [2.06, 1.69, 1.31, 0.94] CPUs (Moderate Spread, CV=0.28)
  75% Diversity : [2.25, 1.75, 1.25, 0.75] CPUs (High Spread, CV=0.37)
 100% Diversity : [2.50, 2.00, 1.00, 0.50] CPUs (Severe Multi-Tier Spread, CV=0.53)
"""
import argparse
import subprocess
import sys

WORKER_CONTAINERS = ["spark-worker-1", "spark-worker-2", "spark-worker-3", "spark-worker-4"]

DIVERSITY_PROFILES = {
    0:   [1.50, 1.50, 1.50, 1.50],
    25:  [1.88, 1.63, 1.38, 1.11],
    50:  [2.06, 1.69, 1.31, 0.94],
    75:  [2.25, 1.75, 1.25, 0.75],
    100: [2.50, 2.00, 1.00, 0.50],
}


def run_cmd(cmd, check=True):
    res = subprocess.run(cmd, shell=True, text=True, capture_output=True)
    if check and res.returncode != 0:
        err = res.stderr.strip() or res.stdout.strip()
        raise RuntimeError(f"Command failed ({res.returncode}): {cmd}\n{err}")
    return res


def apply_diversity(level_pct: int):
    if level_pct not in DIVERSITY_PROFILES:
        raise ValueError(f"Invalid level: {level_pct}%. Choose from: {list(DIVERSITY_PROFILES.keys())}")

    quotas = DIVERSITY_PROFILES[level_pct]
    total_cpu = sum(quotas)

    print(f"\n[SETTING HARDWARE DIVERSITY] Level = {level_pct}%")
    print(f"  Target Quotas: {quotas} (Total: {total_cpu:.2f} CPUs)")

    for container, q in zip(WORKER_CONTAINERS, quotas):
        run_cmd(f"docker update --cpus {q} {container}")
        print(f"  {container:16}: {q:.2f} CPUs")

    print(f"[SUCCESS] Hardware Diversity level {level_pct}% applied.\n")
    return quotas


def main():
    parser = argparse.ArgumentParser(description="Set Hardware Diversity Level for Phase 3")
    parser.add_argument("level", type=int, choices=[0, 25, 50, 75, 100], help="Hardware diversity percentage")
    args = parser.parse_args()
    apply_diversity(args.level)


if __name__ == "__main__":
    main()
