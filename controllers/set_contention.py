#!/usr/bin/env python3
"""Single-Node Contention Controller for Straggler Suite (Phase 2).

Throttles Worker 4 across 5 contention levels (0%, 25%, 50%, 75%, 100%)
while distributing the remaining CPU power equally among Workers 1-3,
strictly holding total cluster CPU capacity constant at 6.0 CPUs.

Levels:
   0% Contention : [1.50, 1.50, 1.50, 1.50] CPUs (Pure Homogeneous)
  25% Contention : [1.63, 1.63, 1.63, 1.12] CPUs (Mild Contention)
  50% Contention : [1.75, 1.75, 1.75, 0.75] CPUs (Moderate Contention)
  75% Contention : [1.88, 1.88, 1.88, 0.38] CPUs (Severe Contention)
 100% Contention : [1.93, 1.93, 1.93, 0.20] CPUs (Extreme Straggler)
"""
import argparse
import subprocess
import sys

WORKER_CONTAINERS = ["spark-worker-1", "spark-worker-2", "spark-worker-3", "spark-worker-4"]

CONTENTION_PROFILES = {
    0:   [1.50, 1.50, 1.50, 1.50],
    25:  [1.63, 1.63, 1.63, 1.12],
    50:  [1.75, 1.75, 1.75, 0.75],
    75:  [1.88, 1.88, 1.88, 0.38],
    100: [1.93, 1.93, 1.93, 0.20],
}


def run_cmd(cmd, check=True):
    res = subprocess.run(cmd, shell=True, text=True, capture_output=True)
    if check and res.returncode != 0:
        err = res.stderr.strip() or res.stdout.strip()
        raise RuntimeError(f"Command failed ({res.returncode}): {cmd}\n{err}")
    return res


def apply_contention(level_pct: int):
    if level_pct not in CONTENTION_PROFILES:
        raise ValueError(f"Invalid level: {level_pct}%. Choose from: {list(CONTENTION_PROFILES.keys())}")

    quotas = CONTENTION_PROFILES[level_pct]
    total_cpu = sum(quotas)

    print(f"\n[SETTING CONTENTION] Level = {level_pct}% on Worker 4")
    print(f"  Target Quotas: {quotas} (Total: {total_cpu:.2f} CPUs)")

    for container, q in zip(WORKER_CONTAINERS, quotas):
        run_cmd(f"docker update --cpus {q} {container}")
        print(f"  {container:16}: {q:.2f} CPUs")

    print(f"[SUCCESS] Contention level {level_pct}% applied.\n")
    return quotas


def main():
    parser = argparse.ArgumentParser(description="Set Single-Node Contention Level for Phase 2")
    parser.add_argument("level", type=int, choices=[0, 25, 50, 75, 100], help="Contention percentage on Worker 4")
    args = parser.parse_args()
    apply_contention(args.level)


if __name__ == "__main__":
    main()
