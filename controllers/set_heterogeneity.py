#!/usr/bin/env python3
"""Heterogeneity Configuration Controller for Straggler Suite (Phase 2).

Manages cluster heterogeneity levels (H0 vs H3) and slot modes (uniform vs proportional)
while strictly conserving total cluster CPU capacity at 6.0 CPUs.

Levels:
  H0: [1.5, 1.5, 1.5, 1.5] CPUs (Homogeneous, CV=0.00)
  H3: [2.5, 2.0, 1.0, 0.5] CPUs (Severe Heterogeneity, CV=0.53)

Slot Modes:
  uniform:      [4, 4, 4, 4] cores (Equal slots)
  proportional: [7, 5, 3, 1] cores (HATS-like capacity-aware slots for H3)
"""
import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
COMPOSE_FILE = ROOT / "docker-compose.yml"
WORKER_CONTAINERS = ["spark-worker-1", "spark-worker-2", "spark-worker-3", "spark-worker-4"]
WORKER_SERVICES = ["worker1", "worker2", "worker3", "worker4"]

LEVEL_CPUS = {
    "H0": [1.5, 1.5, 1.5, 1.5],
    "H3": [2.5, 2.0, 1.0, 0.5],
}

SLOTS_CONFIG = {
    "H0": {"uniform": [4, 4, 4, 4], "proportional": [4, 4, 4, 4]},
    "H3": {"uniform": [4, 4, 4, 4], "proportional": [7, 5, 3, 1]},
}


def run_cmd(cmd, check=True, capture=True, env=None):
    res = subprocess.run(cmd, shell=True, text=True, capture_output=capture, env=env)
    if check and res.returncode != 0:
        err = res.stderr.strip() or res.stdout.strip()
        raise RuntimeError(f"Command failed ({res.returncode}): {cmd}\n{err}")
    return res


def get_master_status():
    code = "import urllib.request; print(urllib.request.urlopen('http://master:8080/json/', timeout=5).read().decode())"
    res = run_cmd(f"docker exec spark-driver python3 -c \"{code}\"", check=False)
    if res.returncode == 0:
        try:
            return json.loads(res.stdout)
        except json.JSONDecodeError:
            pass
    return None


def get_current_cores():
    status = get_master_status()
    if not status:
        return []
    workers = status.get("workers", [])
    alive = [w for w in workers if w.get("state") == "ALIVE"]
    # Sort by hostPort or worker id
    return sorted(int(w.get("cores", 0)) for w in alive)


def wait_for_cores(target_slots, timeout_s=90):
    deadline = time.time() + timeout_s
    sorted_target = sorted(target_slots)
    while time.time() < deadline:
        curr = get_current_cores()
        if curr == sorted_target:
            return True
        time.sleep(2)
    return False


def apply_heterogeneity(level: str, slot_mode: str = "uniform", force: bool = False):
    if level not in LEVEL_CPUS:
        raise ValueError(f"Unknown level: {level}. Choose from: {list(LEVEL_CPUS.keys())}")
    if slot_mode not in ["uniform", "proportional"]:
        raise ValueError(f"Unknown slot_mode: {slot_mode}. Choose 'uniform' or 'proportional'")

    target_cpus = LEVEL_CPUS[level]
    target_slots = SLOTS_CONFIG[level][slot_mode]

    print(f"\n[CONFIGURING CLUSTER] Level={level} | Slot Mode={slot_mode}")
    print(f"  Target CPUs : {target_cpus} (Sum: {sum(target_cpus):.1f} CPUs)")
    print(f"  Target Slots: {target_slots} (Sum: {sum(target_slots)} cores)")

    current_slots = get_current_cores()
    needs_core_recreate = force or (sorted(current_slots) != sorted(target_slots))

    if needs_core_recreate:
        print("  Recreating worker containers to apply new slot allocations...")
        env = os.environ.copy()
        for i, s in enumerate(target_slots):
            env[f"W{i + 1}_CORES"] = str(s)

        services_str = " ".join(WORKER_SERVICES)
        run_cmd(f"docker compose -f {COMPOSE_FILE} up -d --no-deps {services_str}", env=env)

        if not wait_for_cores(target_slots):
            print(f"  Warning: Spark Master did not show cores {target_slots} within timeout. Current: {get_current_cores()}")
    else:
        print("  Worker slot counts already match target. Re-creation not needed.")

    # Apply CPU limits
    print("  Applying CPU quotas via Docker...")
    for container, cpu in zip(WORKER_CONTAINERS, target_cpus):
        run_cmd(f"docker update --cpus {cpu} {container}")

    print(f"[SUCCESS] Cluster configured for {level} ({slot_mode})\n")
    return target_cpus, target_slots


def main():
    parser = argparse.ArgumentParser(description="Configure cluster heterogeneity for Phase 2")
    parser.add_argument("level", choices=["H0", "H3"], help="Heterogeneity level (H0=equal, H3=severe)")
    parser.add_argument("--slots", choices=["uniform", "proportional"], default="uniform", help="Slot allocation mode")
    parser.add_argument("--force", action="store_true", help="Force container recreation")

    args = parser.parse_args()
    apply_heterogeneity(args.level, args.slots, force=args.force)


if __name__ == "__main__":
    main()
