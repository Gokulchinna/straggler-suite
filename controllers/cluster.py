#!/usr/bin/env python3
"""Cluster Management Controller for Straggler Suite.

Usage:
    python3 controllers/cluster.py start      # Starts cluster and waits for 4 ALIVE workers
    python3 controllers/cluster.py status     # Reports cluster health, worker states, and cores
    python3 controllers/cluster.py stop       # Stops cluster containers gracefully
    python3 controllers/cluster.py set-cpus w1 w2 w3 w4  # Sets CPU quotas on running workers
"""
import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
COMPOSE_FILE = ROOT / "docker-compose.yml"
WORKERS = ["spark-worker-1", "spark-worker-2", "spark-worker-3", "spark-worker-4"]


def run_cmd(cmd, check=True, capture=True):
    res = subprocess.run(cmd, shell=True, text=True, capture_output=capture)
    if check and res.returncode != 0:
        raise RuntimeError(f"Command failed ({res.returncode}): {cmd}\n{res.stderr}")
    return res


def get_master_status():
    """Queries Spark Master JSON API via the driver container."""
    code = "import urllib.request; print(urllib.request.urlopen('http://master:8080/json/', timeout=5).read().decode())"
    res = run_cmd(f"docker exec spark-driver python3 -c \"{code}\"", check=False)
    if res.returncode == 0:
        try:
            return json.loads(res.stdout)
        except json.JSONDecodeError:
            pass
    return None


def start_cluster(timeout_s=120):
    print("Starting Spark cluster containers...")
    run_cmd(f"docker compose -f {COMPOSE_FILE} up -d", capture=False)

    print(f"\nWaiting for 4 workers to register as ALIVE with Spark Master (timeout: {timeout_s}s)...")
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        status = get_master_status()
        if status:
            alive_workers = [w for w in status.get("workers", []) if w.get("state") == "ALIVE"]
            if len(alive_workers) == 4:
                print("\n================ CLUSTER READY ================")
                print(f"Master URL: spark://master:7077")
                print(f"Web UI:     http://localhost:8080")
                for w in sorted(alive_workers, key=lambda x: x.get("id", "")):
                    print(f"  Worker: {w.get('id')} | Cores: {w.get('cores')} | Memory: {w.get('memory')}MB | State: {w.get('state')}")
                print("================================================\n")
                return 0
        time.sleep(3)

    print(f"ERROR: Cluster did not reach 4 ALIVE workers within {timeout_s}s.", file=sys.stderr)
    run_cmd("docker compose ps", check=False, capture=False)
    return 1


def cluster_status():
    print("=== Docker Container Status ===")
    run_cmd(f"docker compose -f {COMPOSE_FILE} ps", check=False, capture=False)
    print("\n=== Spark Master Status ===")
    status = get_master_status()
    if status:
        workers = status.get("workers", [])
        print(f"Registered Workers: {len(workers)}")
        for w in workers:
            print(f"  Worker: {w.get('id')} | Host: {w.get('host')} | Cores: {w.get('cores')} | State: {w.get('state')}")
    else:
        print("Spark Master UI API unreachable. Ensure containers are running.")


def stop_cluster():
    print("Stopping Spark cluster...")
    run_cmd(f"docker compose -f {COMPOSE_FILE} down")
    print("Cluster stopped.")


def set_cpus(w1, w2, w3, w4):
    quotas = [float(w1), float(w2), float(w3), float(w4)]
    total = sum(quotas)
    print(f"Updating worker CPU quotas: {quotas} (Total: {total:.2f} CPUs)")
    for container, q in zip(WORKERS, quotas):
        run_cmd(f"docker update --cpus {q} {container}")
        print(f"  {container} set to {q} CPUs")
    print("Quotas updated successfully.")


def main():
    parser = argparse.ArgumentParser(description="Cluster Management Tool")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("start", help="Start the cluster")
    subparsers.add_parser("status", help="Check cluster and worker status")
    subparsers.add_parser("stop", help="Stop the cluster")

    cpu_p = subparsers.add_parser("set-cpus", help="Set CPU quotas for the 4 workers")
    cpu_p.add_argument("w1", type=float, help="Worker 1 CPU")
    cpu_p.add_argument("w2", type=float, help="Worker 2 CPU")
    cpu_p.add_argument("w3", type=float, help="Worker 3 CPU")
    cpu_p.add_argument("w4", type=float, help="Worker 4 CPU")

    args = parser.parse_args()

    if args.command == "start":
        sys.exit(start_cluster())
    elif args.command == "status":
        cluster_status()
    elif args.command == "stop":
        stop_cluster()
    elif args.command == "set-cpus":
        set_cpus(args.w1, args.w2, args.w3, args.w4)


if __name__ == "__main__":
    main()
