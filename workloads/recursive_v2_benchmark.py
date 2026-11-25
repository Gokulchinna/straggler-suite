#!/usr/bin/env python3
"""PySpark Recursive Dynamic Sub-Division V2 Benchmark Workload (Option B).

Architecture:
- Exact 64 x 200,000-item initial partition queue (3.2 billion hash operations).
- Single persistent Spark stage (16 host-registered worker slots across 4 containers)
  communicating with an in-memory Driver ThreadingTCPServer Coordinator.
- Eliminates V1 disk checkpoints and 76+ Spark job launch overheads.
- Enforces Strict Node Quarantine when a host's live aggregate speed falls < 25% of
  cluster median, eliminating Slow-Node Ping-Pong across R1 -> R2 -> R3...
- Enforces ETA-Synchronized Proactive Early Preemption + unlimited dynamic recursion depth
  bounded strictly by the micro-task granularity floor (remaining >= 2 * min_chunk_size).
"""
import argparse
import json
import socket
import socketserver
import statistics
import threading
import time
from collections import deque


def parse_args():
    parser = argparse.ArgumentParser(description="Spark Recursive Sub-Division V2 (Option B) Benchmark")
    parser.add_argument("--partitions", type=int, default=64, help="Initial partitions (default: 64)")
    parser.add_argument("--items-per-partition", type=int, default=200000, help="Items per partition (default: 200000)")
    parser.add_argument("--iterations", type=int, default=250, help="Hash iterations per item (default: 250)")
    parser.add_argument("--min-chunk-size", type=int, default=5000, help="Minimum chunk size floor (default: 5000)")
    parser.add_argument("--max-slots", type=int, default=16, help="Persistent Spark slot tasks (default: 16)")
    parser.add_argument("--driver-host", type=str, default=None, help="Driver hostname reachable from workers")
    return parser.parse_args()


class CoordinatorState:
    def __init__(self, num_parts: int, items_per_part: int, min_chunk: int, max_slots: int):
        self.lock = threading.Lock()
        self.start_wall = time.time()
        self.min_chunk = min_chunk
        self.max_slots = max_slots
        self.expected_total_items = num_parts * items_per_part

        # Exact initial 64-partition push queue
        self.initial_queue = deque()
        for p in range(num_parts):
            s = p * items_per_part
            e = s + items_per_part
            self.initial_queue.append({
                "task_id": f"p{p}_d0",
                "partition_id": p,
                "start_idx": s,
                "end_idx": e,
                "depth": 0,
            })

        # High-priority recursive child slices (routed only to non-quarantined hosts)
        self.recursive_queue = deque()
        self.active_tasks = {}  # task_id -> meta dict

        # Cluster host & slot tracking
        self.slot_to_host = {}            # slot_id -> hostname
        self.host_slots = {}              # hostname -> set(slot_id)
        self.host_slot_rates = {}         # hostname -> {slot_id: rate}
        self.quarantined_hosts = set()
        self.known_hosts = set()

        # Global completion & verification metrics
        self.total_checksum = 0
        self.total_items_processed = 0
        self.completed_initial_tasks = 0
        self.all_done = False

        # Telemetry stats
        self.recursive_splits = 0
        self.subtasks_spawned = 0
        self.max_depth_reached = 0
        self.host_task_counts = {}
        self.host_task_durations = {}
        self.split_logs = []

    def _get_host_agg_rate(self, host: str) -> float:
        rates = self.host_slot_rates.get(host, {})
        valid = [r for r in rates.values() if r > 0]
        return sum(valid)

    def _update_quarantine_locked(self, now: float) -> None:
        # Wait until wave-1 heartbeats stabilize (>= 6.0s) before evaluating quarantine
        if now - self.start_wall < 6.0:
            return
        agg_rates = {}
        for h in self.known_hosts:
            r = self._get_host_agg_rate(h)
            if r > 0:
                agg_rates[h] = r

        if len(agg_rates) < 2:
            return

        med_rate = statistics.median(agg_rates.values())
        if med_rate <= 0:
            return

        for h, r in agg_rates.items():
            if h not in self.quarantined_hosts and (r / med_rate) < 0.25:
                self.quarantined_hosts.add(h)
                self.split_logs.append(
                    f"  [QUARANTINE] Host {h} flagged impaired "
                    f"(rate={r:.0f}/s vs median={med_rate:.0f}/s, health={r/med_rate:.2f} < 0.25)"
                )

    def _estimate_healthy_capacity_locked(self):
        healthy_hosts = [h for h in self.known_hosts if h not in self.quarantined_hosts]
        if not healthy_hosts:
            healthy_hosts = list(self.known_hosts)

        healthy_rate = sum(self._get_host_agg_rate(h) for h in healthy_hosts)
        if healthy_rate <= 0:
            healthy_rate = 120000.0  # fallback baseline before initial HB stabilizes

        healthy_slots_total = sum(len(self.host_slots.get(h, ())) for h in healthy_hosts)
        healthy_slots_busy = sum(
            1 for t in self.active_tasks.values() if t["host"] in healthy_hosts
        )
        healthy_idle_slots = max(0, healthy_slots_total - healthy_slots_busy)

        healthy_active_rem = sum(
            max(0, t["end_idx"] - t["cur_idx"])
            for t in self.active_tasks.values()
            if t["host"] in healthy_hosts
        )
        queued_rem = sum(m["end_idx"] - m["start_idx"] for m in self.initial_queue) + \
                     sum(m["end_idx"] - m["start_idx"] for m in self.recursive_queue)
        t_cluster_rem = (healthy_active_rem + queued_rem) / healthy_rate
        return healthy_hosts, healthy_rate, healthy_slots_total, healthy_idle_slots, t_cluster_rem

    def _evaluate_eta_preemptions_locked(self, now: float) -> None:
        if not self.active_tasks:
            return

        healthy_hosts, healthy_rate, healthy_slots_total, healthy_idle_slots, t_cluster_rem = \
            self._estimate_healthy_capacity_locked()

        in_tail_phase = (len(self.initial_queue) == 0 and len(self.recursive_queue) == 0)
        near_final_wave = (len(self.initial_queue) <= max(4, healthy_slots_total))

        for tid, t in self.active_tasks.items():
            if t["preempt_requested"]:
                continue

            rem = t["end_idx"] - t["cur_idx"]
            if rem < 2 * self.min_chunk:
                continue

            slot_rate = t["rate"]
            if slot_rate <= 0:
                continue

            task_eta = rem / slot_rate
            host = t["host"]

            # Case 1: Early Proactive Quarantine Evacuation
            # Harvest crippled node progress until final wave approaches, then evacuate seamlessly
            if host in self.quarantined_hosts and (near_final_wave or t_cluster_rem <= 14.0):
                if task_eta > t_cluster_rem:
                    t["preempt_requested"] = True
                    continue

            # Case 2: Tail Phase Adaptive Recursive Splitting (Dynamic Depth until 2 * min_chunk)
            if in_tail_phase and healthy_idle_slots >= 2:
                stolen_eta = rem / max(healthy_rate, slot_rate)
                if task_eta > max(1.5, stolen_eta + 0.4):
                    t["preempt_requested"] = True

    def handle_register(self, slot_id: int, host: str) -> dict:
        with self.lock:
            self.slot_to_host[slot_id] = host
            self.known_hosts.add(host)
            self.host_slots.setdefault(host, set()).add(slot_id)
            self.host_slot_rates.setdefault(host, {})[slot_id] = 0.0
            return {"status": "OK"}

    def handle_pull(self, slot_id: int, host: str) -> dict:
        with self.lock:
            now = time.time()
            self._update_quarantine_locked(now)
            self._evaluate_eta_preemptions_locked(now)

            if self.all_done:
                return {"action": "EXIT"}

            # Quarantined host NEVER pulls recursive sub-tasks or late initial partitions
            if host in self.quarantined_hosts:
                return {"action": "EXIT"}

            # 1. Prioritize recursive slices on healthy slots
            meta = None
            if self.recursive_queue:
                meta = self.recursive_queue.popleft()
            elif self.initial_queue:
                meta = self.initial_queue.popleft()

            if meta is not None:
                tid = meta["task_id"]
                self.active_tasks[tid] = {
                    "task_id": tid,
                    "partition_id": meta["partition_id"],
                    "start_idx": meta["start_idx"],
                    "end_idx": meta["end_idx"],
                    "cur_idx": meta["start_idx"],
                    "depth": meta["depth"],
                    "slot_id": slot_id,
                    "host": host,
                    "launch_time": now,
                    "last_hb_time": now,
                    "last_hb_idx": meta["start_idx"],
                    "rate": self.host_slot_rates.get(host, {}).get(slot_id, 0.0),
                    "preempt_requested": False,
                }
                return {
                    "action": "RUN",
                    "task_id": tid,
                    "start": meta["start_idx"],
                    "end": meta["end_idx"],
                    "depth": meta["depth"],
                }

            # Queues are empty: check completion or wait while active slices finish/split
            if not self.active_tasks and self.total_items_processed >= self.expected_total_items:
                self.all_done = True
                return {"action": "EXIT"}

            return {"action": "WAIT"}

    def handle_hb(self, slot_id: int, host: str, task_id: str, cur_idx: int) -> dict:
        with self.lock:
            now = time.time()
            t = self.active_tasks.get(task_id)
            if not t:
                return {"action": "CONTINUE"}

            dt = now - t["last_hb_time"]
            d_items = cur_idx - t["last_hb_idx"]
            if dt >= 0.12 and d_items > 0:
                inst_rate = d_items / dt
                t["rate"] = inst_rate if t["rate"] <= 0 else (0.6 * t["rate"] + 0.4 * inst_rate)
                self.host_slot_rates.setdefault(host, {})[slot_id] = t["rate"]
                t["last_hb_time"] = now
                t["last_hb_idx"] = cur_idx
            t["cur_idx"] = cur_idx

            self._update_quarantine_locked(now)
            self._evaluate_eta_preemptions_locked(now)

            if t["preempt_requested"]:
                return {"action": "PREEMPT"}
            return {"action": "CONTINUE"}

    def handle_done(
        self, slot_id: int, host: str, task_id: str, status: str,
        partial_sum: int, count: int, stopped_idx: int, dur: float
    ) -> dict:
        with self.lock:
            meta = self.active_tasks.pop(task_id, None)
            self.total_checksum += partial_sum
            self.total_items_processed += count
            self.host_task_counts[host] = self.host_task_counts.get(host, 0) + 1
            self.host_task_durations.setdefault(host, []).append(dur)

            if dur > 0.05 and count > 0:
                rate = count / dur
                old = self.host_slot_rates.get(host, {}).get(slot_id, 0.0)
                self.host_slot_rates.setdefault(host, {})[slot_id] = rate if old <= 0 else (0.5 * old + 0.5 * rate)

            if meta:
                if status == "COMPLETED" and meta["depth"] == 0:
                    self.completed_initial_tasks += 1

                if status == "PREEMPTED" and stopped_idx < meta["end_idx"]:
                    rem = meta["end_idx"] - stopped_idx
                    self.recursive_splits += 1

                    _, _, healthy_slots_total, healthy_idle_slots, _ = \
                        self._estimate_healthy_capacity_locked()
                    target_slots = max(2, min(max(healthy_idle_slots, 3), 4))
                    num_slices = min(target_slots, max(1, rem // self.min_chunk))

                    child_depth = meta["depth"] + 1
                    self.max_depth_reached = max(self.max_depth_reached, child_depth)

                    if num_slices <= 1:
                        self.subtasks_spawned += 1
                        self.recursive_queue.appendleft({
                            "task_id": f"{task_id}_r{child_depth}_0",
                            "partition_id": meta["partition_id"],
                            "start_idx": stopped_idx,
                            "end_idx": meta["end_idx"],
                            "depth": child_depth,
                        })
                    else:
                        chunk_size = rem // num_slices
                        cur = stopped_idx
                        for i in range(num_slices):
                            nxt = meta["end_idx"] if i == num_slices - 1 else cur + chunk_size
                            self.subtasks_spawned += 1
                            self.recursive_queue.appendleft({
                                "task_id": f"{task_id}_r{child_depth}_{i}",
                                "partition_id": meta["partition_id"],
                                "start_idx": cur,
                                "end_idx": nxt,
                                "depth": child_depth,
                            })
                            cur = nxt

                    self.split_logs.append(
                        f"  [RECURSIVE SPLIT V2 R{child_depth}] {task_id} on {host} preempted at "
                        f"{stopped_idx}/{meta['end_idx']} -> {num_slices} healthy slices (rem={rem})"
                    )

            if (
                not self.initial_queue
                and not self.recursive_queue
                and not self.active_tasks
                and self.total_items_processed >= self.expected_total_items
            ):
                self.all_done = True

            return {"status": "OK"}


class CoordinatorTCPServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, server_address, RequestHandlerClass, state: CoordinatorState):
        self.state = state
        super().__init__(server_address, RequestHandlerClass)


class CoordinatorHandler(socketserver.StreamRequestHandler):
    def handle(self):
        state: CoordinatorState = self.server.state
        while True:
            line = self.rfile.readline()
            if not line:
                break
            try:
                req = json.loads(line.decode("utf-8"))
            except json.JSONDecodeError:
                break

            cmd = req.get("cmd")
            if cmd == "REGISTER":
                resp = state.handle_register(req["slot"], req["host"])
            elif cmd == "PULL":
                resp = state.handle_pull(req["slot"], req["host"])
            elif cmd == "HB":
                resp = state.handle_hb(req["slot"], req["host"], req["task_id"], req["cur_idx"])
            elif cmd == "DONE":
                resp = state.handle_done(
                    req["slot"], req["host"], req["task_id"], req["status"],
                    req["sum"], req["count"], req["stopped_idx"], req["dur"]
                )
            else:
                resp = {"status": "ERR"}

            self.wfile.write((json.dumps(resp) + "\n").encode("utf-8"))
            self.wfile.flush()

            if resp.get("action") == "EXIT":
                break


def worker_slot_loop(slot_id: int, driver_host: str, driver_port: int, iters: int):
    hostname = socket.gethostname()
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    for attempt in range(5):
        try:
            sock.connect((driver_host, driver_port))
            break
        except OSError:
            if attempt == 4:
                raise
            time.sleep(0.2)

    rfile = sock.makefile("r", encoding="utf-8")

    def rpc(payload: dict) -> dict:
        sock.sendall((json.dumps(payload) + "\n").encode("utf-8"))
        resp_line = rfile.readline()
        if not resp_line:
            return {"action": "EXIT"}
        return json.loads(resp_line)

    try:
        rpc({"cmd": "REGISTER", "slot": slot_id, "host": hostname})
        tasks_executed = 0

        while True:
            pull_resp = rpc({"cmd": "PULL", "slot": slot_id, "host": hostname})
            action = pull_resp.get("action")
            if action == "EXIT":
                break
            if action == "WAIT":
                time.sleep(0.06)
                continue

            tid = pull_resp["task_id"]
            start_idx = pull_resp["start"]
            end_idx = pull_resp["end"]

            t_start = time.time()
            last_hb = t_start
            total = 0
            count = 0
            cur_idx = start_idx
            status = "COMPLETED"

            for number in range(start_idx, end_idx):
                val = number
                for _ in range(iters):
                    val = (val * 31 + 7) % 1000000007
                total += val
                count += 1
                cur_idx = number + 1

                if count % 250 == 0:
                    now = time.time()
                    if now - last_hb >= 0.25:
                        last_hb = now
                        hb_resp = rpc({
                            "cmd": "HB",
                            "slot": slot_id,
                            "host": hostname,
                            "task_id": tid,
                            "cur_idx": cur_idx,
                        })
                        if hb_resp.get("action") == "PREEMPT":
                            status = "PREEMPTED"
                            break

            dur = time.time() - t_start
            rpc({
                "cmd": "DONE",
                "slot": slot_id,
                "host": hostname,
                "task_id": tid,
                "status": status,
                "sum": total,
                "count": count,
                "stopped_idx": cur_idx,
                "dur": dur,
            })
            tasks_executed += 1

        yield (slot_id, hostname, tasks_executed)
    finally:
        try:
            rfile.close()
        except OSError:
            pass
        try:
            sock.close()
        except OSError:
            pass


def main():
    from pyspark.sql import SparkSession
    args = parse_args()
    spark = SparkSession.builder.appName("Straggler-Suite-Recursive-V2").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    num_parts = args.partitions
    items_per_part = args.items_per_partition
    iters = args.iterations
    min_chunk = args.min_chunk_size
    max_slots = args.max_slots
    driver_host = args.driver_host or socket.gethostname()

    state = CoordinatorState(num_parts, items_per_part, min_chunk, max_slots)
    server = CoordinatorTCPServer(("0.0.0.0", 0), CoordinatorHandler, state)
    driver_port = server.server_address[1]

    srv_thread = threading.Thread(target=server.serve_forever, daemon=True)
    srv_thread.start()

    print("\n==================================================")
    print("STARTING RECURSIVE SUB-DIVISION V2 (OPTION B) BENCHMARK")
    print(f"Partitions: {num_parts} | Items/Partition: {items_per_part} | Iterations: {iters}")
    print(f"Strategy: Persistent Slots + Zero-Disk TCP ({driver_host}:{driver_port}) + ETA Cutoff + Quarantine")
    print("==================================================")

    start_wall = time.time()
    state.start_wall = start_wall

    # Launch 16 persistent worker slots across the single Spark stage
    slot_results = sc.parallelize(range(max_slots), max_slots) \
                     .mapPartitionsWithIndex(
                         lambda idx, _: worker_slot_loop(idx, driver_host, driver_port, iters)
                     ).collect()

    jct = time.time() - start_wall
    server.shutdown()
    server.server_close()

    for line in state.split_logs:
        print(line)

    print("\n================ BENCHMARK RESULTS ================")
    print(f"Job Completion Time (JCT): {jct:.2f}s")
    print(f"Total Items Verified: {state.total_items_processed} (Expected: {state.expected_total_items})")
    print(
        f"Recursive Splits Triggered: {state.recursive_splits} | "
        f"Sub-Tasks Spawned: {state.subtasks_spawned} | Max Depth: {state.max_depth_reached} | "
        f"Quarantined Hosts: {sorted(state.quarantined_hosts)}"
    )

    print("\n--- Summary by Worker Node ---")
    for host in sorted(state.host_task_counts.keys()):
        durs = state.host_task_durations[host]
        avg_dur = sum(durs) / len(durs)
        print(
            f"  Worker {host:15}: {len(durs):2} tasks | Avg: {avg_dur:.2f}s | "
            f"Min: {min(durs):.2f}s | Max: {max(durs):.2f}s"
        )
    print("==================================================\n")

    spark.stop()


if __name__ == "__main__":
    main()
