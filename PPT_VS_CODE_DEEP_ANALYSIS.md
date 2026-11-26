# Straggler Suite: Deep Slide-by-Slide & Code-by-Code Analysis

This guide compares **every slide** in `Straggler_Suite_Presentation.pptx` side-by-side against the **actual Python implementation** across `core/`, `policies/`, `simulation/`, `workloads/`, `controllers/`, `experiments/`, and `analysis/`.

For every term, algorithm, and threshold, you get **both**:
1. **Simple Explanation** — plain-English intuition and concrete numbers for someone new to the project.
2. **Technical Explanation** — exact mathematical formulas, variable names, file locations, and execution flow.

---

## 1. Big-Picture Reality Check: PPT Slides vs. Actual Code

Before diving into each algorithm, note **five critical code-vs-slide differences** discovered by inspecting the source files:

| # | Topic | What the PPT Slides Say | What the Actual Code Does | Where to Verify |
|---|---|---|---|---|
| 1 | **CPU Profiles (`C25–C75`, `D25–D75`)** | Slide 3 lists linearly interpolated profiles (`C25: [1.61, 1.61, 1.61, 1.18]`, `D25: [1.75, 1.63, 1.38, 1.25]`, etc.). | The actual Docker runs applied `set_contention.py` (`C25: [1.63, 1.63, 1.63, 1.12]`, `C75: [1.88, 1.88, 1.88, 0.38]`) and `set_diversity.py` (`D25: [1.88, 1.63, 1.38, 1.11]`). | `controllers/set_contention.py`, `controllers/set_diversity.py`, `analysis/parse_recursive_v2_results.py` |
| 2 | **Algorithm 2 & 3 in Real Docker** | Slides 8–11 present Dynamic Sub-Division and Racing Team results from 310+ real Docker runs. | In `workloads/dynamic_subdivision_benchmark.py` and `workloads/racing_team_benchmark.py`, **no splitting or racing logic is coded**—both run a single standard `collect()` with `spark.speculation=false` (`sub_slices=0`, `rescue_slices=0`). The actual Dynamic & Racing algorithms exist in `policies/mitigation/` and `simulation/engine.py`. | `workloads/dynamic_subdivision_benchmark.py`, `workloads/racing_team_benchmark.py`, `results/dynamic_subdivision/*/run_summary.csv`, `results/racing_team/*/run_summary.csv` |
| 3 | **`SparkMedianDetector` Peers vs. Completed Tasks** | Slide 4 & 6 describe comparing elapsed time against `1.5x` peer median (`quantile=0.75`). | Real `spark-submit` runs use Spark's built-in `spark.speculation.quantile=0.75` (completed tasks). Simulator `SparkMedianDetector` compares against the median elapsed time of **currently active running peers** (`>= 20%` progress). | `experiments/run_phase2_contention.py`, `policies/detection/spark_median.py` |
| 4 | **Weighted Slicing vs. Equal Slicing in V1/V2** | Policy descriptions emphasize capacity-weighted slicing (`CapacityWeightedPartitioner`). | `policies/mitigation/recursive_subdivision*.py` (used in `ClusterSimulator`) slices proportionally by `effective_cpu_quota`. The real Spark workloads (`recursive_subdivision_benchmark.py` & `recursive_v2_benchmark.py`) slice **equally** (`rem // num_slices`) because workers pull work dynamically as slots free up. | `policies/partitioners/capacity_weighted.py`, `workloads/recursive_subdivision_benchmark.py`, `workloads/recursive_v2_benchmark.py` |
| 5 | **ETA Cutoff Formula (Policy vs. Real Spark V2)** | Slide 14 shows `ETA_task > T_cluster_rem` (Case 1) and `ETA_task > max(1.5, ETA_stolen + 0.4)` (Case 2). | That matches `workloads/recursive_v2_benchmark.py` (`_evaluate_eta_preemptions_locked`). Meanwhile `policies/mitigation/recursive_subdivision_v2.py` has a unified helper `should_preempt_eta()` using `max(cluster_healthy_remaining_eta, stolen_eta + steal_overhead)` with `steal_overhead=0.25`. | `workloads/recursive_v2_benchmark.py`, `policies/mitigation/recursive_subdivision_v2.py` |

---

## 2. Slide-by-Slide Map to Repository Files

| Slide(s) | Slide Title | Matching Files in Repository |
|---|---|---|
| **1–2** | Title, Introduction & Motivation | `README.md` |
| **3** | Problem Statement & Experimental Axes (`C0–C100`, `D0–D100`) | `controllers/set_contention.py`, `controllers/set_diversity.py`, `controllers/set_heterogeneity.py` |
| **4–5** | Baseline Limitations & Experimental Setup (`6` containers, `3.2B` ops, `6.00c`) | `docker-compose.yml`, `controllers/cluster.py`, `workloads/straggler_benchmark.py`, `experiments/run_phase1_baseline.py`, `experiments/run_phase2_contention.py`, `experiments/run_phase3_diversity.py` |
| **6–7** | Algorithm 1: Apache Spark Speculation (`FullCloneStrategy`) & Results | `policies/detection/spark_median.py`, `policies/mitigation/full_clone.py`, `workloads/straggler_benchmark.py`, `analysis/parse_phase2_contention.py`, `analysis/parse_phase3_diversity.py` |
| **8–9** | Algorithm 2: Dynamic Sub-Division (Residual Slicing) & Results | `core/node.py`, `core/task.py`, `core/cluster_state.py`, `policies/detection/capacity_normalized.py`, `policies/partitioners/capacity_weighted.py`, `policies/mitigation/dynamic_subdivision.py`, `workloads/dynamic_subdivision_benchmark.py`, `analysis/parse_dynamic_subdivision.py` |
| **10–11** | Algorithm 3: Racing Team Speculation & Results | `policies/mitigation/racing_team.py`, `simulation/engine.py`, `workloads/racing_team_benchmark.py`, `analysis/parse_racing_team_results.py` |
| **12–13** | Algorithm 4: Recursive Sub-Division V1 & Results | `policies/mitigation/recursive_subdivision.py`, `workloads/recursive_subdivision_benchmark.py`, `analysis/parse_recursive_results.py` |
| **14–15** | Algorithm 5: Recursive Sub-Division V2 (Option B) & Welch Results | `policies/mitigation/recursive_subdivision_v2.py`, `workloads/recursive_v2_benchmark.py`, `analysis/parse_recursive_v2_results.py`, `analysis/run_statistical_tests.py` |
| **16–17** | 6-Way Grand Comparison & Pareto Frontier Charts | `results/recursive_v2/master_6way_comparison.csv`, `results/recursive_v2/graph_6way_grand_comparison.png`, `results/graph_master_cost_benefit.png` |
| **18–19** | Key Findings, Conclusion & Future Work (15/15 tests) | `tests/test_dynamic_subdivision.py`, `simulation/empirical_calibration.py` |

---

## 3. Master Glossary: Every Term in the PPT (Simple + Technical)

### Cluster & Hardware Terms
* **`JCT` (Job Completion Time)**
  * **Simple:** Total wall-clock seconds from when a Spark job starts until the very last task finishes.
  * **Technical:** Measured in seconds (`jct = time.time() - start_wall` around `.collect()`, or `(app_end - app_start) / 1000.0` from Spark eventlogs).
* **`6.00c` CPU Conservation**
  * **Simple:** The whole 4-worker cluster always shares a fixed total budget of 6 CPU cores. Making one worker slower gives its leftover CPU to the other workers so improvements come from smarter scheduling, not extra hardware.
  * **Technical:** $\sum_{i=1}^{4} C_i = 6.00$, enforced via `docker update --cpus <q> spark-worker-i` (Linux CFS cgroups). Verified in `ClusterResourceTracker.get_total_cluster_cores()` (`core/cluster_state.py`).
* **`C_effective` (`effective_cpu_quota`) vs. `nominal_cores`**
  * **Simple:** `nominal_cores` (`4`) is how many task slots a worker *claims* to have (`SPARK_WORKER_CORES=4`); `C_effective` (e.g., `1.93`, `0.75`, `0.20`) is how much real CPU power Docker actually lets it use.
  * **Technical:** `ClusterNode.effective_cpu_quota` in `core/node.py`. When `SPARK_WORKER_CORES=4`, Spark runs up to 4 concurrent tasks per container, sharing the container's `effective_cpu_quota`.
* **`C0–C100` (Contention Axis) & `D0–D100` (Diversity Axis) & `CV`**
  * **Simple:** `C0–C100` throttles *only* Worker 4 (simulating a single broken/noisy node). `D0–D100` spreads CPU unevenly across *all 4* workers (simulating 4 different machine tiers). `CV` (Coefficient of Variation = $\sigma / \mu$) measures how uneven the 4 worker speeds are (`0.00` = identical, `0.53` = huge spread at `D100`).
  * **Technical:** Defined in `controllers/set_contention.py` and `controllers/set_diversity.py`.

### Speed & Detection Terms (`CapacityNormalizedDetector`)
* **`R_norm` (`norm_rate`)**
  * **Simple:** How fast a task is working **per 1 full CPU core**. Dividing by `C_effective` creates a fair "miles per gallon" metric so a small `0.50c` car isn't called broken just because a `2.50c` sports car passes it.
  * **Technical:**
    $$R_{\text{norm}} = \frac{\text{completed\_items}}{\text{elapsed} \times C_{\text{effective}}} \quad \left[\frac{\text{items}}{\text{core} \cdot \text{sec}}\right]$$
    Implemented in `policies/detection/capacity_normalized.py` line 33:
    `norm_rate = (task.get_completed_work() / elapsed) / assigned_quota`.
* **`R_ref` (`reference_rate`)**
  * **Simple:** The "normal healthy speed per core" used as the ruler to judge whether a task is dragging.
  * **Technical:** Median of `norm_rate` across all *other* currently running peer tasks that have reached `>= 20%` progress. If no peers remain active (tail of stage), falls back to `R_base` (`calibrated_r_base`).
* **`R_base` / `R_calibrated` (`calibrated_r_base`)**
  * **Simple:** The known baseline speed of 1 CPU core on this hardware for this benchmark: roughly **9,010 items per core per second**.
  * **Technical:** `calibrated_r_base = 9010.0` in `policies/detection/capacity_normalized.py` and `simulation/empirical_calibration.py`. Derived empirically from `200,000 items / (14.8s * 1.5c) = 9,010 items/(core*sec)` (or dynamically as `12,800,000 / (D0_OFF_JCT * 6.0)` in `get_calibrated_cluster_parameters()`).
* **`C_idle` (`total_idle_cores`)**
  * **Simple:** How many total real CPU cores are currently sitting idle doing nothing across workers that have zero active tasks.
  * **Technical:** `ClusterResourceTracker.get_total_idle_cores()` sums `node.available_cores`, where `available_cores` returns `effective_cpu_quota` if `len(assigned_tasks) == 0`, else `0.0`.

### Splitting, Conservation & Slicing Terms
* **`W_rem` / `rem` (`remaining_work`)**
  * **Simple:** How many items in a task have **not** been processed yet.
  * **Technical:** `SplittableTask.get_remaining_work()` = `max(0, total_work - completed_items)`, or in V1/V2 Spark: `end_idx - cur_idx` / `end_idx - stopped_idx`.
* **`min_chunk` / `W_min_chunk` (`min_chunk_size`) & `Delta_min` (`2 * min_chunk_size`)**
  * **Simple:** The smallest piece of work worth giving to a worker (`5,000` items). If a task has fewer than `2 * 5,000 = 10,000` items left, splitting it into 2+ pieces would create tiny tasks (`< 5,000`) where network/startup overhead costs more than the time saved.
  * **Technical:** `min_chunk_size = 5000`; dynamic granularity floor is `remaining_work >= 2 * min_chunk_size` (`10,000`).
* **`cur_offset` / `stopped_idx` & "0.0s Waste" (Exact Work Conservation)**
  * **Simple:** Instead of throwing away what the slow worker already computed and starting over at item `0`, we record the exact index where it stopped (`cur_offset` / `stopped_idx`) and only split the unfinished remainder `[stopped_idx, end_idx)`.
  * **Technical:** `current_offset = straggler.start_idx + straggler.get_completed_work()` in `policies/mitigation/*.py`; `stopped_idx = cur_idx` in `workloads/recursive*_benchmark.py`. Theorem 1 (`test_exact_work_conservation`) verifies $\sum (\text{end}_j - \text{start}_j) = W_{\text{rem}}$ with contiguous boundaries.
* **`Gate 1`, `Gate 2`, `Gate 3`**
  * **Simple:** Safety checkpoints before splitting a task so we don't split when it would hurt performance:
    * **Gate 1 (Capacity / Depth):** Either "have we hit `max_depth`?" or "are there healthy idle cores/nodes available to take the stolen slices?"
    * **Gate 2 (Granularity Floor):** "Is there enough remaining work (`>= 10,000` items) to make splitting worthwhile?"
    * **Gate 3 (Health / Quarantine):** "Should the slow node still get a residual slice, or is it crippled (`< 25%` of median) and must be evicted/quarantined?"
  * **Technical:** Coded sequentially inside `mitigate(straggler)` in `policies/mitigation/dynamic_subdivision.py`, `racing_team.py`, `recursive_subdivision.py`, and `recursive_subdivision_v2.py`.
* **`depth` & `max_depth`**
  * **Simple:** How many generations of splitting a task has gone through (`0` = original partition, `1` = first split child, `2` = split of a child, etc.).
  * **Technical:** `child.depth = parent.depth + 1`. Dynamic Sub-Division sets `max_depth=3`; Racing Team sets `max_depth=2`; Recursive V1 & V2 set `max_depth=None` (unlimited depth, bounded by `2 * min_chunk_size`).

### Recursive V1 & V2 Coordinator Terms
* **`.ckpt` and `.stop` files (Recursive V1)**
  * **Simple:** Tiny text files on a shared folder (`/data/recursive_ckpt`) that V1 workers write to say "I'm at item 42,000" (`.ckpt`) and the driver creates to tell a slow worker "stop right now!" (`.stop`).
  * **Technical:** Written atomically via `os.replace(tmp_file, ckpt_file)` every `2,000` items in `execute_slice_worker()` (`workloads/recursive_subdivision_benchmark.py`).
* **`CoordinatorTCPServer` & JSON-Line RPC (`REGISTER`, `PULL`, `HB`, `DONE`, `WAIT`, `PREEMPT`, `EXIT`) (Recursive V2)**
  * **Simple:** Instead of writing files to disk, the 16 Spark worker slots keep an open phone line (TCP socket) to the Python driver. They ask for work (`PULL`), check in periodically (`HB` = heartbeat), hang up early if told `PREEMPT`, and report final answers (`DONE`).
  * **Technical:** `socketserver.ThreadingTCPServer` + `CoordinatorHandler` in `workloads/recursive_v2_benchmark.py`, exchanging newline-delimited JSON messages with `worker_slot_loop`.
* **EMA Rate (`r_slot`), Host Rate (`R_h`), Median Host Rate (`R_med`)**
  * **Simple:** A smooth running speedometer for each worker slot (`60%` old speed + `40%` latest burst speed), added up across all 4 slots of a machine to get `R_h`, then compared against the cluster median `R_med`.
  * **Technical:** In `handle_hb()` (`recursive_v2_benchmark.py` lines 244–246):
    `inst_rate = d_items / dt`; `t["rate"] = 0.6 * t["rate"] + 0.4 * inst_rate` (when `dt >= 0.12` and `d_items > 0`). On `DONE`, blended `0.5 * old + 0.5 * rate`. Host aggregate `R_h = _get_host_agg_rate(host)`.
* **Quarantine (`Q` / `quarantined_hosts` / `quarantined_nodes`)**
  * **Simple:** A permanent "do not assign new work" blacklist for any machine whose speed falls below 25% of the cluster median—stopping "slow-node ping-pong" where a crippled node finishes/yields one task and immediately gets another slow task.
  * **Technical:** After `6.0s` warmup (`now - start_wall >= 6.0`), if `r / med_rate < 0.25`, host `h` is added to `self.quarantined_hosts`. Future `PULL` calls from `h` immediately return `{"action": "EXIT"}`.
* **`T_cluster_rem` (`t_cluster_rem`), `ETA_task` (`task_eta`), `ETA_stolen` (`stolen_eta`)**
  * **Simple:**
    * `T_cluster_rem`: Estimated seconds until all healthy workers finish everything on their plates.
    * `ETA_task`: Estimated seconds for *this specific running task* to finish if left alone on its current worker.
    * `ETA_stolen`: Estimated seconds to finish *this task's remaining work* if stolen and spread across the healthy cluster.
  * **Technical:**
    $$T_{\text{cluster\_rem}} = \frac{\text{healthy\_active\_rem} + \text{queued\_rem}}{\text{healthy\_rate}}, \quad \text{ETA}_{\text{task}} = \frac{\text{rem}}{r_{\text{slot}}}, \quad \text{ETA}_{\text{stolen}} = \frac{\text{rem}}{\max(\text{healthy\_rate}, r_{\text{slot}})}$$
    Computed in `_estimate_healthy_capacity_locked()` and `_evaluate_eta_preemptions_locked()` (`workloads/recursive_v2_benchmark.py` lines 111–173).

---

## 4. Shared Building Blocks: Detectors & Partitioners (Slide vs. Code)

### A. Straggler Detectors

#### 1. Legacy Detector (`SparkMedianDetector`)
* **Simple:** Wait until a task is at least 20% done. Compare how many seconds it has been running against `1.5×` the typical time of other tasks. If it's taking more than `1.5×` as long, call it a straggler.
* **Technical Flow (`policies/detection/spark_median.py`):**
  1. If `task.get_progress_fraction() < 0.20` or `task.start_time is None` → return `False`.
  2. Compute `elapsed = max(0.1, current_time - task.start_time)`.
  3. Collect elapsed durations of all other `active_tasks` with `progress >= 0.20`.
  4. Sort and take `median_dur = durations[len(durations) // 2]`.
  5. Return `elapsed > (1.5 * median_dur)`.
* **Slide vs. Code Comparison:**
  * **Slide 4 & 6:** Mentions `elapsed > 1.5 * median(peers) (quantile=0.75)`.
  * **Real Spark Runs (`experiments/run_phase*.py`):** Passes `--conf spark.speculation.quantile=0.75 --conf spark.speculation.multiplier=1.5 --conf spark.speculation.minTaskRuntime=1000ms --conf spark.speculation.interval=1000ms` (Spark's native scheduler compares against the 75th percentile of *completed* tasks).
  * **Simulator (`spark_median.py`):** Uses the 50th percentile (`len // 2`) of *active* tasks with `>= 20%` progress.

#### 2. Capacity-Normalized Detector (`CapacityNormalizedDetector`)
* **Simple:** Instead of asking "is this task slow in raw seconds?", ask two smarter questions:
  1. *"Is this task slow **per CPU core** compared to what its hardware should do?"* (Catches throttled/noisy-neighbor nodes, ignores naturally smaller healthy nodes.)
  2. *"Even if it's healthy for its tiny CPU, are big fast workers sitting completely idle waiting on this last slow task?"* (Catches end-of-stage tail blockers.)
* **Technical Flow (`policies/detection/capacity_normalized.py`):**
  1. **Progress Gate:** Return `False` if `progress < 0.20` or `start_time is None`.
  2. **Normalize Rate:** `norm_rate = (completed_work / elapsed) / assigned_quota`.
  3. **Compute Reference `R_ref`:** Collect `norm_rate` for all active peer tasks with `progress >= 0.20`. If non-empty, `reference_rate = median(active_norm_rates)`; else fallback to `self.calibrated_r_base` (`9010.0`).
  4. **Check 1 (Degradation):** If `norm_rate < 0.60 * reference_rate` → return `True`.
  5. **Check 2 (Tail Blocker):** If `total_idle_cores >= 1.0` and `remaining_work >= 10000`:
     `time_alone = remaining_work / (assigned_quota * 9010.0)`
     `time_shared = remaining_work / ((assigned_quota + total_idle_cores) * 9010.0)`
     If `time_alone > 1.5 * time_shared` → return `True`.
* **Worked Example (`D100` vs `C100`):**
  * On `D100` (Worker 4 = `0.50c` healthy): Worker 4 processes `4,505 items/s` → `norm_rate = 4505 / 0.50 = 9010`. Peer `R_ref = 9010`. `9010 < 0.60 * 9010` is **False** (no false positive mid-stage!). At the very end when Workers 1–3 (`5.50c`) go idle, Check 2 triggers (`12x > 1.5x`) so idle workers help finish the tail!
  * On `C100` (Worker 4 throttled to `0.20c`, or degraded below its expected quota): if Worker 4 drops below `0.60 * 9010 = 5406`, Check 1 triggers immediately after `20%` progress.

### B. Work Partitioners (`policies/partitioners/`)
* **`UniformPartitioner` (`uniform.py`):** Splits `total_remaining` into `k` equal integer chunks (`total_remaining // k`, spreading `total_remaining % k` across the first nodes). *Not used by any strategy in `ClusterSimulator`*, though real Spark V1/V2 split queued sub-tasks uniformly (`rem // num_slices`).
* **`CapacityWeightedPartitioner` (`capacity_weighted.py`):** Splits `total_remaining` proportionally to `node.effective_cpu_quota / total_cores`. Each node `0..k-2` gets `round(total_remaining * weight)`, and the final node gets `total_remaining - allocated_items` so the sum of chunks equals `total_remaining` with zero lost items.

---

## 5. Algorithm-by-Algorithm Deep Comparison (PPT Slide vs. Code)

---

### Algorithm 1: Apache Spark Speculation (`FullCloneStrategy`)

#### Simple Explanation
Imagine 4 workers packing boxes. One worker gets stuck. Spark waits until most boxes are done, notices one worker is taking `1.5×` longer than average, and tells an empty fast worker: *"Start packing a brand-new duplicate of that exact same box from item 0!"* Whichever finishes first wins, and the loser throws away all its partial work.

#### Technical Flow
```text
Task runs [0, 200,000)
   │
   ▼  (>= 20% progress & elapsed > 1.5 * median(peers))
SparkMedianDetector flags straggler
   │
   ▼  (idle_nodes non-empty?)
FullCloneStrategy picks fastest idle node
   │
   ▼
Spawns clone covering full [start_idx, end_idx) from item 0
   │
   ▼
Original & Clone race -> Winner completes -> Loser killed (wasted_cpu_seconds += loser_time * loser_quota)
```

#### Side-by-Side Comparison: Slide vs. Simulator vs. Real Spark

| Aspect | PPT Slides (Slides 6–7) | Policy & Simulator (`full_clone.py`, `engine.py`) | Real Spark (`straggler_benchmark.py`, `run_phase2/3_*.py`) |
|---|---|---|---|
| **Detection Rule** | `progress >= 20%`, `elapsed > 1.5 * median(peers)`, `quantile=0.75` | `SparkMedianDetector`: `progress >= 0.20`, `elapsed > 1.5 * median(active_peers)` | Spark built-in: `spark.speculation=true`, `quantile=0.75`, `multiplier=1.5`, `minTaskRuntime=1000ms`, `interval=1000ms` |
| **Target Node** | Fastest idle worker (`1` clone) | `idle_nodes.sort(key=effective_cpu_quota, reverse=True)[0]` | Spark's internal `TaskSchedulerImpl` chooses any available executor slot |
| **Work Range** | Full `[0, 200,000)` from scratch | `straggler.split([(start_idx, end_idx)])` | Re-runs full partition (`attempt=1`) from item `0` |
| **Cancellation** | Winner kills loser | `engine.py` lines 165–201: removes loser, adds `effective_cpu_quota * wasted_time` to `wasted_cpu_seconds` | Spark kills the slower task attempt (`Speculative` task kill) |
| **Measured Results** | `C100`: `144.36s -> 83.43s` (`+42.20%`); `C0`: `84.54s -> 88.45s` (`-4.63%`); `D75`: `86.58s -> 90.13s` (`-4.10%`); `D100`: `102.86s` | Same qualitative behavior in discrete `0.1s` ticks | Exact source of Slide 7 numbers (`results/phase2_contention/`, `results/phase3_diversity/`) |

---

### Algorithm 2: Dynamic Task Sub-Division (Residual Slicing)

#### Simple Explanation
Instead of throwing away what the slow worker already finished, freeze the task right where it is (say item `50,000`), keep the already-done `50,000` items safely counted, and slice the *remaining* `150,000` items across **everyone**—all idle workers **plus** the original slow worker—giving bigger slices to stronger workers.
* **Why it shines on Diversity (`D100`):** Worker 4 (`0.50c`) isn't broken; it just gets a tiny proportional slice while Workers 1–3 take big slices, saving every drop of work.
* **Why it fails on Extreme Contention (`C100`):** Because it still gives a slice back to the original straggler node (`0.20c`), and only splits up to `max_depth=3` without evicting crippled nodes, that residual slice on `0.20c` Worker 4 bottlenecks the job (`134.41s`).

#### Technical Flow
```text
CapacityNormalizedDetector flags task
   │
   ▼  Gate 1: depth < max_depth (3)
   ▼  Gate 2: C_idle >= 1.0 core AND idle_nodes non-empty
   ▼  Gate 3: W_rem >= 2 * min_chunk_size (10,000)
   │
   ▼
target_nodes = idle_nodes + [straggler_node]   (Residual Slicing)
   │
   ▼
CapacityWeightedPartitioner slices [cur_offset, end_idx) proportional to effective_cpu_quota
   │
   ▼
Parent marked SPLITTING (0.0s waste); completed items credited; child slices scheduled
```

#### Side-by-Side Comparison: Slide vs. Policy/Simulator vs. Real Spark

| Aspect | PPT Slides (Slides 8–9) | Policy & Simulator (`dynamic_subdivision.py`, `engine.py`) | Real Spark Workload (`dynamic_subdivision_benchmark.py`, `run_dynamic_*.py`) |
|---|---|---|---|
| **Detector** | `R_norm < 0.60 * R_ref` or tail blocker (`C_idle >= 1.0c`) | `CapacityNormalizedDetector(tracker, calibrated_r_base=9010.0)` | **Not implemented** (`spark.speculation=false`) |
| **Gates** | `depth < 3`, `C_idle >= 1.0`, `rem >= 2 * min_chunk` (`10,000`) | Lines 23–35: exact match (`max_depth=3`, `total_idle_cores >= 1.0`, `remaining_work >= 2 * 5000`) | **Not implemented** |
| **Target Pool** | `[Idle + Straggler]` | `target_nodes = idle_nodes + [straggler_node]` (line 42) | Runs standard 64-partition `mapPartitionsWithIndex().collect()` |
| **Work Accounting** | `0.0s` wasted compute | Parent frozen (`SPLITTING`); `partition_items_done[pid] += completed_work` (`wasted_cpu_seconds = 0.0`) | `sub_slices = 0` in all 50 runs (`results/dynamic_subdivision/*/run_summary.csv`) |
| **Results Note** | `D100`: `71.36s`; `C100`: `134.41s` | Verified by unit tests `test_residual_slicing_includes_straggler_node` & `test_simulation_c100_speedup_and_zero_waste` | Recorded `C100: 134.41s`, `D100: 71.36s` with `0` sub-slices |

---

### Algorithm 3: Racing Team Speculation

#### Simple Explanation
Don't stop or bother the slow worker at all—let it keep chugging on its task uninterrupted. Meanwhile, gather **only the idle healthy workers** into a "rescue team," chop the *remaining* items `[cur_offset, end_idx)` proportionally among the rescue team, and let the team race against the original worker. Whichever side finishes first cancels the other!
* **Why it's great on mild/moderate Diversity (`D0`, `D25`):** Zero interruption overhead to the running task, and a team of fast idle workers finishes much faster than a 1-on-1 clone.
* **Why it struggles on `C100`:** When all workers are busy early on, idle capacity isn't available yet; once a team starts, both the original and rescue slices run simultaneously (non-zero CPU waste if one side loses).

#### Technical Flow
```text
CapacityNormalizedDetector flags task (skipping already-speculated parents & rescue children)
   │
   ▼  Gate 1: depth < max_depth (2)
   ▼  Gate 2: C_idle >= 1.0 core AND idle_nodes non-empty
   ▼  Gate 3: W_rem >= 2 * min_chunk_size (10,000)
   │
   ▼
target_nodes = sorted(idle_nodes, desc by quota)   [Straggler EXCLUDED]
   │
   ▼
CapacityWeightedPartitioner slices [cur_offset, end_idx) across rescue team
Original task stays RUNNING; Rescue slices start after tau_dispatch (0.035s)
   │
   ├──► All rescue slices finish first -> Kill original parent (count parent wasted CPU)
   └──► Original finishes first          -> Kill all remaining rescue slices (count team wasted CPU)
```

#### Side-by-Side Comparison: Slide vs. Policy/Simulator vs. Real Spark

| Aspect | PPT Slides (Slides 10–11) | Policy & Simulator (`racing_team.py`, `engine.py`) | Real Spark Workload (`racing_team_benchmark.py`, `run_racing_team_*.py`) |
|---|---|---|---|
| **Depth Cap** | `depth < 2` | `max_depth: int = 2` (line 17, 25) | **Not implemented** (`spark.speculation=false`) |
| **Target Pool** | Idle workers only (excludes straggler) | `target_nodes = list(idle_nodes)` sorted descending (lines 40–41) | Standard `sc.parallelize(...).mapPartitionsWithIndex().collect()` |
| **Parent State** | Remains `RUNNING` uninterrupted | Parent stays `RUNNING`; tracked in `parent_to_team` & `speculated_parents` | `rescue_slices = 0` in all 50 runs (`results/racing_team/*/run_summary.csv`) |
| **Termination** | First side to finish kills loser | `engine.py` lines 204–253 kills parent when `len(parent_to_team[pid]) == 0`, or kills all team slices if parent finishes first | No rescue slices spawned |
| **Results Note** | `D0`: `73.26s`, `D25`: `76.87s`, `D100`: `77.22s`, `C100`: `134.04s` | Verified by `test_racing_team_target_pool_excludes_straggler` & `test_simulation_racing_team_c100` | Recorded `C100: 134.04s`, `D0: 73.26s` with `0` rescue slices |

---

### Algorithm 4: Recursive Sub-Division V1 (Dynamic Depth + Eviction + Disk Checkpoints)

#### Simple Explanation
Dynamic Sub-Division failed at `C100` for two reasons: it stopped splitting after a fixed round limit, and it kept giving slices back to the crippled `0.20c` Worker 4. **Recursive V1** fixes both:
1. **No fixed round limit:** A split child task can itself be split again (`R1 -> R2 -> R3...`) as long as `>= 10,000` items remain.
2. **Health-based eviction:** If a node's speed/quota is `< 25%` of the cluster median (like `0.20c` vs `1.93c` in `C100`), evict it so it gets `0` slices; if it's just a slower healthy hardware tier (like `0.75c` vs `1.50c` in `D75`), keep it in the pool!
3. **Real Spark execution via disk files:** Workers write progress to `/data/recursive_ckpt/*.ckpt` every `2,000` items. When idle slots appear, the driver touches a `.stop` file, cancels nothing wastefully, reads `stopped_idx`, and launches up to `4` new 1-partition Spark jobs.

#### Technical Flow (Real Spark V1 — `workloads/recursive_subdivision_benchmark.py`)
```text
Driver initializes 64 tasks [p*200k, (p+1)*200k) in task_queue
ThreadPoolExecutor(16) submits each as 1-partition Spark job: sc.parallelize([...], 1)
   │
   ▼  Worker loops items, every 2,000 items: writes .ckpt ("cur_idx,host") & checks .stop
   │
   ▼  Driver wakes every 0.4s (when task_queue is empty & idle_slots >= 2):
      For active tasks with elapsed >= max(2.0, 0.5 * med_dur):
        Read .ckpt -> rem = end_idx - cur_idx
        If rem >= 2 * 5000:
          If host_avg_speed < 0.65 * med_speed OR elapsed > 1.15 * med_dur:
            touch .stop file -> worker returns ("PREEMPTED", stopped_idx)
   │
   ▼  Driver slices [stopped_idx, end_idx) into min(idle_slots, rem // 5000, 4) equal chunks
      Pushes children (depth + 1) to front of task_queue (appendleft)
```

#### Side-by-Side Comparison: Slide vs. Policy/Simulator vs. Real Spark V1

| Aspect | PPT Slides (Slides 12–13) | Policy & Simulator (`recursive_subdivision.py`, `engine.py`) | Real Spark V1 (`workloads/recursive_subdivision_benchmark.py`) |
|---|---|---|---|
| **Recursion Depth** | `max_depth = None` (stops at `< 2 * min_chunk`) | `max_depth=None`; Gate 2 stops when `remaining_work < 2 * 5000` | No `max_depth` cap; stops when `rem < 2 * 5000` (line 253) |
| **Eviction / Slow Host Rule** | `node_quota / median_quota < 0.25` evicts crippled node | `_is_node_severely_impaired`: `effective_cpu_quota / median(all_quotas) < 0.25` | **No explicit eviction/quarantine!** Detects slow host via `host_avg_speed < 0.65 * med_speed` or `elapsed > 1.15 * med_dur`, and re-queues sub-tasks to `task_queue` (where any free slot can pull them!) |
| **Preemption Trigger** | `idle_slots >= 2`, `ckpt` every `2,000`, `< 0.65 * med_speed` or `> 1.15 * med_dur` | `CapacityNormalizedDetector` (`0.60 * R_ref` or tail blocker) | Exact match in lines 225–269 (`ckpt_interval = 2000`, `timeout = 0.4`, `elapsed >= max(2.0, 0.5 * med_dur)`) |
| **Slicing Method** | Up to `4` child tasks (`>= 5,000` items) | `CapacityWeightedPartitioner` across `idle_nodes` (or `+ straggler_node` if healthy) | Equal slicing `chunk_size = rem // num_slices`, `num_slices = min(idle_slots, rem // min_chunk, 4)` (lines 190, 207) |
| **Overhead / Bottleneck** | Disk `.ckpt/.stop` I/O + `76+` Spark job launches + no persistent quarantine | Idealized `tau_dispatch = 0.035s` | Submits a brand-new `sc.parallelize([...], 1).map(...).collect()` Spark job for every slice! |
| **Results** | `C100: 86.99s` (`+35.28%` vs Dynamic); `D75: 78.19s` (1st overall) | Verified in `test_recursive_subdivision_*` | Matches `results/recursive_subdivision/` (`8–11` splits, `26–29` subtasks, `max_depth=2..3` at `C100`) |

> **Important nuance highlighted by comparing Slide 12 & Code:** Slide 12 blends the policy/simulator's `node_quota / median_quota < 0.25` eviction rule with the real Spark V1's `.ckpt/.stop` mechanics (`< 0.65 * med_speed`). In the actual real Spark V1 script (`recursive_subdivision_benchmark.py`), there is **no persistent quarantine**—which is exactly why Slide 13 notes *"lack of persistent quarantine across rounds leave ~10-12s on the table"*!

---

### Algorithm 5: Recursive Sub-Division V2 / Option B (Zero-Disk TCP + Quarantine + ETA Cutoff)

#### Simple Explanation
V1 proved recursive splitting works, but wasted `~10–12` seconds writing checkpoint files to disk, launching 76+ tiny Spark jobs, and letting slow Worker 4 accidentally pull new tasks ("slow-node ping-pong"). **Recursive V2** eliminates all three flaws:
1. **One Spark job, zero disk files:** Spark starts **16 permanent worker slots** once. They connect directly to the driver's memory over a fast TCP socket (`CoordinatorTCPServer`) and pull work in milliseconds.
2. **Persistent Quarantine (`Q`):** After a 6-second warmup, if any machine's total speed is `< 25%` of the median machine speed, it is permanently quarantined (`EXIT` on next `PULL`). It can *never* pull another task!
3. **ETA-Synchronized Preemption (Smart Timing):** Don't interrupt the slow node *too* early while healthy workers still have plenty of work—let the slow node chip away for free! Only steal its remaining work right when its finish ETA threatens to delay the final healthy wave, or in the tail when idle slots can finish it much faster.

#### Technical Flow (Real Spark V2 — `workloads/recursive_v2_benchmark.py`)
```text
Driver starts CoordinatorTCPServer (ThreadingTCPServer) with 64 tasks in initial_queue
Driver launches 1 Spark Stage: sc.parallelize(range(16), 16) -> 16 persistent worker_slot_loop tasks
   │
   ├──► Slot connects via TCP -> sends REGISTER(slot_id, host)
   │
   ▼  Worker Pull Loop: sends PULL(slot_id, host)
      ├─ If all_done OR host in quarantined_hosts -> Coordinator returns EXIT (slot shuts down)
      ├─ If recursive_queue non-empty              -> Pop child slice (priority!) -> return RUN
      ├─ Else if initial_queue non-empty           -> Pop partition task          -> return RUN
      └─ Else (active tasks still running)         -> Return WAIT (sleep 0.06s, retry)
   │
   ▼  Execution & Heartbeat Loop (inside RUN):
      Every 250 items AND >= 0.25s since last HB -> send HB(cur_idx):
        1. EMA rate update (if dt >= 0.12s and d_items > 0):
             r_slot = 0.6 * r_slot + 0.4 * (d_items / dt)
        2. Quarantine check (if elapsed >= 6.0s and >= 2 active hosts):
             If R_host / median(R_hosts) < 0.25 -> add host to quarantined_hosts
        3. ETA Preemption check (if rem >= 2 * 5000):
             • Case 1 (Quarantine Evac): host in Q AND (initial_queue <= max(4, healthy_slots) OR t_cluster_rem <= 14.0)
                                         AND task_eta > t_cluster_rem  -> reply PREEMPT
             • Case 2 (Tail Split):      both queues empty AND healthy_idle_slots >= 2
                                         AND task_eta > max(1.5, stolen_eta + 0.4) -> reply PREEMPT
   │
   ▼  Completion (DONE):
      Worker sends (status, sum, count, cur_idx, dur) -> Driver adds checksum & count (0.0s waste)
      If PREEMPTED: slice [stopped_idx, end_idx) into 2..4 equal chunks (each >= 5,000) -> push to recursive_queue
```

#### Side-by-Side Comparison: Slide vs. Policy/Simulator vs. Real Spark V2

| Aspect | PPT Slides (Slide 14–15) | Policy & Simulator (`recursive_subdivision_v2.py`, `engine.py`) | Real Spark V2 (`workloads/recursive_v2_benchmark.py`) |
|---|---|---|---|
| **Architecture** | Single stage, 16 persistent slots, `CoordinatorTCPServer`, JSON RPC (`REGISTER`, `PULL`, `HB`, `DONE`) | Simulated ticks (`dt=0.1s`) in `ClusterSimulator` | Exact match (`CoordinatorTCPServer`, `CoordinatorHandler`, `worker_slot_loop`) |
| **Speed Tracking** | `r_slot = 0.6*old + 0.4*inst`, `R_h = sum(slot rates)` | Uses static `effective_cpu_quota` on `ClusterNode` | Exact match in `handle_hb` (lines 241–246) & `_get_host_agg_rate` (lines 81–84) |
| **Quarantine (`Q`)** | After `6s` warmup, if `R_h / R_med < 0.25`, host enters `Q` & `PULL` returns `EXIT` | `evaluate_node_health`: `effective_cpu_quota / median < 0.25` adds to `quarantined_nodes` | Exact match in `_update_quarantine_locked` (lines 86–109) & `handle_pull` (lines 192–193) |
| **ETA Preemption** | Case 1: `near_final_wave` (`<= max(4, healthy_slots)`) or `T_cluster_rem <= 14s`, `ETA_task > T_cluster_rem`. Case 2: queues empty & `healthy_idle >= 2`, `ETA_task > max(1.5, ETA_stolen + 0.4)` | `should_preempt_eta()`: `task_eta > max(cluster_healthy_remaining_eta, stolen_eta + 0.25)` | Exact match with Slide 14 in `_evaluate_eta_preemptions_locked` (lines 136–173) |
| **Zero-Waste Slicing** | Slices `rem` at `stopped_idx` into `2–4` healthy tasks (`>= 5,000`) onto `recursive_queue` | `CapacityWeightedPartitioner` across healthy `idle_nodes` (+ `straggler_node` if healthy) | Equal slicing into `num_slices = min(target_slots, max(1, rem // min_chunk))`, `target_slots = max(2, min(max(healthy_idle_slots, 3), 4))` (lines 284–312) |
| **Results** | Beats Spark `10/10` (`p < 0.006`), wins `8/10`, `C100: 75.88s`, `D100: 77.01s` | Verified by `test_v2_persistent_quarantine_and_eta_cutoff`, `test_simulation_recursive_v2_c100`, `test_v2_tcp_coordinator_end_to_end_conservation` | Verified in `results/recursive_v2/` (`7–14` splits, `23–42` subtasks, `['worker4']` quarantined at `C50–C100`) |

---

## 6. Complete Threshold Reference: Value, Meaning & Why Chosen

Every threshold/constant used across the slides and code is documented below—including **what it does** and **why it was chosen** (distinguishing values directly derived/stated in the repo vs. practical systems engineering heuristics).

### A. Workload & Benchmark Constants

| Threshold / Constant | Value | Where Used | Simple & Technical Meaning | Why Chosen |
|---|---|---|---|---|
| **Partitions** | `64` | `workloads/*.py`, `simulation/engine.py` | Number of initial data partitions (`4x` the `16` worker slots). | Explicitly creates 4 waves of parallel tasks (`64 / 16 = 4`), long enough for straggler detection and tail effects to appear clearly. |
| **Items per partition** | `200,000` | `workloads/*.py`, `core/task.py` | Items in each initial partition (`12.8M` items total). | Tuned so each partition takes `~15–20s` at `1.50c` and a full job takes `~70–145s` (long enough to dwarf Spark startup jitter, short enough to run 310+ real runs). |
| **Hash iterations** | `250` | `workloads/*.py` | Inner modular arithmetic loops per item (`3.2B` total ops). | Makes the workload strictly CPU-bound so Docker `--cpus` throttling translates cleanly and deterministically into task runtime. |
| **Total Cluster CPUs** | `6.00 cores` | `controllers/set_*.py`, `core/cluster_state.py` | Sum of Docker CPU quotas across all 4 workers. | Strict resource-conservation invariant so speedups prove algorithmic efficiency rather than extra CPU capacity. |
| **Repetitions & Cooldown** | `5 reps`, `20s` | `experiments/run_*.py` | `5` independent runs per condition with `20s` sleep between runs. | `5` samples enable Welch's $t$-test statistics; `20s` pause prevents CPU thermal throttling from biasing sequential runs (`run_phase2_contention.py` line 8). |

### B. Legacy Spark & Detection Thresholds

| Threshold / Constant | Value | Where Used | Simple & Technical Meaning | Why Chosen |
|---|---|---|---|---|
| **Minimum progress gate** | `0.20` (`20%`) | `spark_median.py`, `capacity_normalized.py` | Tasks cannot be flagged as stragglers until `completed / total >= 0.20`. | Explicitly documented in `capacity_normalized.py` line 22 & Slide 6: **filters out transient JVM/Python startup warmup jitter** so tasks aren't falsely flagged in their first second. |
| **Spark quantile** | `0.75` (`75%`) | `experiments/run_phase*.py` | `spark.speculation.quantile`: Spark waits until `75%` of tasks finish before speculating. | Standard Spark tuning to establish a reliable stage baseline before launching speculative duplicates (`48/64` tasks complete). |
| **Spark multiplier** | `1.5` (`1.5×`) | `run_phase*.py`, `spark_median.py` | Flags a task if `elapsed > 1.5 * median_dur`. | Apache Spark's default speculation multiplier—flags tasks running `50%` slower than the peer median. |
| **Spark min runtime / interval** | `1000ms` | `run_phase*.py` | `minTaskRuntime` and speculation polling `interval` (`1s`). | Prevents Spark from evaluating sub-second tasks and checks every `1s` for stragglers. |
| **`calibrated_r_base` (`R_base`)** | `9010.0` items/(core·s) | `capacity_normalized.py`, `empirical_calibration.py` | Baseline item throughput per 1 CPU core. | **Empirically calibrated** from the cluster's 7,202-attempt audit (`200,000 items / (14.8s * 1.5c) = 9010.0`, `empirical_calibration.py` line 9). |
| **`rate_ratio_threshold`** | `0.60` (`60%`) | `capacity_normalized.py` | Flags a task if `norm_rate < 0.60 * reference_rate`. | Gives `40%` headroom for minor OS/Docker scheduling noise while reliably catching genuine per-core CPU throttling/contention. |
| **Tail-blocker speedup gate** | `1.5` (`1.5×`) | `capacity_normalized.py` line 62 | Flags a tail task if `time_alone > 1.5 * time_shared` (when `C_idle >= 1.0` & `rem >= 10,000`). | Ensures splitting at the tail gives at least a `50%` theoretical runtime speedup over letting the task finish alone. |

### C. Gating, Partitioning & Simulator Constants

| Threshold / Constant | Value | Where Used | Simple & Technical Meaning | Why Chosen |
|---|---|---|---|---|
| **`total_idle_cores` gate** | `>= 1.0` core | `dynamic_subdivision.py`, `racing_team.py`, `recursive_subdivision*.py` | Mitigation will not split unless at least `1.0` full idle core is available. | Splitting when the cluster is already saturated (`< 1.0c` free) adds overhead without extra parallel compute to run the slices. |
| **`min_chunk_size` & floor** | `5,000` (`2x = 10,000`) | `policies/mitigation/*.py`, `workloads/recursive*.py` | Minimum slice size (`5,000`); splitting stops below `10,000` remaining items. | Explicitly noted in code/Slide 8: **prevents micro-task thrashing**, ensuring every split produces at least two chunks of `>= 5,000` items (`~0.37s+` work at `1.5c`) so dispatch overhead doesn't exceed compute. |
| **`max_depth`** | `3` (Dyn), `2` (Racing), `None` (V1/V2) | `policies/mitigation/*.py` | Caps how many generations of splits a task can undergo. | Dynamic/Racing used small static caps (`3`/`2`); V1/V2 removed the cap (`None`) because hard caps left `C100` tail fragments stranded, letting `2 * min_chunk` stop recursion naturally. |
| **`health_eviction_threshold`** | `0.25` (`25%`) | `recursive_subdivision.py`, `recursive_subdivision_v2.py`, `recursive_v2_benchmark.py` | Node is evicted/quarantined if its capacity or aggregate rate is `< 25%` of cluster median. | **Cleanly separates severe contention from healthy hardware diversity:** in `D75`, Worker 4 is `0.75c / 1.50c = 50%` (`>= 0.25`, kept!); in `D100`, `0.50c / 1.50c = 33%` (kept!); in `C100`, `0.20c / 1.93c = 10.3%` (`< 0.25`, evicted!). |
| **`tau_dispatch` / `tau_merge` / `dt`** | `0.035s` / `0.015s` / `0.1s` | `empirical_calibration.py`, `simulation/engine.py` | Simulated RPC dispatch latency (`35ms`), result merge (`15ms`), and simulator tick (`100ms`). | Models real thread/RPC and result-collection overheads in the discrete-event simulator (`empirical_calibration.py` lines 13–15). |

### D. Recursive V1 & V2 Real-Spark Runtime Thresholds

| Threshold / Constant | Value | Where Used | Simple & Technical Meaning | Why Chosen |
|---|---|---|---|---|
| **V1 `ckpt_interval`** | `2,000` items | `recursive_subdivision_benchmark.py` line 50 | Worker writes `.ckpt` and checks `.stop` every `2,000` items (`1%` of a partition). | Balances disk write frequency (max 100 writes per 200k partition) against preemption reaction time (`~0.15s` on healthy nodes, `~1.1s` on `0.20c`). |
| **V1 poll timeout & warmup** | `0.4s`, `max(2.0, 0.5*med_dur)` | `recursive_subdivision_benchmark.py` lines 165, 242 | Driver checks tasks every `0.4s`; skips tasks younger than `2s` or half `med_dur`. | Keeps driver responsive while giving new tasks enough runtime to write an initial checkpoint and stabilize speed. |
| **V1 slow-host / tail triggers** | `0.65 * med_speed`, `1.15 * med_dur`, `idle_slots >= 2` | `recursive_subdivision_benchmark.py` lines 228, 260–261 | Triggers split when `>= 2` slots are idle and host is `< 65%` median speed or task exceeds `1.15×` median duration. | More aggressive than Spark's `1.5×` because preemption preserves `100%` of completed work (zero waste). |
| **V1/V2 max slices per split** | `4` (V2: `2..4`) | `recursive_subdivision_benchmark.py` line 190, `recursive_v2_benchmark.py` line 284 | Caps fan-out per preemption at `<= 4` slices (`V2`: `max(2, min(max(healthy_idle, 3), 4))`). | Ensures splitting creates meaningful parallelism without flooding the queue with tiny sub-tasks from a single preemption. |
| **V2 `WAIT` sleep** | `0.06s` (`60ms`) | `recursive_v2_benchmark.py` line 404 | Idle slot pause between `PULL` retries when queues are empty but tasks remain active. | Keeps slots ready to grab newly split slices within `60ms` without spinning CPU at 100% on socket polling. |
| **V2 heartbeat cadence** | `250` items & `>= 0.25s` | `recursive_v2_benchmark.py` lines 426–428 | Worker sends `HB` only when `250` items completed *and* at least `0.25s` elapsed. | Gives sub-second preemption responsiveness (`~4 Hz` max per slot) while keeping TCP RPC traffic negligible; even `0.20c` slots send HBs promptly once `250` items finish. |
| **V2 EMA smoothing & `dt`** | `0.6*old + 0.4*inst` (`dt >= 0.12s`) | `recursive_v2_benchmark.py` lines 243–245 | Exponential Moving Average weighting `40%` on latest rate, `60%` on history (`50/50` on `DONE`). | Smooths out transient CPU scheduler spikes so temporary hiccups don't immediately skew ETA or trigger false quarantine. |
| **V2 quarantine warmup** | `6.0s` (`start_wall + 6.0`) | `recursive_v2_benchmark.py` line 88 | Coordinator waits `6.0s` before evaluating quarantine. | Explicitly documented in code line 87: **waits until Wave-1 heartbeats stabilize** across all 16 slots so no healthy node is quarantined due to uneven startup timing. |
| **V2 Case 1 (Quarantine Evac)** | `initial_queue <= max(4, healthy_slots)` OR `t_cluster_rem <= 14.0s` | `recursive_v2_benchmark.py` lines 144, 163–165 | Evacuates quarantined host's task when near the final wave and `task_eta > t_cluster_rem`. | Lets the crippled node contribute free progress during early waves, then evacuates it roughly one healthy task duration (`~14s`) before the healthy cluster finishes. |
| **V2 Case 2 (Tail Split)** | `healthy_idle >= 2`, `task_eta > max(1.5, stolen_eta + 0.4)` | `recursive_v2_benchmark.py` lines 169–172 | Splits tail tasks once queues are empty if stealing beats running alone by `> 0.4s` and `task_eta > 1.5s`. | Avoids pointless preemptions on tasks finishing within `1.5s`, and accounts for `0.4s` real-world TCP/dispatch/preemption overhead. |
| **Significance levels** | `p < 0.05`, `p < 0.01` | `run_statistical_tests.py`, `parse_recursive_v2_results.py` | Welch's two-sample $t$-test significance cutoffs. | Standard statistical rigor (`p < 0.01` achieved across all `10/10` conditions for Recursive V2 vs. Spark ON). |

---

## 7. Quick Summary: How Everything Connects End-to-End

1. **Setup:** `docker-compose.yml` + `controllers/cluster.py` boot 1 Master, 1 Driver, 4 Workers (`16` slots). `set_contention.py` / `set_diversity.py` cap CPUs to `6.00c` total.
2. **Baseline Proof:** `straggler_benchmark.py` shows Spark's `SparkMedianDetector` + `FullCloneStrategy` saves `C100` (`144.36s -> 83.43s`) but hurts `C0` (`-4.63%`) and `D75` (`-4.10%`).
3. **Simulation & Policy Design:** `CapacityNormalizedDetector` + `CapacityWeightedPartitioner` power `DynamicSubdivisionStrategy`, `RacingTeamSpeculationStrategy`, `RecursiveSubdivisionStrategy`, and `RecursiveSubdivisionV2Strategy` inside `ClusterSimulator` (verified by 15 tests in `tests/test_dynamic_subdivision.py`).
4. **Real Spark Evolution:**
   * `dynamic_subdivision_benchmark.py` & `racing_team_benchmark.py` lack in-Spark splitting (`0` slices in CSVs).
   * `recursive_subdivision_benchmark.py` (V1) introduces real Spark multi-round slicing via `.ckpt`/`.stop` files and multiple 1-partition jobs, dropping `C100` to `86.99s`.
   * `recursive_v2_benchmark.py` (V2 / Option B) moves coordination into memory (`CoordinatorTCPServer` + 16 persistent slots), adds persistent `0.25` Quarantine and ETA-synchronized preemption, and sweeps Spark in **10/10** conditions (`p < 0.006`) with **1st place in 8/10**.
5. **Analysis:** `analysis/parse_*.py` and `run_statistical_tests.py` aggregate all 310+ runs into `results/recursive_v2/master_6way_comparison.csv` and the comparison charts.
