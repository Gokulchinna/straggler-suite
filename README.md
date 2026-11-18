# Straggler Detection and Mitigation in Heterogeneous Big-Data Clusters

**An Empirical Evaluation of Speculative Execution under Controlled CPU Contention and Hardware Diversity**

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![Apache Spark 3.5.0](https://img.shields.io/badge/Apache%20Spark-3.5.0-orange.svg)](https://spark.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-Containerized-blue.svg)](https://www.docker.com/)

---

## 👥 Authors & Academic Affiliation
- **Student Researchers:** Pakalapati Gokul Chinna, Padarthy Sailesh Sai, Mothukuri Sai Teja
- **Faculty Mentor:** Dr. Lachure Jaykumar Suraj
- **Department:** Department of Computer Science and Engineering
- **Institution:** Indian Institute of Information Technology Sri City, Chittoor, India

---

## 📌 Executive Summary & Key Findings

Distributed data-parallel runtimes like Apache Spark partition computation into Directed Acyclic Graph (DAG) stages bounded by barrier synchronization. An entire stage cannot finish until its slowest task completes. In shared clusters, multi-tenant contention and hardware generation skews create **stragglers**—abnormally slow tasks that delay overall Job Completion Time (JCT).

While speculative execution launches redundant backup clones for lagging tasks, prior studies lack fine-grained empirical quantification under strictly conserved compute capacity. This suite provides an end-to-end evaluation across **110 cluster executions** and **7,202 audited task attempts** within a 6-container Docker Spark 3.5.0 cluster, enforcing strict Linux cgroups CPU conservation ($\sum C_j = 6.0\text{ cores}$).

### Core Research Discoveries:
1. **Severe Single-Node Contention Win:** Under severe contention (C100, Worker 4 throttled to $0.20\text{c}$), speculative execution achieved a **+42.20% speedup** ($p = 8.34 \times 10^{-8}$), cutting Workload JCT from $144.36\text{s}$ down to $83.43\text{s}$ with a **100% clone win rate**.
2. **Micro-Trace Straggler Proof:** In unmitigated C100 runs, 4 outlier tasks on Worker 4 averaged **$115.05\text{s}$** (running **$7.97\times$ slower** than the $15.08\text{s}$ median), stalling healthy workers for over 90 seconds. Speculative clones launched on fast workers completed in **$11.16\text{s}$ mean duration**, rescuing the stage.
3. **Multi-Tier Hardware Diversity Penalty:** Under multi-tier hardware spread (Phase 3), speculative speedups remained near zero ($\le 1.07\%$) and caused a **-4.10% slowdown at D75** ($p = 0.01794$). Spark launched 40 speculative attempts across 5 runs, but **zero won** ($100\%$ killed or in-flight), proving that blind speculation on diverse hardware wastes compute without latency reduction.
4. **Balanced Homogeneity Overhead:** In balanced clusters (C0/D0), speculative monitoring imposed an avoidable **4.63% runtime tax** ($p = 0.03156$) with zero stragglers present.

---

## 📊 Summary of Experimental Results (11 Conditions)

| Phase & Condition | Profile ($C_1, C_2, C_3, C_4$) | Total Cores | Spec OFF (s) | Spec ON (s) | Speedup (%) | Welch's $t$ ($p$-value) | Statistical Significance |
|---|---|:---:|:---:|:---:|:---:|:---:|:---:|
| **Phase 1 Reference** | $[1.50, 1.50, 1.50, 0.50]$ | $6.0\text{c}$ | $118.06 \pm 4.87$ | $106.19 \pm 4.39$ | **+10.06%** | $t=4.05$ ($p=0.0038$) | **YES (Mitigation)** |
| **C0 (0% Contention)** | $[1.50, 1.50, 1.50, 1.50]$ | $6.0\text{c}$ | $79.82 \pm 0.72$ | $83.70 \pm 3.23$ | **-4.63%** | $t=-2.64$ ($p=0.0316$) | **YES (Overhead)** |
| **C25 (25% Contention)** | $[1.61, 1.61, 1.61, 1.175]$ | $6.0\text{c}$ | $81.75 \pm 1.48$ | $79.91 \pm 1.86$ | +2.31% | $t=1.72$ ($p=0.1246$) | Inconclusive |
| **C50 (50% Contention)** | $[1.72, 1.72, 1.72, 0.85]$ | $6.0\text{c}$ | $80.20 \pm 1.25$ | $80.71 \pm 1.15$ | -0.63% | $t=-0.67$ ($p=0.5245$) | Inconclusive |
| **C75 (75% Contention)** | $[1.825, 1.825, 1.825, 0.525]$ | $6.0\text{c}$ | $104.91 \pm 7.94$ | $84.00 \pm 9.48$ | **+21.03%** | $t=3.74$ ($p=0.0063$) | **YES (Mitigation)** |
| **C100 (100% Contention)**| $[1.93, 1.93, 1.93, 0.20]$ | $6.0\text{c}$ | $144.36 \pm 1.51$ | $83.43 \pm 1.55$ | **+42.20%** | $t=63.21$ ($p<10^{-7}$) | **YES (Decisive Win)** |
| **D0 (0% Diversity)** | $[1.50, 1.50, 1.50, 1.50]$ | $6.0\text{c}$ | $79.86 \pm 0.61$ | $79.74 \pm 0.68$ | +0.16% | $t=0.30$ ($p=0.7763$) | Inconclusive |
| **D25 (25% Diversity)** | $[1.75, 1.625, 1.375, 1.25]$ | $6.0\text{c}$ | $80.21 \pm 1.16$ | $79.96 \pm 1.37$ | +0.32% | $t=0.31$ ($p=0.7653$) | Inconclusive |
| **D50 (50% Diversity)** | $[2.00, 1.75, 1.25, 1.00]$ | $6.0\text{c}$ | $81.65 \pm 1.12$ | $81.84 \pm 1.36$ | -0.23% | $t=-0.24$ ($p=0.8191$) | Inconclusive |
| **D75 (75% Diversity)** | $[2.25, 1.875, 1.125, 0.75]$ | $6.0\text{c}$ | $82.72 \pm 1.06$ | $86.25 \pm 2.06$ | **-4.10%** | $t=-3.28$ ($p=0.0179$) | **YES (Slowdown)** |
| **D100 (100% Diversity)** | $[2.50, 2.00, 1.00, 0.50]$ | $6.0\text{c}$ | $88.42 \pm 4.29$ | $87.49 \pm 2.22$ | +1.07% | $t=1.94$ ($p=0.0925$) | Inconclusive |

---

## 📁 Repository Directory Structure

```
straggler-suite/
│
├── docker-compose.yml              # 6-container Docker network (1 Master, 1 Driver, 4 Workers)
├── requirements.txt                # Python dependencies (numpy, matplotlib, scipy)
├── .gitignore                      # Git configuration excluding raw logs, temp files, and caches
├── README.md                       # Comprehensive suite documentation
│
├── controllers/                    # Cluster orchestration and dynamic Linux cgroups throttle tools
│   ├── cluster.py                  # Cluster lifecycle management (start, stop, status)
│   ├── set_contention.py           # Single-node contention controller (Phase 2)
│   ├── set_diversity.py            # Multi-tier hardware diversity controller (Phase 3)
│   └── set_heterogeneity.py        # Generic cgroup quota applicator enforcing 6.0-core invariant
│
├── workloads/                      # Deterministic compute-intensive arithmetic benchmark
│   └── straggler_benchmark.py      # 64 partitions, 12.8M integers, 3.2B inner-loop operations
│
├── experiments/                    # Automated multi-phase benchmark runners
│   ├── run_phase1_baseline.py      # Phase 1 Reference Configuration runner (10 runs)
│   ├── run_phase2_contention.py    # Phase 2 Single-Node Contention sweep runner (50 runs)
│   └── run_phase3_diversity.py     # Phase 3 Multi-Tier Hardware Diversity sweep runner (50 runs)
│
├── analysis/                       # Event log parsers and statistical evaluation engines
│   ├── parse_phase1.py             # Parses Phase 1 event logs and extracts JCT distributions
│   ├── parse_phase2_contention.py  # Parses Phase 2 contention metrics and computes speedup
│   ├── parse_phase3_diversity.py   # Parses Phase 3 diversity traces and slowdown metrics
│   ├── parse_task_level_proof.py   # Audits all 7,202 task execution events for skew proof
│   ├── run_statistical_tests.py    # Computes Welch's t-test and paired t-test statistics
│   └── generate_cross_phase_synthesis.py # Synthesizes cross-phase JCT and cost-benefit trade-offs
│
└── results/                        # Final experimental plots and statistical summary tables
    ├── graph_system_architecture.png       # End-to-end distributed execution pipeline diagram
    ├── graph_master_jct_comparison.png     # Grand Cross-Phase unmitigated vs. speculative JCT
    ├── graph_master_cost_benefit.png       # Cost-benefit frontier contrasting wins vs. kills
    ├── master_summary_table.csv            # Consolidated benchmark metrics across all 11 conditions
    ├── statistical_significance_tests.csv  # Inferential statistical significance test outcomes
    ├── task_level_distribution_summary.csv # Duration percentiles, outliers, and straggler ratios
    ├── task_level_speculation_mechanics.csv# Complete accounting of starts, wins, and kills
    ├── phase1_baseline/            # Phase 1 plots (mean JCT & distribution) and summary CSVs
    ├── phase2_contention/          # Phase 2 plots (sensitivity curve & speedup bar) and CSVs
    └── phase3_diversity/           # Phase 3 plots (diversity curve & speedup bar) and CSVs
```

---

## 🚀 Quick Start Guide

### Prerequisites
- Linux / WSL2 environment with Docker and Docker Compose installed.
- Python 3.10+ with `virtualenv` or `pip`.

### 1. Installation & Environment Setup
```bash
cd ~/CC/straggler-suite
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Start the Containerized Spark Cluster
```bash
python3 controllers/cluster.py start
python3 controllers/cluster.py status
```

### 3. Run Experiments (Automated Suite)
Each runner automatically applies profile-specific cgroups quotas, executes the benchmark across 5 independent iterations for both Speculation OFF and ON, and enforces a 20-second thermal stabilization pause between runs:

```bash
# Phase 1: Reference Configuration (10 runs)
python3 experiments/run_phase1_baseline.py --reps 5 --cooldown 20

# Phase 2: Single-Node Contention Sweep (50 runs)
python3 experiments/run_phase2_contention.py --reps 5 --cooldown 20

# Phase 3: Cluster-Wide Hardware Diversity Sweep (50 runs)
python3 experiments/run_phase3_diversity.py --reps 5 --cooldown 20
```

### 4. Process Event Traces & Generate Figures
```bash
# Parse individual experimental phases
python3 analysis/parse_phase1.py
python3 analysis/parse_phase2_contention.py
python3 analysis/parse_phase3_diversity.py

# Task-level trace audit (7,202 task attempts)
python3 analysis/parse_task_level_proof.py

# Inferential statistical testing (Welch's & paired t-tests)
python3 analysis/run_statistical_tests.py

# Generate grand cross-phase comparison & cost-benefit frontier
python3 analysis/generate_cross_phase_synthesis.py
```

### 5. Stop the Cluster
```bash
python3 controllers/cluster.py stop
```
