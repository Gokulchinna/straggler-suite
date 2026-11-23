import csv
from pathlib import Path


def get_calibrated_cluster_parameters(results_dir: Path = None) -> dict:
    """
    Extracts calibrated baseline rate and latency bounds from empirical cluster results.
    Defaults to empirical 7,202-attempt audit parameters:
        200,000 items / (14.8s * 1.5c) = 9,010 items/(core*sec)
    """
    params = {
        "r_base": 26733.5,       # Calibrated empirical throughput: 12.8M items / (79.8s * 6.0 cores)
        "tau_dispatch": 0.035,   # 35ms thread/RPC dispatch overhead
        "tau_merge": 0.015,      # 15ms partial result collection overhead
        "time_step": 0.1,        # 100ms simulation tick resolution
    }

    if results_dir and results_dir.exists():
        p1_file = results_dir / "master_summary_table.csv"
        if p1_file.exists():
            try:
                with open(p1_file, "r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        if row.get("condition") == "D0 (0% Diversity)" and row.get("mode") == "OFF":
                            jct = float(row.get("mean_jct", 79.8))
                            # 64 partitions * 200,000 items = 12.8M items across 6.0 cores
                            # r_base = 12,800,000 / (jct * 6.0)
                            params["r_base"] = round(12800000.0 / (jct * 6.0), 1)
                            break
            except Exception:
                pass

    return params
