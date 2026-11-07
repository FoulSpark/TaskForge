import argparse
import csv
import json
import os
import platform
import statistics
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

# Ensure 'src' is discoverable on python path regardless of execution environment
REPO_ROOT = Path(__file__).resolve().parent.parent.parent
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import httpx

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "raw"
REPORTS_DIR = BASE_DIR / "reports"

RAW_DIR.mkdir(parents=True, exist_ok=True)
REPORTS_DIR.mkdir(parents=True, exist_ok=True)


WORKLOAD_PROFILES = {
    "smoke": {"total_tasks": 100, "concurrency": 5},
    "baseline": {"total_tasks": 1000, "concurrency": 20},
    "moderate": {"total_tasks": 5000, "concurrency": 50},
    "heavy": {"total_tasks": 10000, "concurrency": 100},
}


def run_single_trial(
    base_url: str,
    total_tasks: int,
    concurrency: int = 10,
    task_type: str = "json_transform",
    in_process: bool = False,
) -> Dict[str, Any]:
    """Execute a single load benchmark trial against the TaskForge API."""
    latencies: List[float] = []
    task_ids: List[str] = []
    errors = 0

    if in_process:
        os.environ["TASKFORGE_TESTING"] = "1"
        from fastapi.testclient import TestClient
        from taskforge.main import app
        from taskforge.database.session import init_db
        init_db()
        client = TestClient(app)
    else:
        client = httpx.Client(base_url=base_url, timeout=30.0)

    start_wall = time.perf_counter()

    for i in range(total_tasks):
        t0 = time.perf_counter()
        try:
            resp = client.post(
                "/api/v1/tasks",
                json={
                    "task_name": f"Bench-{task_type}-{i}",
                    "task_type": task_type,
                    "payload": {"records": [x for x in range(10)]},
                    "priority": (i % 4) + 1,
                },
            )
            lat = (time.perf_counter() - t0) * 1000  # ms
            latencies.append(lat)
            if resp.status_code == 201:
                task_ids.append(resp.json()["id"])
            else:
                errors += 1
        except Exception:
            errors += 1

    try:
        client.close()
    except Exception:
        pass

    total_time = time.perf_counter() - start_wall
    throughput = round(total_tasks / total_time, 2) if total_time > 0 else 0

    latencies.sort()
    n = len(latencies)
    p50 = round(latencies[int(n * 0.50)], 2) if n else 0.0
    p95 = round(latencies[int(n * 0.95)], 2) if n else 0.0
    p99 = round(latencies[int(n * 0.99)], 2) if n else 0.0

    return {
        "total_tasks": total_tasks,
        "duration_seconds": round(total_time, 3),
        "throughput_rps": throughput,
        "latency_p50_ms": p50,
        "latency_p95_ms": p95,
        "latency_p99_ms": p99,
        "error_count": errors,
        "error_rate_pct": round((errors / total_tasks) * 100, 2),
    }


def run_five_run_experiment(
    experiment_id: str,
    base_url: str,
    profile_name: str = "smoke",
    task_type: str = "json_transform",
    in_process: bool = False,
) -> Dict[str, Any]:
    """
    Executes the formal 5-Run Benchmark Methodology as required by TaskForge Handbook:
    Records 5 repeated trials, calculates median, mean, variability, and exports raw data.
    """
    profile = WORKLOAD_PROFILES.get(profile_name, WORKLOAD_PROFILES["smoke"])
    total_tasks = profile["total_tasks"]
    concurrency = profile["concurrency"]

    print(f"=== Starting Experiment {experiment_id} ({profile_name.upper()} Profile: {total_tasks} tasks) ===")

    trials = []
    for run_num in range(1, 6):
        print(f"  -> Running Trial {run_num}/5...")
        trial_result = run_single_trial(
            base_url=base_url,
            total_tasks=total_tasks,
            concurrency=concurrency,
            task_type=task_type,
            in_process=in_process,
        )
        trial_result["trial_number"] = run_num
        trials.append(trial_result)
        time.sleep(0.5)

    throughputs = [t["throughput_rps"] for t in trials]
    p50s = [t["latency_p50_ms"] for t in trials]
    p95s = [t["latency_p95_ms"] for t in trials]
    p99s = [t["latency_p99_ms"] for t in trials]

    summary = {
        "experiment_id": experiment_id,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "profile": profile_name,
        "task_type": task_type,
        "total_tasks_per_trial": total_tasks,
        "trials_count": 5,
        "hardware": {
            "platform": platform.platform(),
            "processor": platform.processor(),
            "python_version": platform.python_version(),
        },
        "aggregate_metrics": {
            "throughput_rps": {
                "median": round(statistics.median(throughputs), 2),
                "mean": round(statistics.mean(throughputs), 2),
                "stdev": round(statistics.stdev(throughputs), 2) if len(throughputs) > 1 else 0.0,
            },
            "latency_p50_ms": {
                "median": round(statistics.median(p50s), 2),
                "mean": round(statistics.mean(p50s), 2),
            },
            "latency_p95_ms": {
                "median": round(statistics.median(p95s), 2),
                "mean": round(statistics.mean(p95s), 2),
            },
            "latency_p99_ms": {
                "median": round(statistics.median(p99s), 2),
                "mean": round(statistics.mean(p99s), 2),
            },
        },
        "trials": trials,
    }

    # Export raw JSON
    raw_file = RAW_DIR / f"{experiment_id}_{profile_name}_{int(time.time())}.json"
    with open(raw_file, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    # Export summary CSV
    csv_file = REPORTS_DIR / f"{experiment_id}_summary.csv"
    with open(csv_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["trial", "total_tasks", "duration_s", "throughput_rps", "p50_ms", "p95_ms", "p99_ms", "errors"])
        for t in trials:
            writer.writerow([
                t["trial_number"],
                t["total_tasks"],
                t["duration_seconds"],
                t["throughput_rps"],
                t["latency_p50_ms"],
                t["latency_p95_ms"],
                t["latency_p99_ms"],
                t["error_count"],
            ])

    print(f"=== Experiment {experiment_id} Finished! ===")
    print(f"  Median Throughput: {summary['aggregate_metrics']['throughput_rps']['median']} req/s")
    print(f"  Median P50 Latency: {summary['aggregate_metrics']['latency_p50_ms']['median']} ms")
    print(f"  Median P95 Latency: {summary['aggregate_metrics']['latency_p95_ms']['median']} ms")
    print(f"  Raw JSON: {raw_file}")
    print(f"  CSV Report: {csv_file}\n")

    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TaskForge 5-Run Benchmark Runner")
    parser.add_argument("--url", type=str, default="http://127.0.0.1:8000")
    parser.add_argument("--experiment", type=str, default="E01_baseline")
    parser.add_argument("--profile", type=str, default="smoke", choices=["smoke", "baseline", "moderate", "heavy"])
    parser.add_argument("--task-type", type=str, default="json_transform")
    parser.add_argument("--in-process", action="store_true", help="Run benchmark in-process via FastAPI TestClient")
    args = parser.parse_args()

    run_five_run_experiment(
        experiment_id=args.experiment,
        base_url=args.url,
        profile_name=args.profile,
        task_type=args.task_type,
        in_process=args.in_process,
    )
