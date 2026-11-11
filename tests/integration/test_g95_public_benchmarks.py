"""Gate G9.5: Automated Verification of Canonical Public Benchmark Suite.

Verifies:
1. End-to-end execution of benchmarks/public/run_public_benchmarks.py in quick mode.
2. Independent single-workload invocation (e.g. --workload W-01).
3. Schema compliance of generated benchmark artifacts against benchmarks/public/schema/v1.json.
4. Correctness of throughput calculation, warmup exclusion, and latency percentiles.
"""

import json
from pathlib import Path
import subprocess
import sys

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from benchmarks.public.run_public_benchmarks import validate_schema

SCHEMA_PATH = REPO_ROOT / "benchmarks" / "public" / "schema" / "v1.json"
HARNESS_PATH = REPO_ROOT / "benchmarks" / "public" / "run_public_benchmarks.py"


def test_g95_public_benchmarks_schema_file_exists():
    """Verify that the versioned v1.json schema file exists and is valid JSON."""
    assert SCHEMA_PATH.is_file(), f"Schema file missing at {SCHEMA_PATH}"
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    assert schema["title"] == "TaskForgePublicBenchmarkV1"
    assert "workload_results" in schema["properties"]


def test_g95_public_benchmarks_quick_execution(tmp_path):
    """Verify that run_public_benchmarks.py --quick executes cleanly across all 3 workloads."""
    summary_path = tmp_path / "test_quick_bench.json"
    res = subprocess.run(
        [
            sys.executable,
            str(HARNESS_PATH),
            "--quick",
            "--output",
            str(summary_path),
        ],
        capture_output=True,
        text=True,
        timeout=60,
    )

    assert res.returncode == 0, f"Benchmark harness failed with code {res.returncode}: {res.stderr}"
    assert summary_path.is_file(), "Benchmark summary artifact must be written to disk"

    data = json.loads(summary_path.read_text(encoding="utf-8"))
    validate_schema(data, SCHEMA_PATH)

    assert data["schema_version"] == "1.0.0"
    assert "provenance" in data
    assert data["provenance"]["production_baseline_sha"] == "a89858b73f8488e05cbcc2393a61f3080ffc7a38"

    wr = data["workload_results"]
    assert "W-01" in wr
    assert "W-02" in wr
    assert "W-03" in wr

    for wid in ["W-01", "W-02", "W-03"]:
        res_data = wr[wid]
        assert res_data["task_count"] > 0
        assert res_data["warmup_tasks_excluded"] > 0
        assert res_data["measured_tasks_count"] > 0
        assert res_data["measured_tasks_count"] == res_data["task_count"] - res_data["warmup_tasks_excluded"]
        assert res_data["throughput_tasks_per_sec"] > 0
        
        # Outcome accounting (R3 clarification)
        outcomes = res_data["task_outcomes"]
        assert outcomes["seeded_count"] == res_data["task_count"]
        assert outcomes["warmup_count"] == res_data["warmup_tasks_excluded"]
        assert outcomes["measured_count"] == res_data["measured_tasks_count"]
        assert outcomes["success_count"] == res_data["task_count"]
        assert outcomes["measured_success_count"] == res_data["measured_tasks_count"]
        assert outcomes["failed_count"] == 0

        lats = res_data["latencies_ms"]
        assert lats["p50"] > 0
        assert lats["p95"] >= lats["p50"]
        assert lats["p99"] >= lats["p95"]
        assert lats["max"] >= lats["p99"]
        assert lats["min"] <= lats["p50"]

        # Latency decomposition invariant verification (R1)
        rec = lats["reconciliation"]
        assert rec["reconciled"] is True
        assert rec["mean_discrepancy_ms"] <= 0.002
        assert rec["max_per_task_discrepancy_ms"] < 0.001
        sum_of_means = lats["outbox_dwell_ms"]["mean"] + lats["queue_dwell_ms"]["mean"] + lats["handler_execution_ms"]["mean"]
        assert abs(lats["mean"] - sum_of_means) <= 0.002


def test_g95_public_benchmarks_single_workload(tmp_path):
    """Verify that --workload W-01 executes only W-01 and emits valid schema."""
    summary_path = tmp_path / "test_w01_bench.json"
    res = subprocess.run(
        [
            sys.executable,
            str(HARNESS_PATH),
            "--quick",
            "--workload",
            "W-01",
            "--output",
            str(summary_path),
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert res.returncode == 0, f"Single-workload harness failed: {res.stderr}"
    data = json.loads(summary_path.read_text(encoding="utf-8"))
    validate_schema(data, SCHEMA_PATH)

    assert list(data["workload_results"].keys()) == ["W-01"]
    assert data["workload_results"]["W-01"]["task_count"] == 50
    assert data["workload_results"]["W-01"]["latencies_ms"]["reconciliation"]["reconciled"] is True


def test_g95_public_benchmarks_latency_invariant_enforcement():
    """Verify that the harness validator rejects an artifact where latency components do not reconcile."""
    malformed_data = {
        "schema_version": "1.0.0",
        "timestamp_utc": "2026-11-11T12:00:00Z",
        "provenance": {
            "git_commit": "abcdef1234567890",
            "git_dirty": False,
            "production_baseline_sha": "a89858b73f8488e05cbcc2393a61f3080ffc7a38",
        },
        "environment": {
            "os": "Test OS",
            "cpu_model": "Test CPU",
            "cpu_logical_cores": 8,
            "ram_gb": 16.0,
            "python_version": "3.12.0",
            "postgres_version": "PostgreSQL 16",
            "redis_version": "Redis 7",
        },
        "workload_results": {
            "W-01": {
                "task_count": 100,
                "elapsed_seconds": 2.5,
                "throughput_tasks_per_sec": 40.0,
                "latencies_ms": {
                    "p50": 100.0,
                    "p95": 150.0,
                    "p99": 200.0,
                    "min": 50.0,
                    "max": 250.0,
                    "mean": 100.0,
                    "reconciliation": {
                        "reconciled": False,
                        "mean_discrepancy_ms": 15.0,
                    },
                },
                "raw_observations_count": 90,
            }
        },
    }

    with pytest.raises(ValueError, match="Arithmetic reconciliation violation"):
        validate_schema(malformed_data, SCHEMA_PATH)

