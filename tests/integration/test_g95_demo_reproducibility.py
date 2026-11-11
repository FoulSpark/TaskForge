"""Gate G9.5: Automated Verification of Demo Harness Reproducibility.

Verifies:
1. Environment pre-flight check returns True when services are healthy.
2. CLI invocation with '--check' exits with code 0.
3. Full programmatic execution of run_demo() satisfies all D1–D12 criteria,
   generates machine-readable output, and exits cleanly with code 0.
"""

import json
from pathlib import Path
import subprocess
import sys

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.demo import run_demo, run_preflight_checks


def test_g95_demo_preflight_checks():
    """Verify that environment pre-flight checks pass when services are available."""
    passed = run_preflight_checks()
    assert passed is True, "Pre-flight checks must pass when PostgreSQL and Redis are active"


def test_g95_demo_cli_check_mode():
    """Verify that 'python scripts/demo.py --check' exits with code 0."""
    res = subprocess.run(
        [sys.executable, str(REPO_ROOT / "scripts" / "demo.py"), "--check"],
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert res.returncode == 0, f"Demo --check must exit with 0, got {res.returncode}: {res.stderr}"
    assert "PASS" in res.stdout
    assert "Python Runtime" in res.stdout
    assert "PostgreSQL Database" in res.stdout
    assert "Redis Message Broker" in res.stdout


def test_g95_demo_full_execution(tmp_path):
    """Verify that demo CLI completes with code 0 and satisfies all D1-D12 criteria."""
    summary_path = tmp_path / "test_demo_summary.json"
    res = subprocess.run(
        [
            sys.executable,
            str(REPO_ROOT / "scripts" / "demo.py"),
            "--output",
            str(summary_path),
        ],
        capture_output=True,
        text=True,
        timeout=60,
    )

    assert res.returncode == 0, f"Demo run must exit with 0, got {res.returncode}: {res.stderr}"
    assert summary_path.is_file(), "Summary artifact must be generated"

    data = json.loads(summary_path.read_text(encoding="utf-8"))
    assert data["status"] == "SUCCESS"
    assert data["exit_code"] == 0

    ac = data["acceptance_criteria"]
    assert ac["D1_environment_validation"] is True
    assert ac["D2_isolated_resources"] is True
    assert ac["D3_automatic_migrations"] is True
    assert ac["D4_deterministic_workload"]["total_tasks"] == 30
    assert ac["D5_priority_ordering"]["high_before_normal"] is True
    assert ac["D5_priority_ordering"]["normal_before_low"] is True
    assert ac["D5_priority_ordering"]["fifo_within_tiers"] is True
    assert ac["D6_broker_outage_recovery"]["durable_event_loss"] == 0
    assert ac["D7_stale_lease_fencing"]["stale_commit_rejected"] is True
    assert ac["D7_stale_lease_fencing"]["authoritative_winner"] == "Worker B"
    assert ac["D7_stale_lease_fencing"]["guarded_sql_stale_affected_rows"] == 0
    assert ac["D7_stale_lease_fencing"]["fencing_counter_increment"] >= 1.0
    assert ac["D7_stale_lease_fencing"]["production_path_without_mocking"] is True
    assert ac["D8_terminal_duplicate_suppression"]["duplicate_message_acked"] is True
    assert ac["D9_identity_reconciliation"]["task_identity_set_equality"] is True
    assert ac["D9_identity_reconciliation"]["event_identity_set_equality"] is True
    assert ac["D9_identity_reconciliation"]["task_result_set_equality"] is True
    assert ac["D9_identity_reconciliation"]["unaccounted_tasks_count"] == 0
    assert ac["D9_identity_reconciliation"]["unaccounted_events_count"] == 0
    assert ac["D10_terminal_completion"]["completion_percentage"] == 100.0
    assert ac["D11_safe_teardown"]["status"] == "completed"
    assert ac["D11_safe_teardown"]["db_ownership_verified"] is True
    assert ac["D11_safe_teardown"]["redis_ownership_verified"] is True
