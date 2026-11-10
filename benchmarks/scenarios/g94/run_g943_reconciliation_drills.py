"""Gate G9.4.3: Reconciliation Drills for Heartbeat Renewals and Path-Level Fencing.

Specifically executes and reports:
1. R1: Scraped exposition inspection confirming taskforge_db_pool_checkout_timeouts_total{pool_name="primary"}
       and exact Option A cardinality (25 net-new G9.4, 28 combined).
2. R2: Focused Heartbeat Renewal Drill (Worker A stale renewal rejected, Worker B valid renewal succeeds).
3. R3: Path-Level Stale Token Rejection Drill across all 5 guarded operations.
"""

from datetime import UTC, datetime
import json
import os
from pathlib import Path
import sys
import time
from typing import Any
import uuid

from sqlalchemy import select, text

REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

BENCHMARK_DB_NAME = "taskforge_g94_benchmark"
BENCHMARK_DB_URL = f"postgresql+psycopg://taskforge:taskforge_dev@localhost:5432/{BENCHMARK_DB_NAME}"

os.environ["DATABASE_URL"] = BENCHMARK_DB_URL
os.environ["TASKFORGE_TESTING"] = "1"
os.environ["APP_ENV"] = "development"

from taskforge.core.state_machine import TaskPriority, TaskStatus
from taskforge.database.models import Task, TaskAttempt, TaskResult
from taskforge.database.repositories.task_repository import TaskRepository
from taskforge.database.session import SessionLocal
from taskforge.monitoring.metrics import (
    DB_POOL_CHECKOUT_TIMEOUTS_TOTAL,
    FENCING_REJECTIONS_TOTAL,
    LEASE_RENEWALS_TOTAL,
    get_metrics_content,
)


def run_r1_exposition_audit() -> dict[str, Any]:
    print("\n--- [R1] Scraped Prometheus Exposition Audit ---")
    raw_bytes = get_metrics_content()
    raw_text = raw_bytes.decode("utf-8")
    lines = [l.strip() for l in raw_text.splitlines() if l.strip() and not l.startswith("#")]

    pool_lines = [l for l in lines if "taskforge_db_pool_" in l]
    lease_lines = [l for l in lines if "taskforge_lease_renewals_" in l or "taskforge_fencing_rejections_" in l]
    g94_additions = [l for l in pool_lines if not l.startswith("taskforge_db_pool_connections{")] + lease_lines

    # Verify timeout metric
    timeout_lines = [l for l in pool_lines if "checkout_timeouts" in l]
    has_queue_full = any("queue_full" in l for l in lines)

    print(f"Checkout timeout metric lines in exposition:\n  " + "\n  ".join(timeout_lines))
    print(f"Any 'queue_full' string in exposition: {has_queue_full}")
    print(f"Total G9.2 Legacy Pool Series: {len(pool_lines) - (len(g94_additions) - len(lease_lines))} (Expected: 3)")
    print(f"Total G9.4.2 Net-New Pool Series: {len(g94_additions) - len(lease_lines)} (Expected: 19)")
    print(f"Total G9.4.3 Net-New Lease Series: {len(lease_lines)} (Expected: 6)")
    print(f"Total Net-New G9.4 Additions: {len(g94_additions)} (Ceiling: 25)")
    print(f"Total Combined Runtime Series: {len(pool_lines) + len(lease_lines)} (Ceiling: 28)")

    return {
        "timeout_metric_exact_name": "taskforge_db_pool_checkout_timeouts_total",
        "timeout_series_in_exposition": timeout_lines,
        "contains_queue_full_label": has_queue_full,
        "legacy_g92_series": 3,
        "g942_net_new_series": len(g94_additions) - len(lease_lines),
        "g943_net_new_series": len(lease_lines),
        "total_g94_additions": len(g94_additions),
        "total_combined_exposition": len(pool_lines) + len(lease_lines),
        "cardinality_compliant": len(g94_additions) == 25 and (len(pool_lines) + len(lease_lines)) == 28,
    }


def run_r2_heartbeat_drill() -> dict[str, Any]:
    print("\n--- [R2] Focused Stale Heartbeat Renewal Drill ---")
    with SessionLocal() as db:
        repo = TaskRepository(db)
        tid = f"g943-hb-drill-{uuid.uuid4().hex[:8]}"
        tok_a = f"lease-A-{uuid.uuid4().hex[:8]}"
        tok_b = f"lease-B-{uuid.uuid4().hex[:8]}"

        # Step 0: Create task
        task = Task(
            id=tid,
            task_name="Heartbeat Renewal Drill Task",
            task_type="noop",
            status=TaskStatus.QUEUED.value,
            priority=TaskPriority.HIGH.value,
            payload={},
        )
        db.add(task)
        db.commit()

        # Step 1: Worker A acquires initial short lease (1 second)
        acquired_a = repo.acquire_lease(tid, worker_id="worker-A", lease_duration_seconds=1, lease_token=tok_a)
        repo.update_status(tid, TaskStatus.RUNNING, {"worker_id": "worker-A"}, lease_token=tok_a)
        assert acquired_a is True

        failed_before = LEASE_RENEWALS_TOTAL.labels(status="failed")._value.get()
        success_before = LEASE_RENEWALS_TOTAL.labels(status="success")._value.get()
        fencing_before = FENCING_REJECTIONS_TOTAL._value.get()

        print(f"Step 1: Worker A acquired lease (token: {tok_a[:12]}..., duration: 1s)")
        print(f"  Initial Metrics: failed_renewals={failed_before}, success_renewals={success_before}, fencing_rejections={fencing_before}")

        # Step 2: Worker A pauses; lease expires (1.1s); Worker B claims task
        print("Step 2: Simulating Worker A pause (sleep 1.1s)...")
        time.sleep(1.1)
        recovered = repo.recover_expired_leases()
        print(f"  Lease reaper recovered expired tasks: {[r.task_id for r in recovered]}")

        acquired_b = repo.acquire_lease(tid, worker_id="worker-B", lease_duration_seconds=30, lease_token=tok_b)
        repo.update_status(tid, TaskStatus.RUNNING, {"worker_id": "worker-B"}, lease_token=tok_b)
        assert acquired_b is True
        print(f"  Worker B acquired lease (token: {tok_b[:12]}..., duration: 30s)")

        # Verify DB state: Worker B is authoritative
        db.expire_all()
        t_active = repo.get_by_id(tid)
        assert t_active.lease_token == tok_b
        print(f"  Authoritative DB check: current lease_token={t_active.lease_token[:12]}... (Worker B)")

        # Step 3: Worker A unpauses and attempts renewal with stale tok_a
        print("Step 3: Worker A unpauses and attempts heartbeat renewal with stale tok_a...")
        renew_a_result = repo.renew_lease(tid, lease_token=tok_a)
        assert renew_a_result is False

        failed_after_stale = LEASE_RENEWALS_TOTAL.labels(status="failed")._value.get()
        success_after_stale = LEASE_RENEWALS_TOTAL.labels(status="success")._value.get()
        fencing_after_stale = FENCING_REJECTIONS_TOTAL._value.get()

        print(f"  Renewal result: {renew_a_result} (Rejected)")
        print(f"  Metrics after stale renewal: failed_renewals={failed_after_stale} (+{failed_after_stale - failed_before}), success_renewals={success_after_stale}, fencing_rejections={fencing_after_stale} (+{fencing_after_stale - fencing_before})")
        assert failed_after_stale == failed_before + 1.0
        assert fencing_after_stale == fencing_before  # Heartbeat failure is NOT a terminal fencing rejection

        # Verify DB state: Worker B lease token unharmed
        db.expire_all()
        t_active = repo.get_by_id(tid)
        assert t_active.lease_token == tok_b

        # Step 4: Worker B issues valid heartbeat renewal with tok_b
        print("Step 4: Worker B issues valid heartbeat renewal with tok_b...")
        renew_b_result = repo.renew_lease(tid, lease_token=tok_b)
        assert renew_b_result is True

        failed_final = LEASE_RENEWALS_TOTAL.labels(status="failed")._value.get()
        success_final = LEASE_RENEWALS_TOTAL.labels(status="success")._value.get()
        fencing_final = FENCING_REJECTIONS_TOTAL._value.get()

        print(f"  Renewal result: {renew_b_result} (Succeeded)")
        print(f"  Metrics after valid renewal: failed_renewals={failed_final}, success_renewals={success_final} (+{success_final - success_before}), fencing_rejections={fencing_final}")
        assert success_final == success_before + 1.0

        return {
            "task_id": tid,
            "worker_a_token": tok_a,
            "worker_b_token": tok_b,
            "stale_renewal_rejected": True,
            "valid_renewal_accepted": True,
            "metrics": {
                "failed_renewals_before": failed_before,
                "failed_renewals_after": failed_after_stale,
                "failed_renewals_delta": failed_after_stale - failed_before,
                "success_renewals_before": success_before,
                "success_renewals_after": success_final,
                "success_renewals_delta": success_final - success_before,
                "fencing_rejections_delta": fencing_final - fencing_before,
            },
            "authoritative_owner_unaffected": True,
        }


def run_r3_path_level_drill() -> dict[str, Any]:
    print("\n--- [R3] Path-Level Stale Token Rejection Drill ---")
    results = []

    operations = [
        ("Terminal success (complete_task_success)", lambda r, tid, tok: r.complete_task_success(tid, lease_token=tok, return_value={"ok": True})),
        ("Terminal failure (complete_task_failure)", lambda r, tid, tok: r.complete_task_failure(tid, lease_token=tok, error_message="stale_err")),
        ("Result persistence (record_result)", lambda r, tid, tok: r.record_result(tid, return_value={"stale": True}, lease_token=tok)),
        ("Execution-attempt persistence (record_attempt)", lambda r, tid, tok: r.record_attempt(tid, worker_id="worker-A", status="RUNNING", lease_token=tok)),
        ("Other guarded state updates (update_status)", lambda r, tid, tok: r.update_status(tid, TaskStatus.RUNNING, {"worker": "A"}, lease_token=tok)),
    ]

    with SessionLocal() as db:
        repo = TaskRepository(db)

        for op_desc, op_func in operations:
            tid = f"g943-pdrill-{uuid.uuid4().hex[:8]}"
            tok_a = f"lease-A-{uuid.uuid4().hex[:8]}"
            tok_b = f"lease-B-{uuid.uuid4().hex[:8]}"

            # Create task
            task = Task(
                id=tid,
                task_name=f"Path Drill: {op_desc}",
                task_type="noop",
                status=TaskStatus.QUEUED.value,
                priority=TaskPriority.HIGH.value,
                payload={},
            )
            db.add(task)
            db.commit()

            # Worker A acquires lease
            repo.acquire_lease(tid, worker_id="worker-A", lease_duration_seconds=1, lease_token=tok_a)
            repo.update_status(tid, TaskStatus.RUNNING, {"worker_id": "worker-A"}, lease_token=tok_a)

            # Lease expires and Worker B claims
            time.sleep(1.1)
            repo.recover_expired_leases()
            repo.acquire_lease(tid, worker_id="worker-B", lease_duration_seconds=30, lease_token=tok_b)
            repo.update_status(tid, TaskStatus.RUNNING, {"worker_id": "worker-B"}, lease_token=tok_b)

            # Stale attempt by Worker A
            before_rej = FENCING_REJECTIONS_TOTAL._value.get()
            stale_attempt_made = 1
            stale_rejected = 0
            stale_accepted = 0

            try:
                op_func(repo, tid, tok_a)
                stale_accepted += 1
            except PermissionError:
                stale_rejected += 1

            after_rej = FENCING_REJECTIONS_TOTAL._value.get()
            rej_delta = after_rej - before_rej

            print(f"Operation: {op_desc}")
            print(f"  Stale Attempt: {stale_attempt_made}, Rejected: {stale_rejected}, Accepted: {stale_accepted}, Metric Delta: +{rej_delta}")

            assert stale_attempt_made == 1
            assert stale_rejected == 1
            assert stale_accepted == 0
            assert rej_delta == 1.0

            results.append({
                "operation": op_desc,
                "stale_attempts": stale_attempt_made,
                "rejections": stale_rejected,
                "accepted_stale_commits": stale_accepted,
                "fencing_rejections_metric_delta": rej_delta,
                "status": "VERIFIED_FENCED",
            })

    return {
        "path_evaluations": results,
        "total_stale_attempts": sum(r["stale_attempts"] for r in results),
        "total_rejections": sum(r["rejections"] for r in results),
        "total_accepted_stale_commits": sum(r["accepted_stale_commits"] for r in results),
    }


def main():
    print("=" * 70)
    print("GATE G9.4.3: RECONCILIATION DRILLS (R1, R2, R3)")
    print("=" * 70)

    r1_data = run_r1_exposition_audit()
    r2_data = run_r2_heartbeat_drill()
    r3_data = run_r3_path_level_drill()

    report = {
        "timestamp_utc": datetime.now(UTC).isoformat(),
        "milestone": "G9.4.3_reconciliation",
        "r1_exposition_audit": r1_data,
        "r2_heartbeat_drill": r2_data,
        "r3_path_level_drill": r3_data,
    }

    report_path = REPO_ROOT / "benchmarks" / "scenarios" / "g94" / "g943_reconciliation_report.json"
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"\n[G9.4.3] Reconciliation report successfully generated at:\n  {report_path}")


if __name__ == "__main__":
    main()
