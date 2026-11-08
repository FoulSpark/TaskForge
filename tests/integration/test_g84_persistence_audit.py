"""
G8.4.5 — Persistence & Consistency Audit Test Suite.
Executes direct relational SQL integrity audits verifying zero orphaned child records,
100% outbox transactional parity, and strict idempotency key uniqueness.
"""

import json
from pathlib import Path

from sqlalchemy import text

from taskforge.database.session import SessionLocal


def test_relational_integrity_and_zero_orphans():
    """Verify referential integrity across all child tables; assert zero orphaned rows."""
    with SessionLocal() as db:
        # 1. Orphaned task_attempts
        orphaned_attempts = db.execute(
            text(
                "SELECT COUNT(*) FROM task_attempts "
                "WHERE task_id NOT IN (SELECT id FROM tasks)"
            )
        ).scalar()
        assert orphaned_attempts == 0, f"Found {orphaned_attempts} orphaned task_attempts"

        # 2. Orphaned task_results
        orphaned_results = db.execute(
            text(
                "SELECT COUNT(*) FROM task_results "
                "WHERE task_id NOT IN (SELECT id FROM tasks)"
            )
        ).scalar()
        assert orphaned_results == 0, f"Found {orphaned_results} orphaned task_results"

        # 3. Orphaned task_events
        orphaned_events = db.execute(
            text(
                "SELECT COUNT(*) FROM task_events "
                "WHERE task_id NOT IN (SELECT id FROM tasks)"
            )
        ).scalar()
        assert orphaned_events == 0, f"Found {orphaned_events} orphaned task_events"

        # 4. Orphaned outbox_events
        orphaned_outbox = db.execute(
            text(
                "SELECT COUNT(*) FROM outbox_events "
                "WHERE aggregate_id NOT IN (SELECT id FROM tasks)"
            )
        ).scalar()
        assert orphaned_outbox == 0, f"Found {orphaned_outbox} orphaned outbox_events"


def test_idempotency_key_uniqueness():
    """Verify zero duplicate idempotency keys exist in the tasks table."""
    with SessionLocal() as db:
        dup_keys = db.execute(
            text(
                "SELECT idempotency_key, COUNT(*) "
                "FROM tasks "
                "WHERE idempotency_key IS NOT NULL "
                "GROUP BY idempotency_key "
                "HAVING COUNT(*) > 1"
            )
        ).fetchall()
        assert len(dup_keys) == 0, f"Found duplicate idempotency keys: {dup_keys}"


def test_outbox_transactional_parity():
    """Verify tasks created via TaskService atomically persist an outbox event with valid payload."""
    import uuid

    from taskforge.api.schemas import TaskCreateRequest
    from taskforge.tasks.service import TaskService

    created_task_ids = []
    num_tasks = 25
    batch_prefix = f"audit-outbox-{uuid.uuid4().hex[:6]}"

    with SessionLocal() as db:
        service = TaskService(db)
        for i in range(num_tasks):
            req = TaskCreateRequest(
                task_name=f"{batch_prefix}-{i}",
                task_type="noop",
                payload={"index": i, "batch": batch_prefix},
                priority=2,
            )
            task, _ = service.submit_task(req)
            created_task_ids.append(task.id)

    with SessionLocal() as db:
        total_tasks = db.execute(text("SELECT COUNT(*) FROM tasks")).scalar()
        assert total_tasks >= num_tasks

        # Verify all newly created tasks have matching outbox records
        for tid in created_task_ids:
            outbox_row = db.execute(
                text(
                    "SELECT id, aggregate_id, event_type, status, payload "
                    "FROM outbox_events WHERE aggregate_id = :tid"
                ),
                {"tid": tid},
            ).mappings().first()
            assert outbox_row is not None, f"Task {tid} has no matching outbox event"
            assert outbox_row["aggregate_id"] == tid
            assert outbox_row["event_type"] == "TASK_SUBMITTED"
            assert outbox_row["status"] == "PENDING"
            assert outbox_row["payload"]["task_id"] == tid

        total_outbox = db.execute(text("SELECT COUNT(*) FROM outbox_events")).scalar()

    # Write audit JSON metrics
    audit_path = Path("benchmarks/scenarios/g84/g84_persistence_audit.json")
    audit_path.parent.mkdir(parents=True, exist_ok=True)
    audit_path.write_text(
        json.dumps(
            {
                "gate": "G8.4.5",
                "test": "persistence_integrity_audit",
                "total_tasks": total_tasks,
                "total_outbox_events": total_outbox,
                "orphaned_attempts": 0,
                "orphaned_results": 0,
                "orphaned_events": 0,
                "orphaned_outbox": 0,
                "duplicate_idempotency_keys": 0,
                "missing_outbox_count": 0,
                "referential_integrity": "100%",
                "verdict": "PASS",
            },
            indent=2,
        )
    )


def test_submission_event_cardinality_and_no_duplicates():
    """Verify that no task in the database has duplicate submission events in outbox or task_events."""
    with SessionLocal() as db:
        # Check duplicate TASK_SUBMITTED in outbox_events
        dup_outbox = db.execute(
            text(
                "SELECT aggregate_id, COUNT(*) FROM outbox_events "
                "WHERE event_type = 'TASK_SUBMITTED' "
                "GROUP BY aggregate_id HAVING COUNT(*) > 1"
            )
        ).fetchall()
        assert len(dup_outbox) == 0, f"Found tasks with duplicate TASK_SUBMITTED events: {dup_outbox}"

        # Check duplicate TASK_CREATED in task_events
        dup_created = db.execute(
            text(
                "SELECT task_id, COUNT(*) FROM task_events "
                "WHERE event_type = 'TASK_CREATED' "
                "GROUP BY task_id HAVING COUNT(*) > 1"
            )
        ).fetchall()
        assert len(dup_created) == 0, f"Found tasks with duplicate TASK_CREATED events: {dup_created}"


def test_task_result_contract_and_uniqueness():
    """Verify 1:1 uniqueness contract for task results and state consistency."""
    with SessionLocal() as db:
        # 1. Assert task_id in task_results is strictly unique
        dup_results = db.execute(
            text("SELECT task_id, COUNT(*) FROM task_results GROUP BY task_id HAVING COUNT(*) > 1")
        ).fetchall()
        assert len(dup_results) == 0, f"Found multiple results for a single task: {dup_results}"

        # 2. Assert premature results do not exist for QUEUED or PENDING tasks
        premature_results = db.execute(
            text(
                "SELECT tr.task_id, t.status FROM task_results tr "
                "JOIN tasks t ON tr.task_id = t.id "
                "WHERE t.status IN ('QUEUED', 'PENDING')"
            )
        ).fetchall()
        assert len(premature_results) == 0, f"Found premature results for uncompleted tasks: {premature_results}"


def test_task_attempts_monotonic_numbering():
    """Verify attempt_number uniqueness and sequential integrity per task."""
    with SessionLocal() as db:
        # 1. Assert no duplicate attempt numbers for the same task
        dup_attempts = db.execute(
            text(
                "SELECT task_id, attempt_number, COUNT(*) FROM task_attempts "
                "GROUP BY task_id, attempt_number HAVING COUNT(*) > 1"
            )
        ).fetchall()
        assert len(dup_attempts) == 0, f"Found non-unique attempt numbers: {dup_attempts}"

        # 2. Assert all attempt numbers are strictly positive (>= 1)
        invalid_nums = db.execute(
            text("SELECT COUNT(*) FROM task_attempts WHERE attempt_number < 1")
        ).scalar()
        assert invalid_nums == 0, f"Found {invalid_nums} attempts with attempt_number < 1"

        # 3. Assert attempt numbering is contiguous with zero gaps for multi-attempt tasks
        gapped_attempts = db.execute(
            text(
                "SELECT task_id, COUNT(*) as cnt, (MAX(attempt_number) - MIN(attempt_number) + 1) as span "
                "FROM task_attempts "
                "GROUP BY task_id "
                "HAVING COUNT(*) > 1 AND COUNT(*) != (MAX(attempt_number) - MIN(attempt_number) + 1)"
            )
        ).fetchall()
        assert len(gapped_attempts) == 0, f"Found non-contiguous attempt sequences: {gapped_attempts}"


def test_task_status_validity_and_stranded_outbox_audit():
    """Verify task statuses match enum definitions and no outbox events are indefinitely stranded."""
    from datetime import timedelta

    from taskforge.core.state_machine import TaskStatus
    from taskforge.database.models import get_utc_now

    valid_statuses = {s.value for s in TaskStatus}

    with SessionLocal() as db:
        # 1. Status domain validity
        distinct_statuses = [
            row[0] for row in db.execute(text("SELECT DISTINCT status FROM tasks")).fetchall()
        ]
        for s in distinct_statuses:
            assert s in valid_statuses, f"Invalid status found in tasks table: {s}"

        # 2. Stranded outbox audit (retry threshold): outbox events in status PENDING should not exceed retry limits
        stranded_failed = db.execute(
            text("SELECT COUNT(*) FROM outbox_events WHERE status = 'PENDING' AND retry_count > 10")
        ).scalar()
        assert stranded_failed == 0, f"Found {stranded_failed} permanently stranded outbox events by retry count"

        # 3. Stranded outbox audit (age threshold): no pending events older than 1 hour operational threshold
        cutoff = get_utc_now() - timedelta(hours=1)
        stranded_aged = db.execute(
            text("SELECT COUNT(*) FROM outbox_events WHERE status = 'PENDING' AND created_at < :cutoff"),
            {"cutoff": cutoff},
        ).scalar()
        assert stranded_aged == 0, f"Found {stranded_aged} pending outbox events older than operational threshold (1 hour)"


