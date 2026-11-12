"""Integration tests for G8.1: Persistence Integrity and Transaction Atomicity."""

import uuid

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

from taskforge.database.models import OutboxEvent, Task, TaskAttempt, TaskEvent
from taskforge.database.repositories.task_repository import TaskRepository
from taskforge.database.session import SessionLocal
from taskforge.main import app


def test_idempotency_key_returns_existing_task(db_session):
    """Regression test: duplicate idempotency keys return existing task with single DB row."""
    repo = TaskRepository(db_session)
    idemp_key = f"g81-idemp-{uuid.uuid4().hex[:8]}"

    first = repo.create_task(
        task_name="Idempotency Test",
        task_type="json_transform",
        payload={"value": 1},
        idempotency_key=idemp_key,
    )
    second = repo.create_task(
        task_name="Idempotency Test",
        task_type="json_transform",
        payload={"value": 1},
        idempotency_key=idemp_key,
    )

    assert first.id == second.id
    count = db_session.scalar(select(func.count(Task.id)).where(Task.idempotency_key == idemp_key))
    assert count == 1

    # Cleanup test task to prevent leaving an unmanaged PENDING outbox record
    from sqlalchemy import text

    db_session.execute(text("DELETE FROM outbox_events WHERE aggregate_id = :id"), {"id": first.id})
    db_session.execute(text("DELETE FROM task_events WHERE task_id = :id"), {"id": first.id})
    db_session.execute(text("DELETE FROM tasks WHERE id = :id"), {"id": first.id})
    db_session.commit()


def test_task_creation_atomic_rollback(db_session, monkeypatch):
    """
    Acceptance Criterion G8.1.3:
    Simulates a failure after SQLAlchemy flushes the Task, initial TaskEvent,
    and OutboxEvent, but before commit completes.
    Verifies that the entire transaction is rolled back and no partial records exist.
    """
    repo = TaskRepository(db_session)
    original_commit = db_session.commit
    attempted_task_id = None

    def fail_after_flush():
        nonlocal attempted_task_id
        # At this point, task has been flushed and assigned an ID
        for obj in list(db_session.identity_map.values()) + list(db_session.new):
            if isinstance(obj, Task):
                attempted_task_id = obj.id
        db_session.flush()
        raise RuntimeError("injected_commit_failure")

    monkeypatch.setattr(db_session, "commit", fail_after_flush)

    with pytest.raises(RuntimeError, match="injected_commit_failure"):
        repo.create_task(
            task_name="Rollback Verification Task",
            task_type="json_transform",
            payload={"rollback_test": True},
        )

    # Repository should have executed rollback
    monkeypatch.setattr(db_session, "commit", original_commit)

    assert attempted_task_id is not None, "Failed to capture flushed task ID"

    # Verify atomicity using an independent database session
    with SessionLocal() as separate_db:
        task_exists = separate_db.scalar(
            select(func.count(Task.id)).where(Task.id == attempted_task_id)
        )
        events_count = separate_db.scalar(
            select(func.count(TaskEvent.id)).where(TaskEvent.task_id == attempted_task_id)
        )
        outbox_count = separate_db.scalar(
            select(func.count(OutboxEvent.id)).where(OutboxEvent.aggregate_id == attempted_task_id)
        )

        assert task_exists == 0, "Partial task record survived rollback!"
        assert events_count == 0, "Partial task_events records survived rollback!"
        assert outbox_count == 0, "Partial outbox_events record survived rollback!"


def test_task_foreign_key_integrity(db_session):
    """Acceptance Criterion G8.1.4: Attempts referencing non-existent task IDs violate foreign key constraints."""
    bogus_task_id = str(uuid.uuid4())
    attempt = TaskAttempt(
        task_id=bogus_task_id,
        attempt_number=1,
        worker_id="test-worker",
        status="RUNNING",
    )
    db_session.add(attempt)
    with pytest.raises(IntegrityError):
        db_session.commit()
    db_session.rollback()


def test_database_readiness_failure_returns_503(monkeypatch):
    """Acceptance Criterion G8.1.6: When PostgreSQL is down, /health/ready returns HTTP 503."""
    monkeypatch.setattr("taskforge.main.check_database", lambda: False)

    with TestClient(app) as client:
        resp = client.get("/health/ready")
        assert resp.status_code == 503
        data = resp.json()
        assert data["status"] == "not_ready"
        assert data["checks"]["database"] == "unavailable"
