import concurrent.futures
import json
import uuid
from pathlib import Path
from typing import Any

import pytest
import redis
from sqlalchemy import func, select

from taskforge.core.state_machine import TaskPriority, TaskStatus
from taskforge.database.models import OutboxEvent, TaskResult
from taskforge.database.repositories.task_repository import TaskRepository
from taskforge.queue.broker import RedisBroker
from taskforge.queue.outbox import publish_pending_outbox_events
from taskforge.queue.redis_client import create_redis_client
from taskforge.workers.execution_engine import execute_task

TEST_KEY_PREFIX = "taskforge:g82:test:"
EVIDENCE_DIR = Path("benchmarks/scenarios/g82")


@pytest.fixture(scope="module")
def redis_conn():
    client = create_redis_client()
    try:
        client.ping()
        yield client
    finally:
        client.close()


@pytest.fixture
def test_queue(redis_conn: redis.Redis):
    queue_name = f"{TEST_KEY_PREFIX}{uuid.uuid4().hex}:tasks"
    yield queue_name
    assert queue_name.startswith(TEST_KEY_PREFIX), f"Refusing to delete non-test key: {queue_name}"
    redis_conn.delete(queue_name)


def save_test_evidence(test_name: str, payload: dict[str, Any]) -> None:
    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    evidence_path = EVIDENCE_DIR / f"{test_name}.json"
    with open(evidence_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


# ============================================================================
# Gate 8.2.4: Outbox Delivery & Duplicate Execution Safety
# ============================================================================
def test_outbox_crash_and_duplicate_delivery(redis_conn, test_queue, db_session, monkeypatch):
    """Acceptance Criterion G8.2.4:

    Simulates a publisher crash immediately after broker.enqueue() but before db.commit().
    Verifies that:
      1. Message is enqueued to Redis.
      2. PostgreSQL OutboxEvent remains PENDING.
      3. Next publisher sweep re-publishes the event (at-least-once guarantee).
      4. Downstream worker execution handles duplicate delivery safely.
    """
    repo = TaskRepository(db_session)
    broker = RedisBroker(redis_conn, queue_name=test_queue)

    # Ensure clean outbox state for isolated publisher test
    db_session.query(OutboxEvent).delete()
    db_session.commit()

    task = repo.create_task(
        task_name="Outbox Crash Simulation Task",
        task_type="json_transform",
        payload={"records": [1, 2, 3]},
        priority=TaskPriority.HIGH.value,
    )
    task_id = task.id

    outbox_entry = db_session.scalar(select(OutboxEvent).where(OutboxEvent.aggregate_id == task_id))
    assert outbox_entry is not None
    assert outbox_entry.status == "PENDING"

    # Crash injection: broker.enqueue() succeeds, but db.commit() raises RuntimeError
    original_commit = db_session.commit

    def crash_after_enqueue():
        raise RuntimeError("simulated_outbox_crash_before_commit")

    monkeypatch.setattr(db_session, "commit", crash_after_enqueue)

    with pytest.raises(RuntimeError, match="simulated_outbox_crash_before_commit"):
        publish_pending_outbox_events(db_session, broker)

    monkeypatch.setattr(db_session, "commit", original_commit)
    db_session.rollback()

    # 1. Message was pushed to Redis
    assert broker.get_queue_depth() == 1

    # 2. Outbox record remains PENDING in PostgreSQL
    db_session.expire_all()
    outbox_after_crash = db_session.scalar(
        select(OutboxEvent).where(OutboxEvent.aggregate_id == task_id)
    )
    assert outbox_after_crash.status == "PENDING"

    # 3. Publisher recovery cycle runs
    published_count = publish_pending_outbox_events(db_session, broker)
    assert published_count == 1

    db_session.expire_all()
    outbox_recovered = db_session.scalar(
        select(OutboxEvent).where(OutboxEvent.aggregate_id == task_id)
    )
    assert outbox_recovered.status == "PUBLISHED"

    # 4. Sequential duplicate execution: Worker executes first delivery
    assert execute_task(task_id, "worker-1") is True

    db_session.expire_all()
    task_done = repo.get_by_id(task_id)
    assert task_done.status == TaskStatus.SUCCESS.value
    assert len(task_done.attempts) == 1

    # Worker executes duplicate second delivery
    assert execute_task(task_id, "worker-2") is True  # Idempotent terminal skip

    db_session.expire_all()
    task_final = repo.get_by_id(task_id)
    assert task_final.status == TaskStatus.SUCCESS.value
    assert len(task_final.attempts) == 1  # No duplicate attempt created
    results_count = db_session.scalar(
        select(func.count(TaskResult.id)).where(TaskResult.task_id == task_id)
    )
    assert results_count == 1

    evidence = {
        "gate": "8.2.4",
        "description": "Outbox at-least-once delivery with duplicate delivery safety",
        "task_id": task_id,
        "outbox_status_before_recovery": "PENDING",
        "outbox_status_after_recovery": "PUBLISHED",
        "first_delivery_result": "SUCCESS",
        "duplicate_delivery_result": "SUCCESS (idempotent ignore)",
        "total_task_attempts": len(task_final.attempts),
        "total_task_results": results_count,
        "duplicate_side_effects": 0,
    }
    save_test_evidence("outbox_delivery", evidence)


def test_concurrent_duplicate_delivery_side_effect_protection(db_session):
    """Acceptance Criterion G8.2.4 — Concurrent Duplicate Protection:

    When duplicate messages arrive concurrently at two workers BEFORE either has
    completed the task, lease acquisition must ensure that ONLY ONE worker executes
    the handler. The second worker must fail to acquire the lease and exit without
    causing duplicate handler side effects.
    """
    repo = TaskRepository(db_session)
    task = repo.create_task(
        task_name="Concurrent Duplicate Protection Task",
        task_type="json_transform",
        payload={"records": [10, 20]},
    )
    task_id = task.id

    worker_a = "worker-concurrent-A"
    worker_b = "worker-concurrent-B"

    # Run execute_task concurrently on two worker threads
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        f_a = executor.submit(execute_task, task_id, worker_a)
        f_b = executor.submit(execute_task, task_id, worker_b)
        _ = [f_a.result(), f_b.result()]

    # Exactly one worker succeeded in executing the task
    # The other worker failed to acquire lease and returned False (or completed if serialized)
    db_session.expire_all()
    final_task = repo.get_by_id(task_id)
    assert final_task.status == TaskStatus.SUCCESS.value

    # Exactly 1 result was persisted
    results_count = db_session.scalar(
        select(func.count(TaskResult.id)).where(TaskResult.task_id == task_id)
    )
    assert results_count == 1

    # Exactly 1 successful attempt exists
    successful_attempts = [a for a in final_task.attempts if a.status == "SUCCESS"]
    assert len(successful_attempts) == 1
