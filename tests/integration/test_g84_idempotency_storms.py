"""
G8.4.3 — Idempotency Storms & Collision Stress Test Suite.
Verifies strict serializability, zero duplicate task creation, zero duplicate outbox events,
and conflict detection under high-concurrency multi-threaded thundering herd conditions.
"""

import concurrent.futures
import json
import time
import uuid
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select

from taskforge.api.errors import IdempotencyConflictError
from taskforge.api.schemas import TaskCreateRequest
from taskforge.database.models import OutboxEvent, Task
from taskforge.database.session import SessionLocal
from taskforge.main import app
from taskforge.tasks.service import TaskService


@pytest.fixture
def client():
    with TestClient(app) as test_client:
        yield test_client


def test_thundering_herd_single_key_100_producers():
    """100 concurrent workers simultaneously submit the exact same idempotency key.

    Invariants:
    - 0 unhandled exceptions or deadlocks.
    - Exactly 1 worker gets is_created=True; 99 get is_created=False.
    - All 100 workers receive the identical task ID.
    - Exactly 1 row in PostgreSQL tasks table.
    - Exactly 1 row in PostgreSQL outbox_events table.
    """
    key = f"storm-100-{uuid.uuid4()}"
    req = TaskCreateRequest(
        task_name="G8.4 Storm 100",
        task_type="noop",
        payload={"concurrency": 100, "drill": "thundering_herd"},
        idempotency_key=key,
        priority=3,
    )

    num_workers = 100
    results = []

    def _submit_worker():
        with SessionLocal() as db:
            service = TaskService(db)
            task, is_created = service.submit_task(req)
            return task.id, is_created

    t0 = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(_submit_worker) for _ in range(num_workers)]
        for f in concurrent.futures.as_completed(futures):
            results.append(f.result())
    _elapsed = time.perf_counter() - t0

    task_ids = [r[0] for r in results]
    created_flags = [r[1] for r in results]

    assert len(set(task_ids)) == 1, f"Expected 1 unique task ID, got {len(set(task_ids))}"
    canonical_id = task_ids[0]

    created_count = sum(1 for c in created_flags if c is True)
    dedup_count = sum(1 for c in created_flags if c is False)

    assert created_count == 1, f"Expected exactly 1 created, got {created_count}"
    assert dedup_count == 99, f"Expected 99 deduplications, got {dedup_count}"

    # Verify database persistence directly
    with SessionLocal() as db:
        tasks = list(db.scalars(select(Task).where(Task.idempotency_key == key)).all())
        assert len(tasks) == 1
        assert tasks[0].id == canonical_id

        outbox_events = list(
            db.scalars(select(OutboxEvent).where(OutboxEvent.aggregate_id == canonical_id)).all()
        )
        assert len(outbox_events) == 1
        assert outbox_events[0].event_type == "TASK_SUBMITTED"


def test_thundering_herd_single_key_500_producers():
    """500 concurrent workers simultaneously submit the exact same idempotency key.

    Invariants:
    - 0 unhandled exceptions or deadlocks.
    - Exactly 1 task row created in PostgreSQL.
    - Exactly 1 outbox event row created.
    - All 500 workers return the identical task ID.
    """
    key = f"storm-500-{uuid.uuid4()}"
    req = TaskCreateRequest(
        task_name="G8.4 Storm 500",
        task_type="noop",
        payload={"concurrency": 500, "drill": "thundering_herd"},
        idempotency_key=key,
        priority=2,
    )

    num_workers = 500
    results = []

    def _submit_worker():
        with SessionLocal() as db:
            service = TaskService(db)
            task, is_created = service.submit_task(req)
            return task.id, is_created

    t0 = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(num_workers, 64)) as executor:
        futures = [executor.submit(_submit_worker) for _ in range(num_workers)]
        for f in concurrent.futures.as_completed(futures):
            results.append(f.result())
    _elapsed = time.perf_counter() - t0

    task_ids = [r[0] for r in results]
    created_flags = [r[1] for r in results]

    assert len(set(task_ids)) == 1, f"Expected 1 unique task ID, got {len(set(task_ids))}"
    canonical_id = task_ids[0]

    created_count = sum(1 for c in created_flags if c is True)
    dedup_count = sum(1 for c in created_flags if c is False)

    assert created_count == 1, f"Expected exactly 1 created, got {created_count}"
    assert dedup_count == 499, f"Expected 499 deduplications, got {dedup_count}"

    with SessionLocal() as db:
        tasks = list(db.scalars(select(Task).where(Task.idempotency_key == key)).all())
        assert len(tasks) == 1
        assert tasks[0].id == canonical_id

        outbox_events = list(
            db.scalars(select(OutboxEvent).where(OutboxEvent.aggregate_id == canonical_id)).all()
        )
        assert len(outbox_events) == 1
        assert outbox_events[0].event_type == "TASK_SUBMITTED"


def test_key_reuse_matrix_100_keys_10_repetitions():
    """100 unique idempotency keys, each repeated 10 times concurrently (1,000 total requests).

    Invariants:
    - Exactly 100 unique tasks created in PostgreSQL.
    - Exactly 100 outbox events created in PostgreSQL.
    - Exactly 100 created flags (is_created=True), exactly 900 deduplicated (is_created=False).
    - 0 orphaned or conflicting records.
    """
    num_keys = 100
    repetitions = 10
    total_requests = num_keys * repetitions

    keys = [f"matrix-key-{i}-{uuid.uuid4().hex[:6]}" for i in range(num_keys)]
    requests = []
    for key in keys:
        for rep in range(repetitions):
            requests.append(
                TaskCreateRequest(
                    task_name=f"Matrix Task {key}",
                    task_type="json_transform",
                    payload={"key": key, "rep": rep},
                    idempotency_key=key,
                    priority=2,
                )
            )

    # Note: to ensure identical payload for same key across repetitions:
    for req in requests:
        req.payload = {"key": req.idempotency_key, "data": "fixed_matrix_value"}

    results_by_key: dict[str, list[tuple[str, bool]]] = {k: [] for k in keys}

    def _submit_matrix_item(item_req: TaskCreateRequest):
        with SessionLocal() as db:
            service = TaskService(db)
            task, is_created = service.submit_task(item_req)
            return item_req.idempotency_key, task.id, is_created

    t0 = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        futures = [executor.submit(_submit_matrix_item, req) for req in requests]
        for f in concurrent.futures.as_completed(futures):
            k, tid, is_created = f.result()
            results_by_key[k].append((tid, is_created))
    elapsed = time.perf_counter() - t0

    total_created = 0
    total_dedup = 0
    unique_task_ids = set()

    for k, submissions in results_by_key.items():
        assert len(submissions) == repetitions
        tids = {s[0] for s in submissions}
        assert len(tids) == 1, f"Key {k} produced multiple task IDs: {tids}"
        unique_task_ids.add(list(tids)[0])

        created_in_key = sum(1 for s in submissions if s[1] is True)
        dedup_in_key = sum(1 for s in submissions if s[1] is False)
        assert created_in_key == 1, f"Key {k} had {created_in_key} creations instead of 1"
        assert dedup_in_key == (repetitions - 1)

        total_created += created_in_key
        total_dedup += dedup_in_key

    assert len(unique_task_ids) == num_keys
    assert total_created == num_keys
    assert total_dedup == (total_requests - num_keys)

    # Persistence verification
    with SessionLocal() as db:
        tasks = list(db.scalars(select(Task).where(Task.idempotency_key.in_(keys))).all())
        assert len(tasks) == num_keys

        outbox_events = list(
            db.scalars(select(OutboxEvent).where(OutboxEvent.aggregate_id.in_(unique_task_ids))).all()
        )
        assert len(outbox_events) == num_keys

    # Write benchmark metrics
    metrics_path = Path("benchmarks/scenarios/g84/g84_idempotency_storm_metrics.json")
    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    metrics_path.write_text(
        json.dumps(
            {
                "gate": "G8.4.3",
                "test": "test_key_reuse_matrix_100_keys_10_repetitions",
                "total_requests": total_requests,
                "unique_keys": num_keys,
                "unique_tasks_created": len(unique_task_ids),
                "total_deduplicated": total_dedup,
                "elapsed_seconds": round(elapsed, 3),
                "throughput_rps": round(total_requests / elapsed, 1),
                "duplicate_tasks_in_db": 0,
                "outbox_events_count": len(outbox_events),
                "verdict": "PASS",
            },
            indent=2,
        )
    )


def test_idempotency_conflict_rejection(client: TestClient):
    """Submitting an existing idempotency key with conflicting payload or type returns 409 Conflict.

    Invariants:
    - Raises IdempotencyConflictError in service layer.
    - Returns HTTP 409 Conflict with code 'IDEMPOTENCY_CONFLICT'.
    - Existing task in database remains unmodified.
    """
    key = f"conflict-{uuid.uuid4()}"

    # 1. Initial valid submission
    res1 = client.post(
        "/api/v1/tasks",
        json={
            "task_name": "Original Task",
            "task_type": "noop",
            "payload": {"version": 1, "config": "A"},
            "idempotency_key": key,
        },
    )
    assert res1.status_code == 201
    task_id = res1.json()["id"]

    # 2. Conflicting payload submission
    res_conflict_payload = client.post(
        "/api/v1/tasks",
        json={
            "task_name": "Original Task",
            "task_type": "noop",
            "payload": {"version": 2, "config": "B"},
            "idempotency_key": key,
        },
    )
    assert res_conflict_payload.status_code == 409
    err = res_conflict_payload.json()["error"]
    assert err["code"] == "IDEMPOTENCY_CONFLICT"
    assert key in err["message"]

    # 3. Conflicting task_type submission
    res_conflict_type = client.post(
        "/api/v1/tasks",
        json={
            "task_name": "Original Task",
            "task_type": "cpu_bound",
            "payload": {"version": 1, "config": "A"},
            "idempotency_key": key,
        },
    )
    assert res_conflict_type.status_code == 409
    assert res_conflict_type.json()["error"]["code"] == "IDEMPOTENCY_CONFLICT"

    # 4. Conflicting task_name submission
    res_conflict_name = client.post(
        "/api/v1/tasks",
        json={
            "task_name": "Different Task Name",
            "task_type": "noop",
            "payload": {"version": 1, "config": "A"},
            "idempotency_key": key,
        },
    )
    assert res_conflict_name.status_code == 409
    assert res_conflict_name.json()["error"]["code"] == "IDEMPOTENCY_CONFLICT"

    # 5. Direct service-level exception assertion
    with SessionLocal() as db:
        service = TaskService(db)
        with pytest.raises(IdempotencyConflictError) as exc_info:
            service.submit_task(
                TaskCreateRequest(
                    task_name="Service Conflict",
                    task_type="noop",
                    payload={"version": 99},
                    idempotency_key=key,
                )
            )
        assert exc_info.value.key == key

    # 6. Verify original task is untouched
    with SessionLocal() as db:
        original = TaskService(db).get_task(task_id)
        assert original is not None
        assert original.task_name == "Original Task"
        assert original.task_type == "noop"
        assert original.payload == {"version": 1, "config": "A"}


def test_concurrent_idempotency_race_with_conflicting_payloads():
    """Two concurrent threads simultaneously submit the exact same idempotency key with conflicting payloads.

    Invariants:
    - Exactly one thread succeeds in creating the task (returns is_created=True).
    - The losing thread hits the uniqueness collision, rolls back its transaction, retrieves canonical task,
      detects the payload conflict, and raises IdempotencyConflictError.
    - Exactly 1 task row in PostgreSQL.
    - Exactly 1 outbox_events row in PostgreSQL (the loser's rollback clears its outbox event).
    - Zero orphaned or corrupt records.
    """
    key = f"race-conflict-{uuid.uuid4()}"
    req_a = TaskCreateRequest(
        task_name="Race Task A",
        task_type="noop",
        payload={"winner": "A", "val": 100},
        idempotency_key=key,
        priority=2,
    )
    req_b = TaskCreateRequest(
        task_name="Race Task B",
        task_type="noop",
        payload={"winner": "B", "val": 200},
        idempotency_key=key,
        priority=2,
    )

    def _submit(worker_label: str, req: TaskCreateRequest):
        try:
            with SessionLocal() as db:
                service = TaskService(db)
                task, is_created = service.submit_task(req)
                return worker_label, "SUCCESS", task.id, is_created, None
        except IdempotencyConflictError as exc:
            return worker_label, "CONFLICT", None, False, str(exc)
        except Exception as exc:
            return worker_label, "ERROR", None, False, str(exc)

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        f_a = executor.submit(_submit, "A", req_a)
        f_b = executor.submit(_submit, "B", req_b)
        res_a = f_a.result()
        res_b = f_b.result()

    outcomes = [res_a, res_b]
    successes = [o for o in outcomes if o[1] == "SUCCESS"]
    conflicts = [o for o in outcomes if o[1] == "CONFLICT"]

    assert len(successes) == 1, f"Expected exactly 1 success, got {len(successes)}: {outcomes}"
    assert len(conflicts) == 1, f"Expected exactly 1 conflict, got {len(conflicts)}: {outcomes}"
    assert successes[0][3] is True, "The winning submission must report is_created=True"

    canonical_task_id = successes[0][2]

    # Verify database state: exactly 1 task and exactly 1 outbox event
    with SessionLocal() as db:
        tasks = list(db.scalars(select(Task).where(Task.idempotency_key == key)).all())
        assert len(tasks) == 1
        assert tasks[0].id == canonical_task_id

        outbox_events = list(
            db.scalars(select(OutboxEvent).where(OutboxEvent.aggregate_id == canonical_task_id)).all()
        )
        assert len(outbox_events) == 1
        assert outbox_events[0].event_type == "TASK_SUBMITTED"


def test_idempotency_conflict_rejection_on_scheduled_at(client: TestClient):
    """Submitting an existing idempotency key with conflicting scheduled_at returns 409 Conflict.
    Equivalent instants across different timezone offsets normalize cleanly to UTC and deduplicate (HTTP 200).
    Also asserts scheduled_at survives into both Task and OutboxEvent.payload.
    """
    from datetime import UTC, datetime, timedelta, timezone

    key = f"sched-conflict-{uuid.uuid4()}"
    t1_utc = datetime(2026, 11, 9, 10, 0, 0, tzinfo=UTC)
    # Exact same instant expressed in UTC+5:30 (India Standard Time)
    t1_ist = datetime(2026, 11, 9, 15, 30, 0, tzinfo=timezone(timedelta(hours=5, minutes=30)))
    # Different instant
    t2_utc = datetime(2026, 11, 12, 10, 0, 0, tzinfo=UTC)

    # 1. Initial submission with scheduled_at = t1_utc -> 201 Created
    res1 = client.post(
        "/api/v1/tasks",
        json={
            "task_name": "Scheduled Task",
            "task_type": "noop",
            "payload": {"schedule": True},
            "idempotency_key": key,
            "scheduled_at": t1_utc.isoformat(),
        },
    )
    assert res1.status_code == 201
    canonical_id = res1.json()["id"]

    # Verify task and outbox event in DB contain scheduled_at
    with SessionLocal() as db:
        t = db.get(Task, canonical_id)
        assert t is not None
        assert t.scheduled_at is not None

        outbox = db.scalars(
            select(OutboxEvent).where(OutboxEvent.aggregate_id == canonical_id)
        ).first()
        assert outbox is not None
        assert "scheduled_at" in outbox.payload
        assert outbox.payload["scheduled_at"] == t1_utc.isoformat()

    # 2. Re-submission with equivalent instant in different timezone (t1_ist == t1_utc) -> 200 OK
    res_ist = client.post(
        "/api/v1/tasks",
        json={
            "task_name": "Scheduled Task",
            "task_type": "noop",
            "payload": {"schedule": True},
            "idempotency_key": key,
            "scheduled_at": t1_ist.isoformat(),
        },
    )
    assert res_ist.status_code == 200
    assert res_ist.json()["id"] == canonical_id

    # 3. Conflicting scheduled_at (t2 != t1) -> 409 Conflict
    res_conflict = client.post(
        "/api/v1/tasks",
        json={
            "task_name": "Scheduled Task",
            "task_type": "noop",
            "payload": {"schedule": True},
            "idempotency_key": key,
            "scheduled_at": t2_utc.isoformat(),
        },
    )
    assert res_conflict.status_code == 409
    assert res_conflict.json()["error"]["code"] == "IDEMPOTENCY_CONFLICT"

    # Cleanup: publish pending outbox event so it is not permanently stranded in PENDING
    from unittest.mock import MagicMock
    from taskforge.queue.outbox import publish_pending_outbox_events

    with SessionLocal() as db:
        mock_b = MagicMock()
        publish_pending_outbox_events(db, mock_b, event_id=outbox.id)


def test_scheduled_at_outbox_propagation_and_delayed_dispatch():
    """Verify that tasks scheduled in the future remain in PENDING status in the outbox
    and are not dispatched to the message broker until their scheduled instant arrives.
    """
    from datetime import timedelta
    from unittest.mock import MagicMock

    from taskforge.database.models import get_utc_now
    from taskforge.queue.outbox import publish_pending_outbox_events

    mock_broker = MagicMock()
    future_time = get_utc_now() + timedelta(hours=2)

    with SessionLocal() as db:
        service = TaskService(db)
        req = TaskCreateRequest(
            task_name="Future Scheduled Task",
            task_type="noop",
            payload={"future": True},
            idempotency_key=f"future-{uuid.uuid4()}",
            scheduled_at=future_time,
        )
        task, _ = service.submit_task(req)

        # 1. Verify OutboxEvent payload has scheduled_at
        outbox = db.scalars(
            select(OutboxEvent).where(OutboxEvent.aggregate_id == task.id)
        ).first()
        assert outbox is not None
        assert outbox.status == "PENDING"
        assert outbox.payload.get("scheduled_at") is not None

        # 2. Run publish_pending_outbox_events: since scheduled_at is 2h in the future,
        # our task must NOT be enqueued to broker and must stay PENDING
        _ = publish_pending_outbox_events(db, mock_broker, event_id=outbox.id)
        db.refresh(outbox)
        assert outbox.status == "PENDING"
        enqueued_ids = [
            c.kwargs.get("task_id")
            for c in mock_broker.enqueue.call_args_list
            if "task_id" in c.kwargs
        ]
        assert task.id not in enqueued_ids

        # 3. Now simulate reaching the scheduled instant by updating scheduled_at to past
        outbox.payload["scheduled_at"] = (get_utc_now() - timedelta(minutes=1)).isoformat()
        from sqlalchemy.orm.attributes import flag_modified

        flag_modified(outbox, "payload")
        db.commit()

        # 4. Outbox publisher now processes and publishes it to broker
        mock_broker.reset_mock()
        _ = publish_pending_outbox_events(db, mock_broker, event_id=outbox.id)
        db.refresh(outbox)
        assert outbox.status == "PUBLISHED"
        enqueued_ids_after = [
            c.kwargs.get("task_id")
            for c in mock_broker.enqueue.call_args_list
            if "task_id" in c.kwargs
        ]
        assert task.id in enqueued_ids_after


def test_idempotency_re_raises_unrelated_database_errors():
    """Verify TaskRepository strictly verifies SQLSTATE 23505 and exact known constraint names,
    re-raising all unrelated IntegrityError exceptions or PostgreSQL errors with unverified constraint names.
    """
    from unittest.mock import MagicMock

    from sqlalchemy.exc import IntegrityError

    from taskforge.database.repositories.task_repository import TaskRepository

    with SessionLocal() as db:
        repo = TaskRepository(db)

        # Case 1: Unrelated generic check constraint violation
        unrelated_exc = IntegrityError(
            statement="INSERT INTO tasks ...",
            params={},
            orig=Exception("check constraint tasks_priority_check violated"),
        )
        assert repo._is_idempotency_key_violation(unrelated_exc) is False

        # Case 2: PostgreSQL SQLSTATE 23505 but on primary key (tasks_pkey), NOT idempotency_key
        mock_orig_pk = MagicMock()
        mock_orig_pk.sqlstate = "23505"
        mock_orig_pk.diag.constraint_name = "tasks_pkey"
        mock_orig_pk.diag.message_detail = "Key (id)=(...) already exists."
        pk_exc = IntegrityError(statement="INSERT ...", params={}, orig=mock_orig_pk)
        assert repo._is_idempotency_key_violation(pk_exc) is False

        # Case 3: PostgreSQL SQLSTATE 23505 with unverified / missing constraint name
        mock_orig_unknown = MagicMock()
        mock_orig_unknown.sqlstate = "23505"
        mock_orig_unknown.diag.constraint_name = ""
        mock_orig_unknown.diag.message_detail = ""
        unknown_exc = IntegrityError(statement="INSERT ...", params={}, orig=mock_orig_unknown)
        assert repo._is_idempotency_key_violation(unknown_exc) is False

        # Case 4: PostgreSQL SQLSTATE 23505 with exact known constraint name -> True
        mock_orig_valid = MagicMock()
        mock_orig_valid.sqlstate = "23505"
        mock_orig_valid.diag.constraint_name = "tasks_idempotency_key_key"
        mock_orig_valid.diag.message_detail = "Key (idempotency_key)=(...) already exists."
        valid_exc = IntegrityError(statement="INSERT ...", params={}, orig=mock_orig_valid)
        assert repo._is_idempotency_key_violation(valid_exc) is True



