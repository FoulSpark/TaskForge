import time
from datetime import UTC, datetime, timedelta

import pytest

from taskforge.core.state_machine import TaskPriority, TaskStatus
from taskforge.database.models import get_utc_now
from taskforge.database.repositories.task_repository import TaskRepository
from taskforge.database.session import SessionLocal
from taskforge.monitoring.metrics import (
    DEAD_LETTER_TOTAL,
    LEASE_RECOVERIES_TOTAL,
    RETRIES_TOTAL,
    TASK_EXECUTION_DURATION,
    TASK_QUEUE_WAIT,
    TASKS_COMPLETED,
    TASKS_SUBMITTED,
    normalize_failure_reason,
    normalize_initiator,
    normalize_priority,
    normalize_recovery_result,
    normalize_status,
    normalize_task_type,
)
from taskforge.tasks.registry import register_handler
from taskforge.workers.execution_engine import ExecutionDisposition, execute_task


@pytest.fixture
def repo():
    with SessionLocal() as db:
        yield TaskRepository(db)


def _get_histogram_count(histogram_metric) -> float:
    """Helper to extract _count sample value from prometheus Histogram metric."""
    for s in histogram_metric._child_samples():
        if s.name == "_count":
            return s.value
    return 0.0


def test_acceptance_1_idempotency_submission_accounting(repo):
    """Acceptance Test 1: Repeated idempotent submissions do not increment submission metrics twice."""
    key = f"idem-key-{time.time()}"
    prio = TaskPriority.HIGH.value
    task_type = "payment_reconcile"

    c_before = TASKS_SUBMITTED.labels(task_type=task_type, priority="high")._value.get()

    # First submission -> created=True -> metric increments
    res1 = repo.create_task(
        task_name="test_p1",
        task_type=task_type,
        payload={"k": "v"},
        priority=prio,
        idempotency_key=key,
    )
    assert res1.created is True
    c_after_1 = TASKS_SUBMITTED.labels(task_type=task_type, priority="high")._value.get()
    assert c_after_1 == c_before + 1

    # Second submission (identical idempotency key) -> created=False -> metric MUST NOT increment
    res2 = repo.create_task(
        task_name="test_p1",
        task_type=task_type,
        payload={"k": "v"},
        priority=prio,
        idempotency_key=key,
    )
    assert res2.created is False
    c_after_2 = TASKS_SUBMITTED.labels(task_type=task_type, priority="high")._value.get()
    assert c_after_2 == c_after_1


def test_acceptance_2_transaction_rollback_no_metrics(repo, monkeypatch):
    """Acceptance Test 2: Rolled-back transactions produce no lifecycle observations."""
    task_type = "image_resize"
    c_before = TASKS_SUBMITTED.labels(task_type=task_type, priority="critical")._value.get()

    # Monkeypatch commit on the session to simulate an unhandled database commit error
    def failing_commit():
        raise RuntimeError("Simulated Database Commit Failure")

    monkeypatch.setattr(repo.db, "commit", failing_commit)

    with pytest.raises(RuntimeError, match="Simulated Database Commit Failure"):
        repo.create_task(
            task_name="failing_task",
            task_type=task_type,
            payload={},
            priority=TaskPriority.CRITICAL.value,
        )

    c_after = TASKS_SUBMITTED.labels(task_type=task_type, priority="critical")._value.get()
    assert c_after == c_before, "Rolled back task creation must not record submission metric"


def test_acceptance_3_fencing_duplicate_stale_deliveries_no_metrics(repo):
    """Acceptance Test 3: Duplicate or stale deliveries do not increment completion or retry counters."""
    task_type = "report_generate"
    # Create task
    res = repo.create_task(
        task_name="fencing_test",
        task_type=task_type,
        payload={"report_type": "quarterly"},
        priority=TaskPriority.MEDIUM.value,
    )
    task_id = res.task.id

    # Execute task to terminal success
    disp1 = execute_task(task_id, worker_id="worker-valid-1")
    assert disp1 == ExecutionDisposition.COMPLETED

    c_success_before = TASKS_COMPLETED.labels(task_type=task_type, status="SUCCESS")._value.get()
    c_retries_before = RETRIES_TOTAL.labels(task_type=task_type, failure_reason="exception")._value.get()

    # Duplicate delivery arrives for already-terminal task
    disp2 = execute_task(task_id, worker_id="worker-duplicate-2")
    assert disp2 == ExecutionDisposition.TERMINAL_DUPLICATE

    c_success_after = TASKS_COMPLETED.labels(task_type=task_type, status="SUCCESS")._value.get()
    c_retries_after = RETRIES_TOTAL.labels(task_type=task_type, failure_reason="exception")._value.get()

    assert c_success_after == c_success_before, "Duplicate delivery must not increment completion counter"
    assert c_retries_after == c_retries_before, "Duplicate delivery must not increment retry counter"

    # Stale lease token fencing: worker with forged/expired token rejected
    with pytest.raises(PermissionError):
        repo.complete_task_success(task_id, lease_token="forged-token-999", return_value={"done": True})

    assert TASKS_COMPLETED.labels(task_type=task_type, status="SUCCESS")._value.get() == c_success_before


def test_acceptance_4_execution_timing_handler_only(repo):
    """Acceptance Test 4: Handler duration excludes deserialization, lease renewal, and result persistence."""
    task_type = "webhook_send"

    # Register timed mock handler that runs for exactly 50ms
    sleep_seconds = 0.05

    def timed_handler(payload):
        time.sleep(sleep_seconds)
        return {"delivered": True}

    register_handler(task_type)(timed_handler)

    res = repo.create_task(
        task_name="timed_task",
        task_type=task_type,
        payload={"url": "https://example.com/webhook"},
        priority=TaskPriority.LOW.value,
    )
    task_id = res.task.id

    metric = TASK_EXECUTION_DURATION.labels(task_type=task_type)
    sum_before = metric._sum.get()
    count_before = _get_histogram_count(metric)

    disp = execute_task(task_id, worker_id="worker-timed")
    assert disp == ExecutionDisposition.COMPLETED

    sum_after = metric._sum.get()
    count_after = _get_histogram_count(metric)

    assert count_after == count_before + 1
    duration_recorded = sum_after - sum_before
    # Recorded duration should strictly reflect the handler execution (~0.05s) and not include external overhead
    assert sleep_seconds <= duration_recorded < 0.35, f"Unexpected handler duration: {duration_recorded}"


def test_acceptance_5_retry_accounting_committed_transitions(repo):
    """Acceptance Test 5: Retry metrics increment only after the retry transition commits."""
    task_type = "csv_export"

    res = repo.create_task(
        task_name="retry_task",
        task_type=task_type,
        payload={},
        priority=TaskPriority.HIGH.value,
        max_retries=2,
    )
    task_id = res.task.id

    # Acquire lease and move to RUNNING
    assert repo.acquire_lease(task_id, worker_id="worker-retry", lease_token="token-retry-1") is True
    repo.update_status(task_id, TaskStatus.RUNNING, lease_token="token-retry-1")
    attempt = repo.record_attempt(task_id, worker_id="worker-retry", lease_token="token-retry-1")

    r_exc_before = RETRIES_TOTAL.labels(task_type=task_type, failure_reason="exception")._value.get()
    r_to_before = RETRIES_TOTAL.labels(task_type=task_type, failure_reason="timeout")._value.get()

    # Fail task with exception (retryable)
    task_upd, is_retryable = repo.complete_task_failure(
        task_id=task_id,
        lease_token="token-retry-1",
        error_message="Network glitch",
        attempt_id=attempt.id,
        is_timeout=False,
    )
    assert is_retryable is True
    assert task_upd.status == TaskStatus.RETRY_WAIT.value

    r_exc_after = RETRIES_TOTAL.labels(task_type=task_type, failure_reason="exception")._value.get()
    assert r_exc_after == r_exc_before + 1
    assert RETRIES_TOTAL.labels(task_type=task_type, failure_reason="timeout")._value.get() == r_to_before


def test_acceptance_6_dead_letter_accounting_exact_once(repo):
    """Acceptance Test 6: Dead-letter metrics increment exactly once per authoritative transition."""
    task_type = "image_resize"

    # Task with max_retries=0 -> immediate dead letter on first failure
    res = repo.create_task(
        task_name="dlq_task",
        task_type=task_type,
        payload={},
        priority=TaskPriority.MEDIUM.value,
        max_retries=0,
    )
    task_id = res.task.id

    assert repo.acquire_lease(task_id, worker_id="worker-dlq", lease_token="token-dlq-1") is True
    repo.update_status(task_id, TaskStatus.RUNNING, lease_token="token-dlq-1")
    attempt = repo.record_attempt(task_id, worker_id="worker-dlq", lease_token="token-dlq-1")

    dlq_before = DEAD_LETTER_TOTAL.labels(task_type=task_type)._value.get()
    completed_dlq_before = TASKS_COMPLETED.labels(task_type=task_type, status="DEAD_LETTER")._value.get()

    task_upd, is_retryable = repo.complete_task_failure(
        task_id=task_id,
        lease_token="token-dlq-1",
        error_message="Unrecoverable corruption",
        attempt_id=attempt.id,
        is_timeout=False,
    )
    assert is_retryable is False
    assert task_upd.status == TaskStatus.DEAD_LETTER.value

    dlq_after = DEAD_LETTER_TOTAL.labels(task_type=task_type)._value.get()
    completed_dlq_after = TASKS_COMPLETED.labels(task_type=task_type, status="DEAD_LETTER")._value.get()

    assert dlq_after == dlq_before + 1, "Dead letter counter must increment exactly once"
    assert completed_dlq_after == completed_dlq_before + 1, "Completed counter with DEAD_LETTER must increment exactly once"


def test_acceptance_7_telemetry_isolation(repo, monkeypatch):
    """Acceptance Test 7: Metrics collection failures cannot change task outcomes."""
    task_type = "payment_reconcile"

    # Monkeypatch TASKS_SUBMITTED.labels to throw an unexpected exception
    def failing_labels(**kwargs):
        raise RuntimeError("Prometheus collector catastrophic failure")

    monkeypatch.setattr(TASKS_SUBMITTED, "labels", failing_labels)

    # Core task submission must still succeed and commit cleanly
    res = repo.create_task(
        task_name="isolated_task",
        task_type=task_type,
        payload={"amt": 100},
        priority=TaskPriority.HIGH.value,
    )
    assert res.created is True
    assert res.task.id is not None
    assert repo.get_by_id(res.task.id) is not None, "Task must exist in database despite metrics failure"


def test_acceptance_8_cardinality_label_normalization():
    """Acceptance Test 8: Runtime label values remain within the approved taxonomy."""
    # Priority normalization
    assert normalize_priority(4) == "critical"
    assert normalize_priority("critical") == "critical"
    assert normalize_priority(TaskPriority.CRITICAL) == "critical"
    assert normalize_priority(3) == "high"
    assert normalize_priority(2) == "medium"
    assert normalize_priority(1) == "low"
    assert normalize_priority(9999) == "low"
    assert normalize_priority("unbounded_malicious_string") == "low"

    # Task type normalization
    for approved in ["image_resize", "payment_reconcile", "report_generate", "webhook_send", "csv_export"]:
        assert normalize_task_type(approved) == approved
    assert normalize_task_type("custom_arbitrary_user_type_injection") == "other"
    assert normalize_task_type(None) == "other"

    # Status normalization
    assert normalize_status("SUCCESS") == "SUCCESS"
    assert normalize_status("FAILED") == "FAILED"
    assert normalize_status("CANCELLED") == "CANCELLED"
    assert normalize_status("DEAD_LETTER") == "DEAD_LETTER"
    assert normalize_status("arbitrary_unknown") == "FAILED"

    # Failure reason normalization
    assert normalize_failure_reason("timeout") == "timeout"
    assert normalize_failure_reason("exception") == "exception"
    assert normalize_failure_reason("circuit_breaker") == "circuit_breaker"
    assert normalize_failure_reason("random_error_message") == "other"

    # Initiator normalization
    assert normalize_initiator("supervisor") == "supervisor"
    assert normalize_initiator("operator") == "operator"
    assert normalize_initiator("random_attacker") == "supervisor"

    # Recovery result normalization
    assert normalize_recovery_result("requeued") == "requeued"
    assert normalize_recovery_result("abandoned") == "abandoned"
    assert normalize_recovery_result("unknown_result") == "requeued"


def test_stage_7_lease_recovery_instrumentation(repo):
    """Stage 7 Verification: Lease recovery instrumentation with initiator and recovery_result."""
    now = get_utc_now()
    task_type = "report_generate"

    # Create task and set it directly to RUNNING with expired lease in past
    res = repo.create_task(
        task_name="expired_task",
        task_type=task_type,
        payload={"report_type": "annual"},
        priority=TaskPriority.HIGH.value,
        max_retries=2,
    )
    t = res.task
    t.status = TaskStatus.RUNNING.value
    t.lease_token = "lease-expired-tok"
    t.lease_expires_at = now - timedelta(seconds=60)
    repo.db.commit()

    rec_requeued_before = LEASE_RECOVERIES_TOTAL.labels(initiator="operator", recovery_result="requeued")._value.get()

    recovered = repo.recover_expired_leases(initiator="operator")
    assert len(recovered) >= 1
    target_rec = next(r for r in recovered if r.task_id == t.id)
    assert target_rec.status == TaskStatus.RETRY_WAIT.value

    requeued_count = sum(1 for r in recovered if r.status == TaskStatus.RETRY_WAIT.value)
    rec_requeued_after = LEASE_RECOVERIES_TOTAL.labels(initiator="operator", recovery_result="requeued")._value.get()
    assert rec_requeued_after == rec_requeued_before + requeued_count


def test_stage_3_queue_wait_instrumentation(repo):
    """Stage 3 Verification: Queue wait measured from durable creation to attempt lease acquisition."""
    task_type = "image_resize"
    past_creation = get_utc_now() - timedelta(seconds=2.5)

    res = repo.create_task(
        task_name="queue_wait_task",
        task_type=task_type,
        payload={},
        priority=TaskPriority.CRITICAL.value,
    )
    t = res.task
    t.created_at = past_creation
    repo.db.commit()

    metric = TASK_QUEUE_WAIT.labels(task_type=task_type, priority="critical")
    count_before = _get_histogram_count(metric)
    sum_before = metric._sum.get()

    assert repo.acquire_lease(t.id, worker_id="worker-qw", lease_token="qw-tok") is True
    attempt = repo.record_attempt(t.id, worker_id="worker-qw", lease_token="qw-tok")

    count_after = _get_histogram_count(metric)
    sum_after = metric._sum.get()

    assert count_after == count_before + 1
    recorded_wait = sum_after - sum_before
    assert 2.0 <= recorded_wait <= 4.0, f"Expected queue wait ~2.5s, got {recorded_wait}"
