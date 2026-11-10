import threading
import time
from datetime import UTC, datetime, timedelta

import pytest
from prometheus_client import REGISTRY

from taskforge.core.state_machine import TaskPriority, TaskStatus
from taskforge.database.models import get_utc_now
from taskforge.database.repositories.task_repository import TaskRepository
from taskforge.database.session import SessionLocal
from taskforge.monitoring.metrics import (
    ACTIVE_WORKERS,
    API_REQUEST_DURATION,
    API_REQUESTS_TOTAL,
    DB_POOL_CONNECTIONS,
    DEAD_LETTER_TOTAL,
    FAILURE_REASON_WHITELIST,
    HTTP_METHOD_WHITELIST,
    INITIATOR_WHITELIST,
    LEASE_RECOVERIES_TOTAL,
    OUTBOX_LAG_SECONDS,
    OUTBOX_PENDING_EVENTS,
    POOL_STATE_WHITELIST,
    PRIORITY_WHITELIST,
    QUEUE_DEPTH,
    RATE_LIMIT_EXCEEDED_TOTAL,
    RECOVERY_RESULT_WHITELIST,
    RETRIES_TOTAL,
    ROUTE_WHITELIST,
    STATUS_CODE_CLASS_WHITELIST,
    STATUS_WHITELIST,
    TASK_EXECUTION_DURATION,
    TASK_QUEUE_WAIT,
    TASK_TYPE_WHITELIST,
    TASKS_COMPLETED,
    TASKS_SUBMITTED,
    TELEMETRY_SCRAPE_ERRORS_TOTAL,
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
    """Acceptance Test 4: Handler duration measured strictly inside callable thread wrapper, excluding overhead."""
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
    # Recorded duration should strictly reflect the handler execution (~0.05s) and not include external scheduling overhead
    assert sleep_seconds <= duration_recorded < 0.35, f"Unexpected handler duration: {duration_recorded}"


def test_acceptance_4_timeout_does_not_record_premature_caller_duration(repo):
    """Finding 1 Verification: A timed-out task does not record premature elapsed duration from caller."""
    task_type = "image_resize"
    handler_unblock = threading.Event()

    def hanging_handler(payload):
        # Blocks until explicitly unblocked by test teardown
        handler_unblock.wait(timeout=2.0)
        return {"hung": False}

    register_handler(task_type)(hanging_handler)

    res = repo.create_task(
        task_name="hanging_task",
        task_type=task_type,
        payload={},
        priority=TaskPriority.MEDIUM.value,
        timeout_seconds=1,  # Authoritative 1s timeout
    )
    task_id = res.task.id

    metric = TASK_EXECUTION_DURATION.labels(task_type=task_type)
    count_before = _get_histogram_count(metric)

    # Execute task with timeout - caller times out
    disp = execute_task(task_id, worker_id="worker-timeout-test", timeout_seconds=1)
    # The caller caught TimeoutError and scheduled retry
    assert disp in (ExecutionDisposition.RETRY_DURABLY_SCHEDULED, ExecutionDisposition.TERMINAL_FAILURE_RECORDED)

    # While the thread is still running inside hanging_handler, caller must NOT have recorded a premature duration!
    count_immediately_after = _get_histogram_count(metric)
    assert count_immediately_after == count_before, "Caller must not record premature handler duration upon TimeoutError"

    # Unblock handler thread so it can exit cleanly
    handler_unblock.set()
    time.sleep(0.1)


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


def test_terminal_transition_idempotency_all_terminal_states(repo):
    """Finding 2.B Verification: Repeated updates on already-terminal tasks must NEVER increment completion metrics."""
    # 1. CANCELLED state
    res_cancel = repo.create_task("t_c", "report_generate", {}, TaskPriority.LOW.value)
    tid_c = res_cancel.task.id
    c_before = TASKS_COMPLETED.labels(task_type="report_generate", status="CANCELLED")._value.get()

    repo.cancel_task(tid_c)
    assert TASKS_COMPLETED.labels(task_type="report_generate", status="CANCELLED")._value.get() == c_before + 1

    # Repeated update_status(force=True, CANCELLED) on already-CANCELLED task
    repo.update_status(tid_c, TaskStatus.CANCELLED, force=True)
    assert TASKS_COMPLETED.labels(task_type="report_generate", status="CANCELLED")._value.get() == c_before + 1

    # 2. SUCCESS state
    res_success = repo.create_task("t_s", "webhook_send", {}, TaskPriority.LOW.value)
    tid_s = res_success.task.id
    s_before = TASKS_COMPLETED.labels(task_type="webhook_send", status="SUCCESS")._value.get()

    repo.update_status(tid_s, TaskStatus.RUNNING)
    repo.update_status(tid_s, TaskStatus.SUCCESS)
    assert TASKS_COMPLETED.labels(task_type="webhook_send", status="SUCCESS")._value.get() == s_before + 1

    # Repeated transition on already-SUCCESS task
    repo.update_status(tid_s, TaskStatus.SUCCESS, force=True)
    assert TASKS_COMPLETED.labels(task_type="webhook_send", status="SUCCESS")._value.get() == s_before + 1

    # 3. DEAD_LETTER state
    res_dlq = repo.create_task("t_d", "csv_export", {}, TaskPriority.LOW.value)
    tid_d = res_dlq.task.id
    d_before = TASKS_COMPLETED.labels(task_type="csv_export", status="DEAD_LETTER")._value.get()
    dlq_count_before = DEAD_LETTER_TOTAL.labels(task_type="csv_export")._value.get()

    repo.update_status(tid_d, TaskStatus.RUNNING)
    repo.update_status(tid_d, TaskStatus.DEAD_LETTER, force=True)
    assert TASKS_COMPLETED.labels(task_type="csv_export", status="DEAD_LETTER")._value.get() == d_before + 1
    assert DEAD_LETTER_TOTAL.labels(task_type="csv_export")._value.get() == dlq_count_before + 1

    # Repeated transition on already-DEAD_LETTER task
    repo.update_status(tid_d, TaskStatus.DEAD_LETTER, force=True)
    assert TASKS_COMPLETED.labels(task_type="csv_export", status="DEAD_LETTER")._value.get() == d_before + 1
    assert DEAD_LETTER_TOTAL.labels(task_type="csv_export")._value.get() == dlq_count_before + 1

    # 4. FAILED state (explicit verification of FAILED terminal transition idempotency)
    res_failed = repo.create_task("t_f", "image_resize", {}, TaskPriority.LOW.value)
    tid_f = res_failed.task.id
    f_before = TASKS_COMPLETED.labels(task_type="image_resize", status="FAILED")._value.get()

    repo.update_status(tid_f, TaskStatus.RUNNING)
    repo.update_status(tid_f, TaskStatus.FAILED)
    assert TASKS_COMPLETED.labels(task_type="image_resize", status="FAILED")._value.get() == f_before + 1

    # Repeated transition on already-FAILED task MUST NOT increment completion counter
    repo.update_status(tid_f, TaskStatus.FAILED, force=True)
    assert TASKS_COMPLETED.labels(task_type="image_resize", status="FAILED")._value.get() == f_before + 1


def test_repeated_lease_recovery_idempotency(repo):
    """Finding 2.C Verification: A repeated recovery scan cannot count the same transition twice."""
    now = get_utc_now()
    task_type = "payment_reconcile"

    res = repo.create_task("exp_task_idem", task_type, {}, TaskPriority.HIGH.value, max_retries=1)
    t = res.task
    t.status = TaskStatus.RUNNING.value
    t.lease_token = "lease-tok-rep"
    t.lease_expires_at = now - timedelta(seconds=90)
    repo.db.commit()

    requeued_before = LEASE_RECOVERIES_TOTAL.labels(initiator="supervisor", recovery_result="requeued")._value.get()

    # First recovery scan: reaps the task to RETRY_WAIT
    recovered_1 = repo.recover_expired_leases(initiator="supervisor")
    assert any(r.task_id == t.id for r in recovered_1)

    requeued_after_1 = LEASE_RECOVERIES_TOTAL.labels(initiator="supervisor", recovery_result="requeued")._value.get()
    assert requeued_after_1 > requeued_before

    # Immediate second recovery scan: task is in RETRY_WAIT, not RUNNING -> matches 0 tasks
    recovered_2 = repo.recover_expired_leases(initiator="supervisor")
    assert not any(r.task_id == t.id for r in recovered_2)

    requeued_after_2 = LEASE_RECOVERIES_TOTAL.labels(initiator="supervisor", recovery_result="requeued")._value.get()
    assert requeued_after_2 == requeued_after_1, "Repeated recovery scan must not increment recovery metric"


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


def test_cardinality_ceiling_exhaustive_runtime_exposition():
    """Finding 2.D Verification: Exhaustive population of all approved taxonomy combinations strictly conforms to 662 base series (855 total)."""
    # 1. TASKS_SUBMITTED: 6 task_types * 4 priorities = 24
    for t in list(TASK_TYPE_WHITELIST) + ["other"]:
        for p in PRIORITY_WHITELIST:
            TASKS_SUBMITTED.labels(task_type=t, priority=p).inc(0)

    # 2. TASK_QUEUE_WAIT: 6 task_types * 4 priorities = 24 combinations * 12 buckets = 288 series
    for t in list(TASK_TYPE_WHITELIST) + ["other"]:
        for p in PRIORITY_WHITELIST:
            TASK_QUEUE_WAIT.labels(task_type=t, priority=p).observe(0.1)

    # 3. TASK_EXECUTION_DURATION: 6 task_types = 6 combinations * 11 buckets = 66 series
    for t in list(TASK_TYPE_WHITELIST) + ["other"]:
        TASK_EXECUTION_DURATION.labels(task_type=t).observe(0.01)

    # 4. TASKS_COMPLETED: 6 task_types * 4 statuses = 24
    for t in list(TASK_TYPE_WHITELIST) + ["other"]:
        for s in STATUS_WHITELIST:
            TASKS_COMPLETED.labels(task_type=t, status=s).inc(0)

    # 5. RETRIES_TOTAL: 6 task_types * 4 failure_reasons = 24
    for t in list(TASK_TYPE_WHITELIST) + ["other"]:
        for f in list(FAILURE_REASON_WHITELIST) + ["other"]:
            RETRIES_TOTAL.labels(task_type=t, failure_reason=f).inc(0)

    # 6. DEAD_LETTER_TOTAL: 6 task_types = 6
    for t in list(TASK_TYPE_WHITELIST) + ["other"]:
        DEAD_LETTER_TOTAL.labels(task_type=t).inc(0)

    # 7. QUEUE_DEPTH: 4 priorities = 4
    for p in PRIORITY_WHITELIST:
        QUEUE_DEPTH.labels(priority=p).set(0)

    # 8. OUTBOX_PENDING_EVENTS = 1
    OUTBOX_PENDING_EVENTS.set(0)

    # 9. OUTBOX_LAG_SECONDS = 1
    OUTBOX_LAG_SECONDS.set(0)

    # 10. LEASE_RECOVERIES_TOTAL: 2 initiators * 2 recovery_results = 4
    for i in INITIATOR_WHITELIST:
        for r in RECOVERY_RESULT_WHITELIST:
            LEASE_RECOVERIES_TOTAL.labels(initiator=i, recovery_result=r).inc(0)

    # 11. API_REQUEST_DURATION: 3 methods * 5 routes = 15 combinations * 10 buckets = 150 series
    for m in list(HTTP_METHOD_WHITELIST) + ["other"]:
        for route in list(ROUTE_WHITELIST) + ["other"]:
            API_REQUEST_DURATION.labels(http_method=m, route=route).observe(0.01)

    # 12. API_REQUESTS_TOTAL: 3 methods * 5 routes * 4 status_code_classes = 60
    for m in list(HTTP_METHOD_WHITELIST) + ["other"]:
        for route in list(ROUTE_WHITELIST) + ["other"]:
            for sc in list(STATUS_CODE_CLASS_WHITELIST) + ["other"]:
                API_REQUESTS_TOTAL.labels(http_method=m, route=route, status_code_class=sc).inc(0)

    # 13. RATE_LIMIT_EXCEEDED_TOTAL: 5 routes = 5
    for route in list(ROUTE_WHITELIST) + ["other"]:
        RATE_LIMIT_EXCEEDED_TOTAL.labels(route=route).inc(0)

    # 14. ACTIVE_WORKERS = 1
    ACTIVE_WORKERS.set(0)

    # 15. DB_POOL_CONNECTIONS: 3 pool_states = 3
    for ps in POOL_STATE_WHITELIST:
        DB_POOL_CONNECTIONS.labels(pool_state=ps).set(0)

    # 16. TELEMETRY_SCRAPE_ERRORS_TOTAL = 1
    TELEMETRY_SCRAPE_ERRORS_TOTAL.inc(0)

    G92_METRIC_NAMES = {
        "taskforge_tasks_submitted",
        "taskforge_task_queue_wait_seconds",
        "taskforge_task_execution_duration_seconds",
        "taskforge_tasks_completed",
        "taskforge_retries",
        "taskforge_dead_letter",
        "taskforge_queue_depth",
        "taskforge_outbox_pending_events",
        "taskforge_outbox_lag_seconds",
        "taskforge_lease_recoveries",
        "taskforge_api_request_duration_seconds",
        "taskforge_api_requests",
        "taskforge_rate_limit_exceeded",
        "taskforge_active_workers",
        "taskforge_db_pool_connections",
        "taskforge_telemetry_scrape_errors",
    }

    # Collect actual exported samples from Prometheus client registry for G9.2 metric families
    all_taskforge_samples = []
    for metric in REGISTRY.collect():
        if metric.name in G92_METRIC_NAMES:
            for s in metric.samples:
                all_taskforge_samples.append(s)

    created_samples = [s for s in all_taskforge_samples if s.name.endswith("_created")]
    base_samples = [s for s in all_taskforge_samples if not s.name.endswith("_created")]

    # Exact equality against documented architectural contract budgets
    assert len(base_samples) == 662, f"Expected 662 base series, got {len(base_samples)}"
    assert len(created_samples) == 193, f"Expected 193 _created series, got {len(created_samples)}"
    assert len(all_taskforge_samples) == 855, f"Expected 855 total series, got {len(all_taskforge_samples)}"


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
