"""Gate G9.4.3: Worker Lease Renewal Telemetry & Multi-Worker Fencing Rejection Test Suite.

Verifies AC-9.4.4 and AC-9.4.6:
1. Telemetry Metric Contract & Cardinality Ceiling:
   - taskforge_lease_renewals_total (Counter, status in {"success", "failed"})
   - taskforge_fencing_rejections_total (Counter, global unlabelled)
   - Option A budget: exactly 6 G9.4.3 series, 25 net-new G9.4 series, 28 combined series.
2. Multi-Worker Stale Ownership & Heartbeat Renewal Rejection:
   - Stale worker heartbeat renewal is rejected when lease is owned by another worker.
   - taskforge_lease_renewals_total{status="failed"} increments.
   - Valid worker renewal succeeds and increments {status="success"}.
3. Authoritative Fencing Rejection on Terminal Commit:
   - Stale worker completion and failure commits strictly rejected with PermissionError.
   - taskforge_fencing_rejections_total increments on each rejected attempt.
   - Accepted stale commits == 0.
   - Final task state and owner reflect the valid worker's authoritative commit.
4. Duplicate Handler Invocation Accounting:
   - Separates duplicate handler execution (at-least-once delivery) from state commit fencing.
   - Verifies duplicate executions recorded independently without corrupting authoritative state.
5. Telemetry Failure Isolation:
   - Telemetry recording failures do not disrupt or mask database transactions.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import uuid

import pytest
from sqlalchemy import select, text
from sqlalchemy.orm import Session

from taskforge.core.state_machine import TaskPriority, TaskStatus
from taskforge.database.models import Task, TaskResult, get_utc_now
from taskforge.database.repositories.task_repository import TaskRepository
from taskforge.database.session import SessionLocal, engine
from taskforge.monitoring.metrics import (
    FENCING_REJECTIONS_TOTAL,
    LEASE_RENEWALS_TOTAL,
    get_metrics_content,
    record_fencing_rejection,
    record_lease_renewal,
)


@pytest.fixture(autouse=True)
def clean_benchmark_tasks():
    """Clean benchmark tasks before and after tests."""
    yield
    with SessionLocal() as db:
        db.execute(text("DELETE FROM task_events WHERE task_id LIKE 'g943-%'"))
        db.execute(text("DELETE FROM task_attempts WHERE task_id LIKE 'g943-%'"))
        db.execute(text("DELETE FROM task_results WHERE task_id LIKE 'g943-%'"))
        db.execute(text("DELETE FROM tasks WHERE id LIKE 'g943-%'"))
        db.commit()


def test_g943_telemetry_metric_contract_and_cardinality_ceiling():
    """AC-9.4.6: Verifies finite taxonomy, exact series count, and Option A 28-series ceiling."""
    content = get_metrics_content().decode("utf-8")
    lines = [line.strip() for line in content.splitlines() if line.strip() and not line.startswith("#")]

    pool_lines = [l for l in lines if "taskforge_db_pool_" in l]
    lease_lines = [l for l in lines if "taskforge_lease_renewals_" in l or "taskforge_fencing_rejections_" in l]
    g94_additions = [l for l in pool_lines if not l.startswith("taskforge_db_pool_connections{")] + lease_lines

    # 1. G9.4.3 Lease & Fencing Metrics Exist
    assert any("taskforge_lease_renewals_total" in l for l in lease_lines)
    assert any("taskforge_fencing_rejections_total" in l for l in lease_lines)

    # 2. Strict Option A Cardinality Accounting
    # G9.4.3 contributes exactly 6 runtime series:
    # 2 renewal base + 2 renewal _created + 1 fencing base + 1 fencing _created = 6
    assert len(lease_lines) == 6, f"Expected exactly 6 G9.4.3 series, got {len(lease_lines)}"

    # G9.4 Net-New Additions: 19 pool + 6 lease = exactly 25 series
    assert len(g94_additions) == 25, f"Expected exactly 25 G9.4 additions, got {len(g94_additions)}"

    # Combined Exposition: 3 legacy + 25 G9.4 = exactly 28 series
    combined_total = len(pool_lines) + len(lease_lines)
    assert combined_total == 28, f"Expected exactly 28 combined series, got {combined_total}"


def test_g943_stale_worker_heartbeat_renewal_rejection():
    """AC-9.4.4: Verifies stale worker heartbeat renewal rejection after lease takeover."""
    with SessionLocal() as db:
        repo = TaskRepository(db)
        task_id = f"g943-task-heartbeat-{uuid.uuid4().hex[:8]}"
        task = Task(
            id=task_id,
            task_name="G943 Heartbeat Test",
            task_type="noop",
            status=TaskStatus.QUEUED.value,
            priority=TaskPriority.HIGH.value,
            payload={},
        )
        db.add(task)
        db.commit()

        token_a = f"lease-A-{uuid.uuid4().hex[:8]}"
        token_b = f"lease-B-{uuid.uuid4().hex[:8]}"

        # 1. Worker A acquires initial short lease
        assert repo.acquire_lease(task_id, worker_id="worker-A", lease_duration_seconds=1, lease_token=token_a) is True
        repo.update_status(task_id, TaskStatus.RUNNING, {"worker_id": "worker-A"}, lease_token=token_a)

        # Baseline counter values
        failed_before = LEASE_RENEWALS_TOTAL.labels(status="failed")._value.get()
        success_before = LEASE_RENEWALS_TOTAL.labels(status="success")._value.get()

        # 2. Worker A experiences pause; lease expires and Worker B reclaims task
        time.sleep(1.1)
        repo.recover_expired_leases()

        assert repo.acquire_lease(task_id, worker_id="worker-B", lease_duration_seconds=30, lease_token=token_b) is True
        repo.update_status(task_id, TaskStatus.RUNNING, {"worker_id": "worker-B"}, lease_token=token_b)

        # Confirm database state: Worker B is authoritative owner
        db.expire_all()
        t_active = repo.get_by_id(task_id)
        assert t_active.lease_token == token_b

        # 3. Worker A resumes and attempts stale heartbeat renewal with token_A
        renew_a = repo.renew_lease(task_id, lease_token=token_a)
        assert renew_a is False, "Stale worker lease renewal must be rejected"

        failed_after = LEASE_RENEWALS_TOTAL.labels(status="failed")._value.get()
        assert failed_after == failed_before + 1.0, "Failed renewal counter must increment"

        # Confirm Worker B's lease ownership is unharmed
        db.expire_all()
        t_check = repo.get_by_id(task_id)
        assert t_check.lease_token == token_b

        # 4. Worker B issues valid heartbeat renewal with token_B
        renew_b = repo.renew_lease(task_id, lease_token=token_b)
        assert renew_b is True, "Valid owner lease renewal must succeed"

        success_after = LEASE_RENEWALS_TOTAL.labels(status="success")._value.get()
        assert success_after == success_before + 1.0, "Successful renewal counter must increment"


def test_g943_stale_worker_terminal_commit_fenced_and_rejected():
    """AC-9.4.4: Verifies stale worker terminal commits are rejected and recorded."""
    with SessionLocal() as db:
        repo = TaskRepository(db)
        task_id = f"g943-task-terminal-{uuid.uuid4().hex[:8]}"
        task = Task(
            id=task_id,
            task_name="G943 Terminal Fencing Test",
            task_type="noop",
            status=TaskStatus.QUEUED.value,
            priority=TaskPriority.HIGH.value,
            payload={},
        )
        db.add(task)
        db.commit()

        token_a = f"lease-A-{uuid.uuid4().hex[:8]}"
        token_b = f"lease-B-{uuid.uuid4().hex[:8]}"

        # 1. Worker A acquires lease
        assert repo.acquire_lease(task_id, worker_id="worker-A", lease_duration_seconds=1, lease_token=token_a) is True
        repo.update_status(task_id, TaskStatus.RUNNING, {"worker_id": "worker-A"}, lease_token=token_a)

        # Baseline fencing rejection counter
        rejections_before = FENCING_REJECTIONS_TOTAL._value.get()

        # 2. Lease expires and Worker B reclaims
        time.sleep(1.1)
        repo.recover_expired_leases()
        assert repo.acquire_lease(task_id, worker_id="worker-B", lease_duration_seconds=30, lease_token=token_b) is True
        repo.update_status(task_id, TaskStatus.RUNNING, {"worker_id": "worker-B"}, lease_token=token_b)

        # 3. Worker A attempts terminal success commit with token_A
        with pytest.raises(PermissionError, match="Fencing violation"):
            repo.complete_task_success(
                task_id=task_id,
                lease_token=token_a,
                return_value={"unauthorized_result": "from_worker_A"},
            )

        # 4. Worker A attempts terminal failure commit with token_A
        with pytest.raises(PermissionError, match="Fencing violation"):
            repo.complete_task_failure(
                task_id=task_id,
                lease_token=token_a,
                error_message="unauthorized_failure_from_worker_A",
            )

        # Invariant: exactly 2 stale commit attempts reached guard and were rejected
        rejections_after = FENCING_REJECTIONS_TOTAL._value.get()
        assert rejections_after == rejections_before + 2.0

        # Authoritative state in DB is unaffected (still RUNNING under Worker B)
        db.expire_all()
        t_mid = repo.get_by_id(task_id)
        assert t_mid.status == TaskStatus.RUNNING.value
        assert t_mid.lease_token == token_b

        # 5. Worker B commits valid terminal result
        t_final = repo.complete_task_success(
            task_id=task_id,
            lease_token=token_b,
            return_value={"authoritative_result": "from_worker_B"},
        )
        assert t_final.status == TaskStatus.SUCCESS.value
        assert t_final.lease_token is None  # Released on completion

        # Verify persisted result is strictly from Worker B
        result = db.scalars(select(TaskResult).where(TaskResult.task_id == task_id)).first()
        assert result is not None
        assert result.return_value == {"authoritative_result": "from_worker_B"}


def test_g943_duplicate_handler_invocation_accounting():
    """AC-9.4.4: Demonstrates distinction between duplicate handler execution (at-least-once delivery)
    and authoritative state commit fencing.
    """
    handler_executions = 0
    stale_commit_attempts = 0
    accepted_stale_commits = 0

    with SessionLocal() as db:
        repo = TaskRepository(db)
        task_id = f"g943-task-duphandler-{uuid.uuid4().hex[:8]}"
        task = Task(
            id=task_id,
            task_name="G943 Duplicate Handler Accounting",
            task_type="noop",
            status=TaskStatus.QUEUED.value,
            priority=TaskPriority.HIGH.value,
            payload={},
        )
        db.add(task)
        db.commit()

        token_a = f"lease-A-{uuid.uuid4().hex[:8]}"
        token_b = f"lease-B-{uuid.uuid4().hex[:8]}"

        # Worker A acquires lease
        repo.acquire_lease(task_id, worker_id="worker-A", lease_duration_seconds=1, lease_token=token_a)
        repo.update_status(task_id, TaskStatus.RUNNING, {"worker_id": "worker-A"}, lease_token=token_a)

        # Worker A starts handler execution
        handler_executions += 1

        # While Worker A is processing, Worker A's lease expires; Worker B acquires task
        time.sleep(1.1)
        repo.recover_expired_leases()
        repo.acquire_lease(task_id, worker_id="worker-B", lease_duration_seconds=30, lease_token=token_b)
        repo.update_status(task_id, TaskStatus.RUNNING, {"worker_id": "worker-B"}, lease_token=token_b)

        # Worker B also executes handler (simulating redelivery)
        handler_executions += 1

        # Worker A finishes execution and attempts commit with token_A
        stale_commit_attempts += 1
        try:
            repo.complete_task_success(task_id, token_a, {"worker": "A"})
            accepted_stale_commits += 1
        except PermissionError:
            pass  # Expected fencing rejection

        # Worker B finishes execution and commits with token_B
        t_final = repo.complete_task_success(task_id, token_b, {"worker": "B"})

        # Accounting Invariants
        assert handler_executions == 2, "Handler was invoked twice across workers (at-least-once)"
        assert stale_commit_attempts == 1, "Exactly one stale commit attempt occurred"
        assert accepted_stale_commits == 0, "Accepted stale commits must strictly be 0"
        assert t_final.status == TaskStatus.SUCCESS.value


def test_g943_telemetry_failure_isolation(monkeypatch):
    """AC-9.4.6: Confirms that failures in telemetry recording do not corrupt or abort database state transitions."""
    with SessionLocal() as db:
        repo = TaskRepository(db)
        task_id = f"g943-task-telemetryfail-{uuid.uuid4().hex[:8]}"
        task = Task(
            id=task_id,
            task_name="G943 Telemetry Failure Test",
            task_type="noop",
            status=TaskStatus.QUEUED.value,
            priority=TaskPriority.HIGH.value,
            payload={},
        )
        db.add(task)
        db.commit()

        token = f"lease-tok-{uuid.uuid4().hex[:8]}"
        repo.acquire_lease(task_id, worker_id="worker-test", lease_duration_seconds=30, lease_token=token)
        repo.update_status(task_id, TaskStatus.RUNNING, {"worker_id": "worker-test"}, lease_token=token)

        # Inject exception into record_lease_renewal
        def crashing_renewal(status):
            raise RuntimeError("CRASH_IN_TELEMETRY_RECORD_LEASE_RENEWAL")

        monkeypatch.setattr("taskforge.database.repositories.task_repository.record_lease_renewal", crashing_renewal)

        # Business operation MUST succeed despite telemetry failure
        renewed = repo.renew_lease(task_id, lease_token=token)
        assert renewed is True, "Business lease renewal must succeed despite telemetry failure"

        # Inject exception into record_fencing_rejection
        def crashing_fencing():
            raise RuntimeError("CRASH_IN_TELEMETRY_RECORD_FENCING")

        monkeypatch.setattr("taskforge.database.repositories.task_repository.record_fencing_rejection", crashing_fencing)

        # Fencing guard must STILL raise PermissionError (business security invariant preserved)
        with pytest.raises(PermissionError, match="Fencing violation"):
            repo.complete_task_success(task_id, lease_token="bad-token", return_value={})


def test_g943_concurrent_multi_worker_fencing_accounting():
    """AC-9.4.4: Executes a multi-worker concurrency drill across 10 tasks verifying:
    1. Stale commit attempts reaching guard == Fencing rejections.
    2. Accepted stale commits == 0.
    3. Final authoritative state committed solely by valid owner.
    """
    num_tasks = 10
    task_ids = []
    tokens_a = {}
    tokens_b = {}

    with SessionLocal() as db:
        repo = TaskRepository(db)
        for i in range(num_tasks):
            tid = f"g943-task-multi-{i}-{uuid.uuid4().hex[:6]}"
            task = Task(
                id=tid,
                task_name=f"Multi Fencing Task {i}",
                task_type="noop",
                status=TaskStatus.QUEUED.value,
                priority=TaskPriority.HIGH.value,
                payload={"idx": i},
            )
            db.add(task)
            task_ids.append(tid)
            tokens_a[tid] = f"tok-A-{i}-{uuid.uuid4().hex[:6]}"
            tokens_b[tid] = f"tok-B-{i}-{uuid.uuid4().hex[:6]}"
        db.commit()

        # Step 1: Worker A acquires 1-second leases for all tasks
        for tid in task_ids:
            repo.acquire_lease(tid, worker_id="worker-A", lease_duration_seconds=1, lease_token=tokens_a[tid])
            repo.update_status(tid, TaskStatus.RUNNING, {"worker_id": "worker-A"}, lease_token=tokens_a[tid])

    # Step 2: Leases expire; Worker B reclaims all tasks
    time.sleep(1.1)
    with SessionLocal() as db:
        repo = TaskRepository(db)
        repo.recover_expired_leases()
        for tid in task_ids:
            repo.acquire_lease(tid, worker_id="worker-B", lease_duration_seconds=30, lease_token=tokens_b[tid])
            repo.update_status(tid, TaskStatus.RUNNING, {"worker_id": "worker-B"}, lease_token=tokens_b[tid])

    # Step 3: Concurrently execute stale commits from Worker A and valid commits from Worker B
    stale_commit_attempts = 0
    accepted_stale_commits = 0
    valid_commits_accepted = 0

    rejections_before = FENCING_REJECTIONS_TOTAL._value.get()
    failed_renewals_before = LEASE_RENEWALS_TOTAL.labels(status="failed")._value.get()

    def _worker_a_stale_action(tid: str):
        nonlocal stale_commit_attempts, accepted_stale_commits
        with SessionLocal() as db:
            r = TaskRepository(db)
            # Try stale renewal
            r.renew_lease(tid, lease_token=tokens_a[tid])
            # Try stale terminal commit
            stale_commit_attempts += 1
            try:
                r.complete_task_success(tid, lease_token=tokens_a[tid], return_value={"worker": "A"})
                accepted_stale_commits += 1
            except PermissionError:
                pass  # Fencing rejection expected

    def _worker_b_valid_action(tid: str):
        nonlocal valid_commits_accepted
        with SessionLocal() as db:
            r = TaskRepository(db)
            r.complete_task_success(tid, lease_token=tokens_b[tid], return_value={"worker": "B"})
            valid_commits_accepted += 1

    with ThreadPoolExecutor(max_workers=10) as executor:
        # Run stale actions and valid actions concurrently
        a_futures = [executor.submit(_worker_a_stale_action, tid) for tid in task_ids]
        b_futures = [executor.submit(_worker_b_valid_action, tid) for tid in task_ids]

        for f in as_completed(a_futures + b_futures):
            f.result()

    # Step 4: Verify Accounting Invariants
    rejections_after = FENCING_REJECTIONS_TOTAL._value.get()
    failed_renewals_after = LEASE_RENEWALS_TOTAL.labels(status="failed")._value.get()

    assert stale_commit_attempts == num_tasks, f"Expected {num_tasks} stale commit attempts, got {stale_commit_attempts}"
    assert accepted_stale_commits == 0, f"Accepted stale commits must be 0, got {accepted_stale_commits}"
    assert valid_commits_accepted == num_tasks, f"Expected {num_tasks} valid commits, got {valid_commits_accepted}"

    # Fencing rejections incremented by exactly 10
    assert rejections_after == rejections_before + num_tasks
    # Failed renewals incremented by exactly 10
    assert failed_renewals_after == failed_renewals_before + num_tasks

    # Verify final database state for all 10 tasks
    with SessionLocal() as db:
        repo = TaskRepository(db)
        for tid in task_ids:
            t = repo.get_by_id(tid)
            assert t.status == TaskStatus.SUCCESS.value
            assert t.lease_token is None  # Released cleanly on completion
            res = db.scalars(select(TaskResult).where(TaskResult.task_id == tid)).first()
            assert res is not None
            assert res.return_value == {"worker": "B"}, "Authoritative result must strictly be from Worker B"
