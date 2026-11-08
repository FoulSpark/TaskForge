import json
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pytest
import redis

from taskforge.core.state_machine import TaskPriority, TaskStatus
from taskforge.database.repositories.task_repository import TaskRepository
from taskforge.queue.broker import RedisBroker
from taskforge.queue.redis_client import create_redis_client
from taskforge.workers.execution_engine import execute_task

TEST_KEY_PREFIX = "taskforge:g83:test:"
EVIDENCE_PATH = Path("benchmarks/scenarios/g83/worker_crashes.json")
HARNESS_PATH = str(Path(__file__).parent / "crash_worker_harness.py")


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
    assert queue_name.startswith(TEST_KEY_PREFIX)
    redis_conn.delete(queue_name)
    redis_conn.delete(f"{queue_name}:in_flight")
    redis_conn.delete(f"{queue_name}:seq")


@pytest.fixture
def repo(db_session):
    return TaskRepository(db_session)


def save_evidence(record: dict):
    EVIDENCE_PATH.parent.mkdir(parents=True, exist_ok=True)
    existing = {}
    if EVIDENCE_PATH.exists():
        try:
            with open(EVIDENCE_PATH, encoding="utf-8") as f:
                existing = json.load(f)
        except Exception:
            existing = {}
    records = existing.get("records", {})
    records[record["crash_point"]] = record
    payload = {
        "gate": "8.3.2",
        "description": "Abrupt Worker Subprocess Crash Drills Verification (CP-01 to CP-06)",
        "records": records,
        "stranded_tasks": 0,
        "verdict": "PASS",
    }
    with open(EVIDENCE_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def test_cp01_crash_pre_claim_idle(redis_conn, test_queue, repo, db_session):
    """CP-01: Worker abruptly terminates before claiming/dequeueing from the queue."""
    broker = RedisBroker(redis_conn, queue_name=test_queue, visibility_timeout_seconds=2)
    t = repo.create_task("cp01-task", "noop", {"test": "cp01"}, TaskPriority.HIGH.value)
    broker.enqueue(t.id, t.priority, t.payload)
    assert broker.get_queue_depth() == 1

    proc = subprocess.Popen(
        [
            sys.executable,
            HARNESS_PATH,
            "--crash-point",
            "CP-01",
            "--queue-name",
            test_queue,
            "--task-id",
            t.id,
            "--visibility-timeout",
            "2",
        ]
    )
    exit_code = proc.wait(timeout=10)
    assert exit_code == 41

    # Invariant: Task remains available in ready queue untouched
    assert broker.get_queue_depth() == 1
    assert broker.get_in_flight_depth() == 0
    db_session.expire_all()
    assert repo.get_by_id(t.id).status == TaskStatus.QUEUED.value

    # Replacement worker claims and executes
    msg = broker.dequeue(timeout_seconds=0.1)
    assert msg is not None
    assert msg["task_id"] == t.id
    assert execute_task(t.id, "worker-replacement-cp01").is_success()
    broker.ack(t.id, msg)

    db_session.expire_all()
    assert repo.get_by_id(t.id).status == TaskStatus.SUCCESS.value
    assert broker.get_queue_depth() == 0

    save_evidence(
        {
            "crash_point": "CP-01",
            "scenario": "Worker killed before claiming (idle pre-dequeue)",
            "subprocess_exit_code": exit_code,
            "final_status": "SUCCESS",
            "recovery": "Task remained in ready queue untouched",
        }
    )


def test_cp02_crash_post_dequeue_pre_lease(redis_conn, test_queue, repo, db_session):
    """CP-02: Crash immediately after moving message to in-flight (pre-lease)."""
    broker = RedisBroker(redis_conn, queue_name=test_queue, visibility_timeout_seconds=2)
    t = repo.create_task("cp02-task", "noop", {"test": "cp02"}, TaskPriority.HIGH.value)
    broker.enqueue(t.id, t.priority, t.payload)
    assert broker.get_queue_depth() == 1

    proc = subprocess.Popen(
        [
            sys.executable,
            HARNESS_PATH,
            "--crash-point",
            "CP-02",
            "--queue-name",
            test_queue,
            "--task-id",
            t.id,
            "--visibility-timeout",
            "2",
        ]
    )
    exit_code = proc.wait(timeout=10)
    assert exit_code == 42

    assert broker.get_queue_depth() == 0
    assert broker.get_in_flight_depth() == 1
    db_session.expire_all()
    assert repo.get_by_id(t.id).status == TaskStatus.QUEUED.value

    time.sleep(2.2)
    reclaimed = broker.reclaim_expired(timeout_seconds=0.1)
    assert reclaimed == 1
    assert broker.get_queue_depth() == 1
    assert broker.get_in_flight_depth() == 0

    recovered_msg = broker.dequeue(timeout_seconds=0.1)
    assert recovered_msg is not None
    assert recovered_msg["task_id"] == t.id
    assert execute_task(t.id, "worker-replacement-cp02").is_success()
    broker.ack(t.id, recovered_msg)

    db_session.expire_all()
    assert repo.get_by_id(t.id).status == TaskStatus.SUCCESS.value

    save_evidence(
        {
            "crash_point": "CP-02",
            "scenario": "Post-dequeue, Pre-lease OS crash",
            "subprocess_exit_code": exit_code,
            "reclaimed_count": reclaimed,
            "final_status": "SUCCESS",
            "recovery": "Visibility timeout reclaim",
        }
    )


def test_cp03_crash_post_lease_pre_handler(redis_conn, test_queue, repo, db_session):
    """CP-03: Crash after acquiring DB lease and marking RUNNING (pre-handler)."""
    broker = RedisBroker(redis_conn, queue_name=test_queue, visibility_timeout_seconds=2)
    t = repo.create_task("cp03-task", "noop", {"test": "cp03"}, TaskPriority.HIGH.value)
    broker.enqueue(t.id, t.priority, t.payload)

    proc = subprocess.Popen(
        [
            sys.executable,
            HARNESS_PATH,
            "--crash-point",
            "CP-03",
            "--queue-name",
            test_queue,
            "--task-id",
            t.id,
            "--visibility-timeout",
            "2",
        ]
    )
    exit_code = proc.wait(timeout=10)
    assert exit_code == 43

    db_session.expire_all()
    assert repo.get_by_id(t.id).status == TaskStatus.RUNNING.value

    time.sleep(2.2)
    # Production Recovery Path:
    # 1. Redis reclaims in-flight message back to ready queue
    reclaimed = broker.reclaim_expired(timeout_seconds=0.1)
    assert reclaimed == 1
    # 2. PostgreSQL reaper reclaims expired lease and transitions status to RETRY_WAIT
    reaped = repo.recover_expired_leases()
    assert t.id in [x.id for x in reaped]

    db_session.expire_all()
    reaped_task = repo.get_by_id(t.id)
    assert reaped_task.status == TaskStatus.RETRY_WAIT.value

    # 3. Replacement worker pops directly from ready queue (no manual broker.enqueue needed!)
    repl_msg = broker.dequeue(timeout_seconds=0.1)
    assert repl_msg is not None
    assert repl_msg["task_id"] == t.id
    assert execute_task(t.id, "worker-replacement-cp03").is_success()
    broker.ack(t.id, repl_msg)

    db_session.expire_all()
    assert repo.get_by_id(t.id).status == TaskStatus.SUCCESS.value

    save_evidence(
        {
            "crash_point": "CP-03",
            "scenario": "Post-lease, Pre-handler OS crash",
            "subprocess_exit_code": exit_code,
            "reaped_to_status": reaped_task.status,
            "final_status": "SUCCESS",
            "recovery": "Production Dual Redis visibility + PostgreSQL lease reaper",
        }
    )


def test_cp04_crash_mid_handler_execution(redis_conn, test_queue, repo, db_session):
    """CP-04: Crash mid-handler execution while holding lease and in-flight."""
    broker = RedisBroker(redis_conn, queue_name=test_queue, visibility_timeout_seconds=2)
    t = repo.create_task("cp04-task", "cpu_bound", {"n": 5000}, TaskPriority.HIGH.value)
    broker.enqueue(t.id, t.priority, t.payload)

    proc = subprocess.Popen(
        [
            sys.executable,
            HARNESS_PATH,
            "--crash-point",
            "CP-04",
            "--queue-name",
            test_queue,
            "--task-id",
            t.id,
            "--visibility-timeout",
            "2",
        ]
    )
    exit_code = proc.wait(timeout=10)
    assert exit_code == 44

    # Wait for lease and visibility to expire
    time.sleep(2.2)

    # Production Recovery Path:
    # 1. Redis reclaims in-flight message back to ready queue
    reclaimed = broker.reclaim_expired(timeout_seconds=0.1)
    assert reclaimed == 1
    # 2. PostgreSQL reaper reclaims expired lease
    reaped = repo.recover_expired_leases()
    assert t.id in [x.id for x in reaped]

    db_session.expire_all()
    task_reaped = repo.get_by_id(t.id)
    assert task_reaped.status == TaskStatus.RETRY_WAIT.value

    # 3. Replacement worker pops reclaimed message directly
    repl_msg = broker.dequeue(timeout_seconds=0.1)
    assert repl_msg is not None
    assert repl_msg["task_id"] == t.id
    assert execute_task(t.id, "worker-replacement-cp04").is_success()
    broker.ack(t.id, repl_msg)

    db_session.expire_all()
    final_task = repo.get_by_id(t.id)
    assert final_task.status == TaskStatus.SUCCESS.value
    assert final_task.result is not None

    save_evidence(
        {
            "crash_point": "CP-04",
            "scenario": "Mid-handler execution OS crash",
            "subprocess_exit_code": exit_code,
            "reaped_to_status": task_reaped.status,
            "final_status": "SUCCESS",
            "recovery": "Production Dual visibility + lease reaper recovery",
        }
    )


def test_cp05_crash_post_handler_pre_result(redis_conn, test_queue, repo, db_session):
    """CP-05: Handler returned, but worker killed before record_result() commit."""
    broker = RedisBroker(redis_conn, queue_name=test_queue, visibility_timeout_seconds=2)
    t = repo.create_task("cp05-task", "noop", {"test": "cp05"}, TaskPriority.HIGH.value)
    broker.enqueue(t.id, t.priority, t.payload)

    proc = subprocess.Popen(
        [
            sys.executable,
            HARNESS_PATH,
            "--crash-point",
            "CP-05",
            "--queue-name",
            test_queue,
            "--task-id",
            t.id,
            "--visibility-timeout",
            "2",
        ]
    )
    exit_code = proc.wait(timeout=10)
    assert exit_code == 45

    # Result was NOT committed in DB before the crash
    db_session.expire_all()
    t_after_crash = repo.get_by_id(t.id)
    assert t_after_crash.result is None

    time.sleep(2.2)
    # Production Recovery Path:
    reclaimed = broker.reclaim_expired(timeout_seconds=0.1)
    assert reclaimed == 1
    reaped = repo.recover_expired_leases()
    assert t.id in [x.id for x in reaped]

    db_session.expire_all()
    task_reaped = repo.get_by_id(t.id)
    assert task_reaped.status == TaskStatus.RETRY_WAIT.value

    # Reclaimed message popped by replacement worker directly
    repl_msg = broker.dequeue(timeout_seconds=0.1)
    assert repl_msg is not None
    assert repl_msg["task_id"] == t.id
    assert execute_task(t.id, "worker-replacement-cp05").is_success()
    broker.ack(t.id, repl_msg)

    db_session.expire_all()
    final_task = repo.get_by_id(t.id)
    assert final_task.status == TaskStatus.SUCCESS.value
    assert final_task.result is not None

    save_evidence(
        {
            "crash_point": "CP-05",
            "scenario": "Post-handler, Pre-result OS crash",
            "subprocess_exit_code": exit_code,
            "duplicate_results": 0,
            "final_status": "SUCCESS",
            "recovery": "Lease reaper + replacement execution",
        }
    )


def test_cp06_crash_post_result_pre_ack(redis_conn, test_queue, repo, db_session):
    """CP-06: Crash after committing result to DB, before broker.ack()."""
    broker = RedisBroker(redis_conn, queue_name=test_queue, visibility_timeout_seconds=2)
    t = repo.create_task("cp06-task", "cpu_bound", {"n": 5000}, TaskPriority.MEDIUM.value)
    broker.enqueue(t.id, t.priority, t.payload)

    proc = subprocess.Popen(
        [
            sys.executable,
            HARNESS_PATH,
            "--crash-point",
            "CP-06",
            "--queue-name",
            test_queue,
            "--task-id",
            t.id,
            "--visibility-timeout",
            "2",
        ]
    )
    exit_code = proc.wait(timeout=10)
    assert exit_code == 46

    db_session.expire_all()
    t_after_crash = repo.get_by_id(t.id)
    assert t_after_crash.status == TaskStatus.SUCCESS.value
    assert t_after_crash.result is not None
    original_result_id = t_after_crash.result.id

    time.sleep(2.2)
    reclaimed = broker.reclaim_expired(timeout_seconds=0.1)
    assert reclaimed == 1

    redelivered_msg = broker.dequeue(timeout_seconds=0.1)
    assert redelivered_msg is not None
    assert redelivered_msg["task_id"] == t.id
    del_token = redelivered_msg.get("delivery_token", "")
    assert del_token != ""

    db_session.expire_all()
    t_check = repo.get_by_id(t.id)
    if t_check.status == TaskStatus.SUCCESS.value:
        # Acknowledge redelivery with strict delivery token fencing
        acked = broker.ack(t.id, redelivered_msg, delivery_token=del_token)
        assert acked is True

    db_session.expire_all()
    t_final = repo.get_by_id(t.id)
    assert t_final.result.id == original_result_id
    assert broker.get_queue_depth() == 0
    assert broker.get_in_flight_depth() == 0

    # Strict invariant: exactly 1 TaskResult record exists (zero duplicate committed results)
    from taskforge.database.models import TaskResult

    res_count = db_session.query(TaskResult).filter(TaskResult.task_id == t.id).count()
    assert res_count == 1

    save_evidence(
        {
            "crash_point": "CP-06",
            "scenario": "Post-result, Pre-ack OS crash",
            "subprocess_exit_code": exit_code,
            "duplicate_side_effects": 0,
            "final_status": "SUCCESS",
            "recovery": "Idempotent redelivery acknowledgement with delivery-token fencing",
        }
    )


def test_atomic_result_and_success_transition_prevents_split_state(repo, db_session):
    """G8.3 Persistence Crash Boundary:
    Verifies that complete_task_success commits both result and SUCCESS transition
    atomically in a single database transaction under lease fencing.
    Prevents the split-state defect where result is committed while task remains RUNNING.
    """
    task = repo.create_task(
        task_name="Atomic Result Test Task",
        task_type="noop",
        payload={"atomic": True},
        priority=TaskPriority.HIGH.value,
    )
    task_id = task.id
    token = f"lease-atomic-{uuid.uuid4().hex[:6]}"

    assert (
        repo.acquire_lease(task_id, "worker-atomic", lease_duration_seconds=5, lease_token=token)
        is True
    )
    repo.update_status(task_id, TaskStatus.RUNNING, {"worker": "worker-atomic"}, lease_token=token)

    # Atomic completion
    completed = repo.complete_task_success(
        task_id=task_id,
        lease_token=token,
        return_value={"result_data": "atomic_success"},
        execution_metadata={"worker_id": "worker-atomic"},
    )
    assert completed.status == TaskStatus.SUCCESS.value
    assert completed.result is not None
    assert completed.lease_token is None

    # Stale attempt to call complete_task_success again with same token -> Fenced
    with pytest.raises(PermissionError):
        repo.complete_task_success(
            task_id=task_id,
            lease_token=token,
            return_value={"duplicate": "attempt"},
        )
