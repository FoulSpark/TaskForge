import uuid

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from taskforge.api.schemas import TaskCreateRequest
from taskforge.database.repositories.task_repository import TaskRepository
from taskforge.database.session import get_db
from taskforge.queue.broker import get_broker
from taskforge.tasks.service import TaskService

router = APIRouter(prefix="/chaos", tags=["Chaos Engineering"])


@router.post("/batch", summary="Submit a mixed batch of computational tasks")
def submit_batch(
    count: int = Query(10, ge=1, le=500, description="Number of tasks to generate"),
    db: Session = Depends(get_db),
):
    service = TaskService(db)
    workload_types = [
        "json_transform",
        "compute_prime",
        "io_simulation",
        "db_process",
        "long_running",
    ]
    created_ids = []

    for i in range(count):
        w_type = workload_types[i % len(workload_types)]
        payload = {}
        if w_type == "json_transform":
            payload = {"records": [x for x in range(i + 1, i + 10)]}
        elif w_type == "compute_prime":
            payload = {"n": 500 + (i * 50)}
        elif w_type == "io_simulation":
            payload = {"delay_seconds": 0.05, "jitter": True}
        elif w_type == "db_process":
            payload = {"batch_size": 25}
        elif w_type == "long_running":
            payload = {"steps": 3, "step_duration": 0.02}

        req = TaskCreateRequest(
            task_name=f"Batch {w_type.capitalize()} #{i + 1}",
            task_type=w_type,
            payload=payload,
            priority=(i % 4) + 1,
            timeout_seconds=30,
            max_retries=3,
        )
        task, _ = service.submit_task(req)
        created_ids.append(task.id)

    return {
        "batch_size": count,
        "task_ids": created_ids,
        "message": f"Successfully queued {count} tasks across {len(workload_types)} workload types.",
    }


@router.post("/fail-task", summary="Submit a task that fails to test retries and DLQ")
def fail_task(
    fail_until_attempt: int = Query(
        999, description="Attempt number after which task succeeds, or 999 to exhaust all retries"
    ),
    max_retries: int = Query(3, ge=0, le=5),
    db: Session = Depends(get_db),
):
    service = TaskService(db)
    req = TaskCreateRequest(
        task_name="Chaos Failure Test",
        task_type="failure_simulation",
        payload={
            "fail_until_attempt": fail_until_attempt,
            "error_message": "Chaos injection: artificial upstream service 503 outage",
        },
        max_retries=max_retries,
    )
    task, _ = service.submit_task(req)
    return {
        "task_id": task.id,
        "max_retries": max_retries,
        "fail_until_attempt": fail_until_attempt,
        "expected_outcome": "RETRY_WAIT then DEAD_LETTER"
        if fail_until_attempt > max_retries
        else "SUCCESS after retries",
    }


@router.post("/timeout-task", summary="Submit a task with an intentional timeout violation")
def timeout_task(
    timeout_seconds: int = Query(1, ge=1, le=5),
    sleep_seconds: float = Query(3.0, ge=1.0, le=10.0),
    db: Session = Depends(get_db),
):
    service = TaskService(db)
    req = TaskCreateRequest(
        task_name="Chaos Timeout Test",
        task_type="timeout_simulation",
        payload={"sleep_seconds": sleep_seconds},
        timeout_seconds=timeout_seconds,
        max_retries=1,
    )
    task, _ = service.submit_task(req)
    return {
        "task_id": task.id,
        "timeout_seconds": timeout_seconds,
        "sleep_seconds": sleep_seconds,
        "expected_outcome": "Timeout violation captured in attempt history",
    }


@router.post(
    "/idempotency-storm", summary="Execute 100 duplicate submissions to prove deduplication"
)
def idempotency_storm(
    count: int = Query(100, ge=10, le=200),
    db: Session = Depends(get_db),
):
    service = TaskService(db)
    storm_key = f"storm-{uuid.uuid4().hex[:8]}"

    created_task_id = None
    created_count = 0
    dedup_count = 0

    storm_payload = {"key": storm_key, "test": "idempotency_storm"}
    for _ in range(count):
        req = TaskCreateRequest(
            task_name="Idempotency Storm Task",
            task_type="json_transform",
            payload=storm_payload,
            idempotency_key=storm_key,
        )
        task, is_created = service.submit_task(req)
        if is_created:
            created_count += 1
            created_task_id = task.id
        else:
            dedup_count += 1

    return {
        "idempotency_key": storm_key,
        "total_requests": count,
        "tasks_created": created_count,
        "duplicate_requests_absorbed": dedup_count,
        "resulting_task_id": created_task_id,
        "verified": created_count == 1 and dedup_count == (count - 1),
    }


@router.post("/recover-stale", summary="Trigger stale lease recovery sweep")
def recover_stale_leases(db: Session = Depends(get_db)):
    repo = TaskRepository(db)
    recovered = repo.recover_expired_leases(initiator="operator")
    broker = get_broker()
    for t in recovered:
        if t.status == "RETRY_WAIT":
            broker.enqueue(t.id, t.priority, t.payload)

    return {
        "recovered_count": len(recovered),
        "recovered_tasks": [{"id": t.id, "status": t.status} for t in recovered],
    }
