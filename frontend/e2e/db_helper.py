"""Database helper for Playwright F2-E browser verification against live PostgreSQL 16."""

import json
import sys
import uuid
from datetime import UTC, datetime, timedelta

from sqlalchemy import delete, update

from taskforge.core.auth import ApiKeyService
from taskforge.core.state_machine import TaskPriority, TaskStatus
from taskforge.database.models import (
    ApiKey,
    OutboxEvent,
    Task,
    TaskAttempt,
    TaskEvent,
    TaskResult,
)
from taskforge.database.session import SessionLocal, init_db


def cmd_init_tenants() -> dict:
    init_db()
    suffix = uuid.uuid4().hex[:8]
    tenant_a = f"tenant-e2e-a-{suffix}"
    tenant_b = f"tenant-e2e-b-{suffix}"

    with SessionLocal() as db:
        svc = ApiKeyService(db)
        key_a_rw, rec_a_rw = svc.create_api_key(
            tenant_id=tenant_a,
            name="e2e-a-rw",
            scopes=["tasks:read", "tasks:write"],
        )
        key_a_ro, _ = svc.create_api_key(
            tenant_id=tenant_a,
            name="e2e-a-ro",
            scopes=["tasks:read"],
        )
        key_a_rev, rec_a_rev = svc.create_api_key(
            tenant_id=tenant_a,
            name="e2e-a-revokable",
            scopes=["tasks:read", "tasks:write"],
        )
        key_op, _ = svc.create_api_key(
            tenant_id=tenant_a,
            name="e2e-operator",
            scopes=["tasks:read", "tasks:write", "admin:operator"],
        )
        key_b_rw, _ = svc.create_api_key(
            tenant_id=tenant_b,
            name="e2e-b-rw",
            scopes=["tasks:read", "tasks:write"],
        )

    return {
        "tenant_a": tenant_a,
        "tenant_b": tenant_b,
        "key_a_rw": key_a_rw,
        "key_a_rw_id": rec_a_rw.key_id,
        "key_a_ro": key_a_ro,
        "key_a_rev": key_a_rev,
        "key_a_rev_id": rec_a_rev.key_id,
        "key_op": key_op,
        "key_b_rw": key_b_rw,
    }


def cmd_revoke_key(key_id: str) -> dict:
    with SessionLocal() as db:
        svc = ApiKeyService(db)
        ok = svc.revoke_api_key(key_id)
    return {"revoked": ok, "key_id": key_id}


def cmd_seed_failed_task(tenant_id: str) -> dict:
    tid = str(uuid.uuid4())
    now = datetime.now(UTC)
    with SessionLocal() as db:
        task = Task(
            id=tid,
            task_name="E2E Retryable Failed Task",
            task_type="compute_prime",
            payload={"n": 20},
            status=TaskStatus.FAILED.value,
            priority=TaskPriority.HIGH.value,
            tenant_id=tenant_id,
            retry_count=1,
            max_retries=1,
            timeout_seconds=30,
            created_at=now - timedelta(seconds=10),
            updated_at=now - timedelta(seconds=5),
            started_at=now - timedelta(seconds=8),
            completed_at=now - timedelta(seconds=5),
        )
        db.add(task)
        db.flush()
        db.add(
            TaskAttempt(
                task_id=tid,
                attempt_number=1,
                worker_id="e2e-seed-worker",
                status="FAILED",
                error_message="Simulated transient fault before manual retry",
                started_at=now - timedelta(seconds=8),
                completed_at=now - timedelta(seconds=5),
                duration_ms=14.5,
            )
        )
        db.add(
            TaskEvent(
                task_id=tid,
                event_type="TASK_FAILED",
                from_state="RUNNING",
                to_state="FAILED",
                metadata_json={"reason": "simulated_e2e_failure"},
                created_at=now - timedelta(seconds=5),
            )
        )
        db.commit()
    return {"task_id": tid}


def cmd_seed_explorer_tasks(tenant_a: str, tenant_b: str) -> dict:
    now = datetime.now(UTC)
    with SessionLocal() as db:
        for i in range(12):
            tid = str(uuid.uuid4())
            is_failed = i >= 10
            db.add(
                Task(
                    id=tid,
                    task_name=f"E2E Explorer Job {i:02d}",
                    task_type="compute_prime" if i % 2 == 0 else "noop",
                    payload={"n": 15} if i % 2 == 0 else {},
                    status=TaskStatus.FAILED.value if is_failed else TaskStatus.SUCCESS.value,
                    priority=TaskPriority.HIGH.value if is_failed else TaskPriority.MEDIUM.value,
                    tenant_id=tenant_a,
                    created_at=now - timedelta(minutes=20 - i),
                    updated_at=now - timedelta(minutes=20 - i),
                    started_at=now - timedelta(minutes=20 - i),
                    completed_at=now - timedelta(minutes=20 - i),
                )
            )
        b_id = str(uuid.uuid4())
        db.add(
            Task(
                id=b_id,
                task_name="Tenant B Private Secret Job",
                task_type="noop",
                payload={},
                status=TaskStatus.SUCCESS.value,
                priority=TaskPriority.CRITICAL.value,
                tenant_id=tenant_b,
                created_at=now,
                updated_at=now,
                started_at=now,
                completed_at=now,
            )
        )
        db.commit()
    return {"seeded_a": 12, "tenant_b_task_id": b_id}


def cmd_force_task_success(task_id: str) -> dict:
    now = datetime.now(UTC)
    with SessionLocal() as db:
        db.execute(
            update(Task)
            .where(Task.id == task_id)
            .values(status=TaskStatus.SUCCESS.value, completed_at=now, updated_at=now)
        )
        db.commit()
    return {"task_id": task_id, "status": "SUCCESS"}


def cmd_cleanup(tenant_a: str, tenant_b: str) -> dict:
    with SessionLocal() as db:
        task_ids = [
            row[0]
            for row in db.query(Task.id)
            .filter(Task.tenant_id.in_([tenant_a, tenant_b]))
            .all()
        ]
        if task_ids:
            db.execute(delete(TaskEvent).where(TaskEvent.task_id.in_(task_ids)))
            db.execute(delete(TaskAttempt).where(TaskAttempt.task_id.in_(task_ids)))
            db.execute(delete(TaskResult).where(TaskResult.task_id.in_(task_ids)))
            db.execute(delete(OutboxEvent).where(OutboxEvent.aggregate_id.in_(task_ids)))
            db.execute(delete(Task).where(Task.id.in_(task_ids)))
        db.execute(delete(ApiKey).where(ApiKey.tenant_id.in_([tenant_a, tenant_b])))
        db.commit()
    return {"cleaned_tasks": len(task_ids)}


def main() -> int:
    action = sys.argv[1]
    if action == "init-tenants":
        res = cmd_init_tenants()
    elif action == "revoke-key":
        res = cmd_revoke_key(sys.argv[2])
    elif action == "seed-failed":
        res = cmd_seed_failed_task(sys.argv[2])
    elif action == "seed-explorer":
        res = cmd_seed_explorer_tasks(sys.argv[2], sys.argv[3])
    elif action == "force-success":
        res = cmd_force_task_success(sys.argv[2])
    elif action == "cleanup":
        res = cmd_cleanup(sys.argv[2], sys.argv[3])
    else:
        raise ValueError(f"Unknown helper action: {action}")
    print(json.dumps(res))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

