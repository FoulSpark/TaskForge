from datetime import datetime, timedelta, timezone
from typing import Any, List, Optional, Tuple

from sqlalchemy import desc, func, select
from sqlalchemy.orm import Session, selectinload

from taskforge.core.state_machine import (
    TaskPriority,
    TaskStatus,
    can_cancel_task,
    validate_transition,
)
from taskforge.database.models import (
    OutboxEvent,
    Task,
    TaskAttempt,
    TaskEvent,
    TaskResult,
    get_utc_now,
)


class TaskRepository:
    def __init__(self, db: Session):
        self.db = db

    def get_by_id(self, task_id: str) -> Optional[Task]:
        stmt = (
            select(Task)
            .where(Task.id == task_id)
            .options(
                selectinload(Task.attempts),
                selectinload(Task.result),
                selectinload(Task.events),
            )
        )
        return self.db.scalars(stmt).first()

    def get_by_idempotency_key(self, idempotency_key: str) -> Optional[Task]:
        if not idempotency_key:
            return None
        stmt = (
            select(Task)
            .where(Task.idempotency_key == idempotency_key)
            .options(
                selectinload(Task.attempts),
                selectinload(Task.result),
                selectinload(Task.events),
            )
        )
        return self.db.scalars(stmt).first()

    def create_task(
        self,
        task_name: str,
        task_type: str,
        payload: dict[str, Any],
        priority: int = TaskPriority.MEDIUM.value,
        idempotency_key: Optional[str] = None,
        timeout_seconds: int = 60,
        max_retries: int = 3,
        scheduled_at: Optional[datetime] = None,
    ) -> Task:
        # Check idempotency first
        if idempotency_key:
            existing = self.get_by_idempotency_key(idempotency_key)
            if existing:
                return existing

        now = get_utc_now()
        task = Task(
            task_name=task_name,
            task_type=task_type,
            payload=payload,
            status=TaskStatus.PENDING.value,
            priority=priority,
            idempotency_key=idempotency_key,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            retry_count=0,
            scheduled_at=scheduled_at,
            created_at=now,
            updated_at=now,
        )
        self.db.add(task)
        self.db.flush()

        # Record initial event
        event = TaskEvent(
            task_id=task.id,
            event_type="TASK_CREATED",
            from_state=None,
            to_state=TaskStatus.PENDING.value,
            metadata_json={"task_name": task_name, "priority": priority},
            created_at=now,
        )
        self.db.add(event)

        # Create transactional outbox entry
        outbox = OutboxEvent(
            aggregate_type="task",
            aggregate_id=task.id,
            event_type="TASK_SUBMITTED",
            payload={
                "task_id": task.id,
                "task_name": task_name,
                "task_type": task_type,
                "payload": payload,
                "priority": priority,
                "timeout_seconds": timeout_seconds,
                "max_retries": max_retries,
            },
            status="PENDING",
            created_at=now,
        )
        self.db.add(outbox)

        # Automatically transition to QUEUED once staged in outbox
        validate_transition(task.status, TaskStatus.QUEUED)
        task.status = TaskStatus.QUEUED.value
        task.updated_at = now

        queued_event = TaskEvent(
            task_id=task.id,
            event_type="STATUS_CHANGED",
            from_state=TaskStatus.PENDING.value,
            to_state=TaskStatus.QUEUED.value,
            metadata_json={"reason": "enqueued_to_outbox"},
            created_at=now,
        )
        self.db.add(queued_event)

        self.db.commit()
        return self.get_by_id(task.id)  # type: ignore

    def list_tasks(
        self,
        status: Optional[str] = None,
        task_type: Optional[str] = None,
        priority: Optional[int] = None,
        limit: int = 20,
        offset: int = 0,
    ) -> Tuple[List[Task], int]:
        stmt = select(Task)
        count_stmt = select(func.count(Task.id))

        if status:
            stmt = stmt.where(Task.status == status)
            count_stmt = count_stmt.where(Task.status == status)
        if task_type:
            stmt = stmt.where(Task.task_type == task_type)
            count_stmt = count_stmt.where(Task.task_type == task_type)
        if priority is not None:
            stmt = stmt.where(Task.priority == priority)
            count_stmt = count_stmt.where(Task.priority == priority)

        total = self.db.scalar(count_stmt) or 0

        stmt = (
            stmt.order_by(desc(Task.priority), desc(Task.created_at))
            .offset(offset)
            .limit(limit)
            .options(
                selectinload(Task.attempts),
                selectinload(Task.result),
                selectinload(Task.events),
            )
        )
        tasks = list(self.db.scalars(stmt).all())
        return tasks, total

    def update_status(
        self,
        task_id: str,
        new_status: TaskStatus | str,
        metadata: Optional[dict[str, Any]] = None,
    ) -> Task:
        task = self.get_by_id(task_id)
        if not task:
            raise ValueError(f"Task with id {task_id} not found")

        curr_status = TaskStatus(task.status)
        nxt_status = TaskStatus(new_status)

        validate_transition(curr_status, nxt_status)

        now = get_utc_now()
        task.status = nxt_status.value
        task.updated_at = now

        if nxt_status == TaskStatus.RUNNING and not task.started_at:
            task.started_at = now
        elif nxt_status in {TaskStatus.SUCCESS, TaskStatus.DEAD_LETTER, TaskStatus.CANCELLED, TaskStatus.FAILED}:
            task.completed_at = now

        event = TaskEvent(
            task_id=task.id,
            event_type="STATUS_CHANGED",
            from_state=curr_status.value,
            to_state=nxt_status.value,
            metadata_json=metadata or {},
            created_at=now,
        )
        self.db.add(event)
        self.db.commit()
        self.db.refresh(task)

        # Invalidate read cache
        try:
            from taskforge.core.cache import get_cache
            get_cache().invalidate(task_id)
        except Exception:
            pass

        return task

    def cancel_task(self, task_id: str, reason: str = "user_cancelled") -> Task:
        task = self.get_by_id(task_id)
        if not task:
            raise ValueError(f"Task with id {task_id} not found")

        if not can_cancel_task(task.status):
            raise ValueError(f"Task in status {task.status} cannot be cancelled")

        return self.update_status(task_id, TaskStatus.CANCELLED, {"reason": reason})

    def record_attempt(
        self,
        task_id: str,
        attempt_number: int,
        worker_id: Optional[str],
        status: str,
        error_message: Optional[str] = None,
        error_traceback: Optional[str] = None,
        duration_ms: Optional[float] = None,
        started_at: Optional[datetime] = None,
        completed_at: Optional[datetime] = None,
    ) -> TaskAttempt:
        now = get_utc_now()
        attempt = TaskAttempt(
            task_id=task_id,
            attempt_number=attempt_number,
            worker_id=worker_id,
            status=status,
            error_message=error_message,
            error_traceback=error_traceback,
            started_at=started_at or now,
            completed_at=completed_at,
            duration_ms=duration_ms,
        )
        self.db.add(attempt)
        self.db.commit()
        return attempt

    def record_result(
        self,
        task_id: str,
        return_value: Optional[dict[str, Any]],
        execution_metadata: Optional[dict[str, Any]] = None,
    ) -> TaskResult:
        now = get_utc_now()
        result = TaskResult(
            task_id=task_id,
            return_value=return_value,
            execution_metadata=execution_metadata or {},
            created_at=now,
        )
        self.db.add(result)
        self.db.commit()
        return result

    def acquire_lease(
        self, task_id: str, worker_id: str, lease_duration_seconds: int = 30
    ) -> bool:
        stmt = select(Task).where(Task.id == task_id).with_for_update()
        task = self.db.scalars(stmt).first()
        if not task:
            return False

        now = get_utc_now()
        if task.lease_expires_at is not None:
            exp = task.lease_expires_at
            if exp.tzinfo is None:
                exp = exp.replace(tzinfo=timezone.utc)
            if exp >= now and task.lease_token != worker_id:
                return False

        expires_at = now + timedelta(seconds=lease_duration_seconds)
        task.lease_token = worker_id
        task.lease_expires_at = expires_at
        task.updated_at = now
        self.db.commit()
        return True

    def renew_lease(
        self, task_id: str, worker_id: str, lease_duration_seconds: int = 30
    ) -> bool:
        now = get_utc_now()
        expires_at = now + timedelta(seconds=lease_duration_seconds)
        stmt = (
            select(Task)
            .where(Task.id == task_id)
            .where(Task.lease_token == worker_id)
            .with_for_update()
        )
        task = self.db.scalars(stmt).first()
        if not task:
            return False

        task.lease_expires_at = expires_at
        task.updated_at = now
        self.db.commit()
        return True

    def release_lease(self, task_id: str, worker_id: str) -> bool:
        stmt = select(Task).where(Task.id == task_id).where(Task.lease_token == worker_id)
        task = self.db.scalars(stmt).first()
        if not task:
            return False

        task.lease_token = None
        task.lease_expires_at = None
        task.updated_at = get_utc_now()
        self.db.commit()
        return True

    def recover_expired_leases(self) -> List[Task]:
        """Find RUNNING tasks whose lease has expired and transition them to RETRY_WAIT or QUEUED."""
        now = get_utc_now()
        stmt = (
            select(Task)
            .where(Task.status == TaskStatus.RUNNING.value)
            .where(Task.lease_expires_at < now)
        )
        expired_tasks = list(self.db.scalars(stmt).all())
        recovered = []
        for task in expired_tasks:
            task.retry_count += 1
            if task.retry_count <= task.max_retries:
                task.status = TaskStatus.RETRY_WAIT.value
                event_reason = f"lease_expired_recovered_for_retry_{task.retry_count}"
            else:
                task.status = TaskStatus.DEAD_LETTER.value
                event_reason = "lease_expired_retries_exhausted_sent_to_dlq"

            task.lease_token = None
            task.lease_expires_at = None
            task.updated_at = now

            event = TaskEvent(
                task_id=task.id,
                event_type="LEASE_EXPIRED_RECOVERED",
                from_state=TaskStatus.RUNNING.value,
                to_state=task.status,
                metadata_json={"reason": event_reason},
                created_at=now,
            )
            self.db.add(event)
            recovered.append(task)

        if recovered:
            self.db.commit()
        return recovered
