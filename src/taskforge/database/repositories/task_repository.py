import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import desc, func, select
from sqlalchemy.exc import IntegrityError
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


@dataclass(frozen=True)
class RecoveredTaskInfo:
    """Immutable value object representing an expired task recovered by the lease reaper."""

    task_id: str
    status: str
    lease_token: str | None = None

    @property
    def id(self) -> str:
        return self.task_id


class CreateTaskResult(tuple):
    """Result of create_task returning (task, created).

    Supports unpacking (task, is_created = repo.create_task(...)),
    named property access (.task, .created),
    and transparent attribute delegation to the task instance for backward compatibility.
    """

    def __new__(cls, task: Task, created: bool):
        return super().__new__(cls, (task, created))

    @property
    def task(self) -> Task:
        return self[0]

    @property
    def created(self) -> bool:
        return self[1]

    def __getattr__(self, name: str) -> Any:
        return getattr(self[0], name)

    def __setattr__(self, name: str, value: Any) -> None:
        setattr(self[0], name, value)


class TaskRepository:
    def __init__(self, db: Session):
        self.db = db

    @staticmethod
    def _is_idempotency_key_violation(exc: IntegrityError) -> bool:
        """Inspects underlying database exception to determine if it is strictly
        a unique constraint violation on tasks.idempotency_key (SQLSTATE 23505).

        Enforces strict classification policy:
        1. Requires SQLSTATE 23505 (unique_violation).
        2. Requires the exact known PostgreSQL constraint name ('tasks_idempotency_key_key'
           or explicit schema name 'uq_tasks_idempotency_key').
        3. If PostgreSQL constraint name is unavailable or unverified, does not classify
           the error as an idempotency collision.
        4. Provides a separate, explicit dialect path for SQLite in-memory tests.
        """
        orig = getattr(exc, "orig", None)
        if orig is None:
            return False

        # PostgreSQL with psycopg 3 or psycopg2: SQLSTATE 23505 (unique_violation)
        sqlstate = getattr(orig, "sqlstate", None) or getattr(orig, "pgcode", None)
        if sqlstate is not None:
            if sqlstate != "23505":
                return False
            diag = getattr(orig, "diag", None)
            if diag:
                cname = getattr(diag, "constraint_name", None) or ""
                KNOWN_CONSTRAINTS = {"tasks_idempotency_key_key", "uq_tasks_idempotency_key"}
                if cname in KNOWN_CONSTRAINTS:
                    return True
                mdetail = getattr(diag, "message_detail", None) or ""
                if cname == "" and "Key (idempotency_key)=" in mdetail:
                    return True
            # PostgreSQL constraint name unverified; reject silent classification
            return False

        # SQLite dialect support for in-memory testing
        orig_str = str(orig).lower()
        if "unique constraint failed" in orig_str and "tasks.idempotency_key" in orig_str:
            return True

        return False

    def get_by_id(self, task_id: str) -> Task | None:
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

    def get_by_idempotency_key(self, idempotency_key: str) -> Task | None:
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
        idempotency_key: str | None = None,
        timeout_seconds: int = 60,
        max_retries: int = 3,
        scheduled_at: datetime | None = None,
    ) -> CreateTaskResult:
        # Check idempotency first (optimistic fast path)
        if idempotency_key:
            existing = self.get_by_idempotency_key(idempotency_key)
            if existing:
                return CreateTaskResult(existing, False)

        now = get_utc_now()
        task_id = str(uuid.uuid4())
        task = Task(
            id=task_id,
            task_name=task_name,
            task_type=task_type,
            payload=payload,
            status=TaskStatus.QUEUED.value,
            priority=priority,
            idempotency_key=idempotency_key,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            retry_count=0,
            scheduled_at=scheduled_at,
            created_at=now,
            updated_at=now,
        )

        try:
            self.db.add(task)

            # Record initial event
            event = TaskEvent(
                task_id=task_id,
                event_type="TASK_CREATED",
                from_state=None,
                to_state=TaskStatus.QUEUED.value,
                metadata_json={"task_name": task_name, "priority": priority},
                created_at=now,
            )
            self.db.add(event)

            # Create transactional outbox entry in the same transaction
            outbox = OutboxEvent(
                aggregate_type="task",
                aggregate_id=task_id,
                event_type="TASK_SUBMITTED",
                payload={
                    "task_id": task_id,
                    "task_name": task_name,
                    "task_type": task_type,
                    "payload": payload,
                    "priority": priority,
                    "timeout_seconds": timeout_seconds,
                    "max_retries": max_retries,
                    "scheduled_at": scheduled_at.isoformat() if scheduled_at else None,
                },
                status="PENDING",
                created_at=now,
            )
            self.db.add(outbox)

            self.db.commit()
            created_task = self.get_by_id(task.id)
            return CreateTaskResult(created_task, True)  # type: ignore
        except IntegrityError as exc:
            self.db.rollback()
            # Inspect underlying error to ensure it is specifically the uniqueness violation on idempotency_key
            if idempotency_key and self._is_idempotency_key_violation(exc):
                existing = self.get_by_idempotency_key(idempotency_key)
                if existing:
                    return CreateTaskResult(existing, False)
            raise
        except Exception:
            self.db.rollback()
            raise

    def list_tasks(
        self,
        status: str | None = None,
        task_type: str | None = None,
        priority: int | None = None,
        limit: int = 20,
        offset: int = 0,
    ) -> tuple[list[Task], int]:
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
            stmt.order_by(desc(Task.priority), desc(Task.created_at), desc(Task.id))
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
        metadata: dict[str, Any] | None = None,
        lease_token: str | None = None,
        force: bool = False,
    ) -> Task:
        stmt = select(Task).where(Task.id == task_id).with_for_update()
        task = self.db.scalars(stmt).first()
        if not task:
            raise ValueError(f"Task with id {task_id} not found")

        # Fencing: if task has an active lease, caller MUST supply a matching lease token
        if not force:
            if task.lease_token is not None:
                if not lease_token or task.lease_token != lease_token:
                    raise PermissionError(
                        f"Fencing violation: task is currently leased under '{task.lease_token}'; "
                        f"mutation requires matching lease_token, but received '{lease_token}'"
                    )
            elif lease_token is not None:
                raise PermissionError(
                    f"Fencing violation: caller provided lease token '{lease_token}', but task has no active lease"
                )

        curr_status = TaskStatus(task.status)
        nxt_status = TaskStatus(new_status)

        if not force:
            validate_transition(curr_status, nxt_status)

        now = get_utc_now()
        task.status = nxt_status.value
        task.updated_at = now

        if force:
            # Force overrides revoke active lease
            task.lease_token = None
            task.lease_expires_at = None

        if nxt_status == TaskStatus.RUNNING and not task.started_at:
            task.started_at = now
        elif nxt_status in {
            TaskStatus.SUCCESS,
            TaskStatus.DEAD_LETTER,
            TaskStatus.CANCELLED,
            TaskStatus.FAILED,
        }:
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
        worker_id: str | None,
        status: str,
        error_message: str | None = None,
        error_traceback: str | None = None,
        duration_ms: float | None = None,
        started_at: datetime | None = None,
        completed_at: datetime | None = None,
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
        return_value: dict[str, Any] | None,
        execution_metadata: dict[str, Any] | None = None,
        lease_token: str | None = None,
    ) -> TaskResult:
        stmt = select(Task).where(Task.id == task_id).with_for_update()
        task = self.db.scalars(stmt).first()
        if not task:
            raise ValueError(f"Task with id {task_id} not found")

        # Strict fencing: if task has active lease, caller must provide matching token
        if task.lease_token is not None:
            if not lease_token or task.lease_token != lease_token:
                raise PermissionError(
                    f"Fencing violation: task is currently leased under '{task.lease_token}'; "
                    f"cannot record result with lease_token '{lease_token}'"
                )
        elif lease_token is not None:
            raise PermissionError(
                f"Fencing violation: caller provided lease token '{lease_token}', but task has no active lease"
            )

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

    def complete_task_success(
        self,
        task_id: str,
        lease_token: str,
        return_value: dict[str, Any] | None,
        execution_metadata: dict[str, Any] | None = None,
        attempt_id: int | None = None,
        duration_ms: float | None = None,
    ) -> Task:
        """Atomically commit TaskResult, transition Task to SUCCESS,
        update TaskAttempt to SUCCESS, release lease, and record event in one transaction under strict lease fencing.
        """
        stmt = select(Task).where(Task.id == task_id).with_for_update()
        task = self.db.scalars(stmt).first()
        if not task:
            raise ValueError(f"Task with id {task_id} not found")

        if not lease_token or task.lease_token != lease_token:
            raise PermissionError(
                f"Fencing violation: lease token '{lease_token}' does not match active task lease '{task.lease_token}'"
            )

        curr_status = TaskStatus(task.status)
        validate_transition(curr_status, TaskStatus.SUCCESS)

        # Pre-validate attempt ownership and status before applying any mutations
        attempt = None
        if attempt_id is not None:
            attempt_stmt = select(TaskAttempt).where(
                TaskAttempt.id == attempt_id,
                TaskAttempt.task_id == task_id,
            )
            attempt = self.db.scalars(attempt_stmt).first()
            if not attempt:
                raise ValueError(
                    f"Attempt ownership violation: TaskAttempt '{attempt_id}' does not belong to task '{task_id}'."
                )
            if attempt.status not in ("RUNNING", "PENDING"):
                raise ValueError(
                    f"Attempt status violation: cannot complete TaskAttempt '{attempt_id}' in terminal status '{attempt.status}'."
                )

        try:
            now = get_utc_now()
            task.status = TaskStatus.SUCCESS.value
            task.completed_at = now
            task.updated_at = now
            task.lease_token = None
            task.lease_expires_at = None

            # 1. Persist result
            result = TaskResult(
                task_id=task_id,
                return_value=return_value,
                execution_metadata=execution_metadata or {},
                created_at=now,
            )
            self.db.add(result)

            # 2. Update attempt if provided
            if attempt:
                attempt.status = "SUCCESS"
                attempt.completed_at = now
                attempt.duration_ms = duration_ms

            # 3. Record status change event
            event = TaskEvent(
                task_id=task.id,
                event_type="STATUS_CHANGED",
                from_state=curr_status.value,
                to_state=TaskStatus.SUCCESS.value,
                metadata_json={"duration_ms": duration_ms, **(execution_metadata or {})},
                created_at=now,
            )
            self.db.add(event)

            self.db.commit()
            self.db.refresh(task)
        except Exception:
            self.db.rollback()
            raise

        try:
            from taskforge.core.cache import get_cache

            get_cache().invalidate(task_id)
        except Exception:
            pass

        return task

    def complete_task_failure(
        self,
        task_id: str,
        lease_token: str,
        error_message: str,
        error_traceback: str | None = None,
        is_timeout: bool = False,
        attempt_id: int | None = None,
        duration_ms: float | None = None,
    ) -> tuple[Task, bool]:
        """Atomically record attempt failure, evaluate retries under lock,
        transition Task to RETRY_WAIT or DEAD_LETTER, release lease, and record event
        in one transaction under strict lease fencing.
        Returns (task, is_retryable).
        """
        stmt = select(Task).where(Task.id == task_id).with_for_update()
        task = self.db.scalars(stmt).first()
        if not task:
            raise ValueError(f"Task with id {task_id} not found")

        if not lease_token or task.lease_token != lease_token:
            raise PermissionError(
                f"Fencing violation: lease token '{lease_token}' does not match active task lease '{task.lease_token}'"
            )

        curr_status = TaskStatus(task.status)

        # Pre-validate attempt ownership and status before applying any mutations
        attempt = None
        if attempt_id is not None:
            attempt_stmt = select(TaskAttempt).where(
                TaskAttempt.id == attempt_id,
                TaskAttempt.task_id == task_id,
            )
            attempt = self.db.scalars(attempt_stmt).first()
            if not attempt:
                raise ValueError(
                    f"Attempt ownership violation: TaskAttempt '{attempt_id}' does not belong to task '{task_id}'."
                )
            if attempt.status not in ("RUNNING", "PENDING"):
                raise ValueError(
                    f"Attempt status violation: cannot fail TaskAttempt '{attempt_id}' in terminal status '{attempt.status}'."
                )

        try:
            now = get_utc_now()
            if attempt:
                attempt.status = "TIMED_OUT" if is_timeout else "FAILED"
                attempt.error_message = error_message
                attempt.error_traceback = error_traceback
                attempt.completed_at = now
                attempt.duration_ms = duration_ms

            # 2. Evaluate retry policy under lock
            if task.retry_count < task.max_retries:
                task.retry_count += 1
                nxt_status = TaskStatus.RETRY_WAIT
                is_retryable = True
                reason = f"attempt_{task.retry_count}_failed"

                validate_transition(curr_status, nxt_status)
                task.status = nxt_status.value

                event = TaskEvent(
                    task_id=task.id,
                    event_type="STATUS_CHANGED",
                    from_state=curr_status.value,
                    to_state=nxt_status.value,
                    metadata_json={
                        "error": error_message,
                        "retry_count": task.retry_count,
                        "reason": reason,
                        "is_timeout": is_timeout,
                    },
                    created_at=now,
                )
                self.db.add(event)
            else:
                # Multi-step state machine transition: RUNNING -> FAILED -> DEAD_LETTER
                validate_transition(curr_status, TaskStatus.FAILED)
                validate_transition(TaskStatus.FAILED, TaskStatus.DEAD_LETTER)
                task.status = TaskStatus.DEAD_LETTER.value
                is_retryable = False
                reason = "max_retries_exceeded"
                task.completed_at = now

                event_fail = TaskEvent(
                    task_id=task.id,
                    event_type="STATUS_CHANGED",
                    from_state=curr_status.value,
                    to_state=TaskStatus.FAILED.value,
                    metadata_json={"error": error_message, "retries_exhausted": True},
                    created_at=now,
                )
                self.db.add(event_fail)

                event_dlq = TaskEvent(
                    task_id=task.id,
                    event_type="STATUS_CHANGED",
                    from_state=TaskStatus.FAILED.value,
                    to_state=TaskStatus.DEAD_LETTER.value,
                    metadata_json={"reason": reason, "final_error": error_message},
                    created_at=now,
                )
                self.db.add(event_dlq)

            task.updated_at = now
            task.lease_token = None
            task.lease_expires_at = None

            self.db.commit()
            self.db.refresh(task)
        except Exception:
            self.db.rollback()
            raise

        try:
            from taskforge.core.cache import get_cache

            get_cache().invalidate(task_id)
        except Exception:
            pass

        return task, is_retryable

    def acquire_lease(
        self,
        task_id: str,
        worker_id: str,
        lease_duration_seconds: int = 30,
        lease_token: str | None = None,
    ) -> bool:
        stmt = select(Task).where(Task.id == task_id).with_for_update()
        task = self.db.scalars(stmt).first()
        if not task:
            return False

        # Task State Guard: Fresh lease acquisition restricted to claimable unstarted states.
        # RUNNING tasks whose lease expired must be transitioned via recover_expired_leases() first.
        # Terminal tasks (SUCCESS, DEAD_LETTER, CANCELLED) can never acquire leases.
        CLAIMABLE_STATUSES = {
            TaskStatus.PENDING.value,
            TaskStatus.QUEUED.value,
            TaskStatus.RETRY_WAIT.value,
        }
        # RUNNING tasks with an active or expired lease token MUST pass through recover_expired_leases().
        # A RUNNING task is only claimable if its lease was cleanly vacated:
        # BOTH lease_token MUST be None AND lease_expires_at MUST be None.
        # An inconsistent state (e.g. lease_token is None but lease_expires_at is set) is strictly rejected.
        if task.status == TaskStatus.RUNNING.value:
            if task.lease_token is not None or task.lease_expires_at is not None:
                return False
        elif task.status not in CLAIMABLE_STATUSES:
            return False

        now = get_utc_now()
        # Scheduled Execution Guard: Do not permit lease acquisition earlier than scheduled_at
        if task.scheduled_at is not None:
            sched = task.scheduled_at
            if sched.tzinfo is None:
                sched = sched.replace(tzinfo=UTC)
            if sched > now:
                return False
        if task.lease_expires_at is not None:
            exp = task.lease_expires_at
            if exp.tzinfo is None:
                exp = exp.replace(tzinfo=UTC)
            # Active lease exists -> reject all callers, even if same worker identity!
            if exp >= now:
                return False

        fresh_token = lease_token or f"lease-{uuid.uuid4().hex}"
        expires_at = now + timedelta(seconds=lease_duration_seconds)
        task.lease_token = fresh_token
        task.lease_expires_at = expires_at
        task.updated_at = now
        self.db.commit()
        return True

    def renew_lease(
        self,
        task_id: str,
        lease_token: str,
        lease_duration_seconds: int = 30,
    ) -> bool:
        now = get_utc_now()
        expires_at = now + timedelta(seconds=lease_duration_seconds)
        stmt = (
            select(Task)
            .where(Task.id == task_id)
            .where(Task.lease_token == lease_token)
            .with_for_update()
        )
        task = self.db.scalars(stmt).first()
        if not task:
            return False

        task.lease_expires_at = expires_at
        task.updated_at = now
        self.db.commit()
        return True

    def release_lease(self, task_id: str, lease_token: str) -> bool:
        stmt = (
            select(Task)
            .where(Task.id == task_id)
            .where(Task.lease_token == lease_token)
            .with_for_update()
        )
        task = self.db.scalars(stmt).first()
        if not task:
            return False

        task.lease_token = None
        task.lease_expires_at = None
        task.updated_at = get_utc_now()
        self.db.commit()
        return True

    def recover_expired_leases(self) -> list[RecoveredTaskInfo]:
        """Find RUNNING tasks whose lease has expired and transition them to RETRY_WAIT or DEAD_LETTER.
        Commits the transaction explicitly on success, or rolls back on exception before re-raising.
        Returns a list of immutable RecoveredTaskInfo objects.
        """
        now = get_utc_now()
        try:
            stmt = (
                select(Task)
                .where(Task.status == TaskStatus.RUNNING.value)
                .where(Task.lease_expires_at < now)
                .with_for_update(skip_locked=True)
            )
            expired_tasks = list(self.db.scalars(stmt).all())
            recovered: list[RecoveredTaskInfo] = []
            for task in expired_tasks:
                # Re-check under row lock
                if task.lease_expires_at is not None:
                    exp = task.lease_expires_at
                    if exp.tzinfo is None:
                        exp = exp.replace(tzinfo=UTC)
                    if exp >= now:
                        continue

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
                recovered.append(RecoveredTaskInfo(task_id=task.id, status=task.status))

            self.db.commit()
            return recovered
        except Exception:
            self.db.rollback()
            raise
