import concurrent.futures
import random
import time
import traceback
from enum import StrEnum
from typing import Any

from sqlalchemy import func, select

import taskforge.tasks  # noqa: F401
from taskforge.core.logging import get_logger
from taskforge.core.state_machine import TaskStatus, is_terminal_state
from taskforge.database.models import TaskAttempt, get_utc_now
from taskforge.database.repositories.task_repository import TaskRepository
from taskforge.database.session import SessionLocal
from taskforge.definitions.hashing import compute_definition_content_hash
from taskforge.definitions.models import TaskDefinitionVersion
from taskforge.definitions.validation import validate_schema_instance
from taskforge.monitoring.metrics import record_task_execution_duration
from taskforge.monitoring.profiler import record_diagnostic_event
from taskforge.tasks.registry import TaskRegistry, get_handler
from taskforge.workers.lease_manager import LeaseManager

logger = get_logger(__name__)


class TaskExecutionAdapterError(Exception):
    """Base error for execution adapter failures."""

    allow_retry: bool = False
    error_type: str = "EXECUTION_ERROR"


class SchemaValidationError(TaskExecutionAdapterError):
    """Input JSON schema validation failed (non-retryable)."""

    allow_retry = False
    error_type = "SCHEMA_VALIDATION_ERROR"


class OutputValidationError(TaskExecutionAdapterError):
    """Output JSON schema validation failed (non-retryable)."""

    allow_retry = False
    error_type = "OUTPUT_VALIDATION_ERROR"


class HandlerUnavailableError(TaskExecutionAdapterError):
    """Handler is not registered or not callable (non-retryable)."""

    allow_retry = False
    error_type = "HANDLER_UNAVAILABLE_ERROR"


class DefinitionIntegrityError(TaskExecutionAdapterError):
    """Definition version content hash mismatch or corrupted (non-retryable)."""

    allow_retry = False
    error_type = "DEFINITION_INTEGRITY_ERROR"


class ExecutionDisposition(StrEnum):
    """Explicit outcome contract for task execution to govern queue delivery ACK decisions."""

    COMPLETED = "completed"
    TERMINAL_DUPLICATE = "terminal_duplicate"
    TERMINAL_FAILURE_RECORDED = "terminal_failure_recorded"
    RETRY_DURABLY_SCHEDULED = "retry_durably_scheduled"
    RETRY_PENDING_PUBLICATION = "retry_pending_publication"
    OWNERSHIP_REJECTED = "ownership_rejected"
    UNEXPECTED_FAILURE = "unexpected_failure"

    def is_success(self) -> bool:
        return self in (ExecutionDisposition.COMPLETED, ExecutionDisposition.TERMINAL_DUPLICATE)

    def should_ack(self) -> bool:
        return self in (
            ExecutionDisposition.COMPLETED,
            ExecutionDisposition.TERMINAL_DUPLICATE,
            ExecutionDisposition.TERMINAL_FAILURE_RECORDED,
            ExecutionDisposition.RETRY_DURABLY_SCHEDULED,
        )

    def __bool__(self) -> bool:
        return self.is_success()

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, bool):
            return self.is_success() is other
        return super().__eq__(other)


def calculate_backoff(attempt: int, base: float = 1.0, max_backoff: float = 60.0) -> float:
    """Calculate exponential backoff with full jitter."""
    exp = min(attempt, 6)
    backoff = min(max_backoff, base * (2**exp))
    jitter = random.uniform(0.05, 0.5)
    return backoff + jitter


def execute_task(
    task_id: str,
    worker_id: str,
    lease_manager: LeaseManager | None = None,
    timeout_seconds: int | None = None,
    lease_token: str | None = None,
    use_outbox: bool = True,
) -> ExecutionDisposition:
    """Executes a single task run with lease management, timeout enforcement,
    attempt tracking, atomic result persistence, and retry publication.
    Returns an ExecutionDisposition indicating the outcome and ACK eligibility.
    """
    if lease_token and not lease_manager:
        lease = LeaseManager(task_id, worker_id, lease_token=lease_token)
    else:
        lease = lease_manager or LeaseManager(task_id, worker_id)
        if not lease.acquire():
            # If lease acquisition was rejected because task is already terminal,
            # safely ignore duplicate delivery without performing unnecessary work or side effects.
            with SessionLocal() as db:
                repo = TaskRepository(db)
                cur_task = repo.get_by_id(task_id)
                if cur_task and is_terminal_state(cur_task.status):
                    logger.info(
                        "duplicate_delivery_ignored_terminal_state",
                        task_id=task_id,
                        status=cur_task.status,
                    )
                    return ExecutionDisposition.TERMINAL_DUPLICATE
            logger.warning(
                "task_lease_acquisition_failed_ownership_rejected",
                task_id=task_id,
                worker_id=worker_id,
            )
            return ExecutionDisposition.OWNERSHIP_REJECTED

    lease_token = lease.lease_token

    start_time = time.perf_counter()
    start_utc = get_utc_now()
    attempt_num = 1
    attempt_id: int | None = None

    try:
        with SessionLocal() as db:
            repo = TaskRepository(db)
            task = repo.get_by_id(task_id)
            if not task:
                logger.error("task_not_found_for_execution", task_id=task_id)
                return ExecutionDisposition.UNEXPECTED_FAILURE

            # Duplicate message handling - if already in terminal state, ignore safely
            if is_terminal_state(task.status):
                logger.info(
                    "duplicate_delivery_ignored_terminal_state",
                    task_id=task_id,
                    status=task.status,
                )
                return ExecutionDisposition.TERMINAL_DUPLICATE

            # Calculate strictly monotonic attempt number per task
            max_attempt = (
                db.scalars(
                    select(func.max(TaskAttempt.attempt_number)).where(
                        TaskAttempt.task_id == task_id
                    )
                ).first()
                or 0
            )
            attempt_num = max(task.retry_count + 1, max_attempt + 1)

            if task.status == TaskStatus.RETRY_WAIT.value:
                repo.update_status(
                    task_id,
                    TaskStatus.QUEUED,
                    {"reason": "retrying_execution"},
                    lease_token=lease_token,
                )

            # Transition to RUNNING with lease fencing if not already running
            if task.status != TaskStatus.RUNNING.value:
                repo.update_status(
                    task_id,
                    TaskStatus.RUNNING,
                    {"worker_id": worker_id},
                    lease_token=lease_token,
                )

            # Record attempt start under strict lease fencing
            attempt = repo.record_attempt(
                task_id=task_id,
                attempt_number=attempt_num,
                worker_id=worker_id,
                status="RUNNING",
                lease_token=lease_token,
                started_at=start_utc,
            )
            attempt_id = attempt.id
            task_type = task.task_type
            payload = dict(task.payload)
            payload["_current_attempt"] = attempt_num

            output_schema = None
            if task.definition_version_id:
                ver = (
                    db.execute(
                        select(TaskDefinitionVersion).where(
                            TaskDefinitionVersion.id == task.definition_version_id,
                            TaskDefinitionVersion.definition_id == task.definition_id,
                        )
                    )
                    .scalars()
                    .first()
                )
                if not ver:
                    raise DefinitionIntegrityError(
                        f"Pinned version '{task.definition_version_id}' not found for task '{task_id}'."
                    )

                # R1: Content hash integrity verification against executable content
                expected_hash = compute_definition_content_hash(
                    task_type=ver.task_type,
                    execution_type=ver.execution_type,
                    configuration_json=ver.configuration_json,
                    input_schema_json=ver.input_schema_json,
                    output_schema_json=ver.output_schema_json,
                    execution_policy_json=ver.execution_policy_json,
                )
                if ver.content_hash != expected_hash:
                    raise DefinitionIntegrityError(
                        f"Content hash mismatch for version {ver.id}: expected {expected_hash}, recorded {ver.content_hash}."
                    )

                # R5 / G11.4-05: Runtime input validation prior to handler invocation
                if ver.input_schema_json:
                    input_data = payload.get("_input", payload)
                    val_errors = validate_schema_instance(ver.input_schema_json, input_data)
                    if val_errors:
                        raise SchemaValidationError(
                            f"Input schema validation failed: {val_errors[0]}"
                        )

                output_schema = ver.output_schema_json
                task_type = ver.task_type

            # Determine authoritative timeout
            if timeout_seconds is None:
                timeout_seconds = task.timeout_seconds
            else:
                timeout_seconds = min(timeout_seconds, task.timeout_seconds)

        # G11.4-03: Handler resolution strictly through live TaskRegistry
        if not TaskRegistry.has_handler(task_type):
            raise HandlerUnavailableError(
                f"Handler for task type '{task_type}' is not registered in live TaskRegistry."
            )
        handler = get_handler(task_type)
        if not callable(handler):
            raise HandlerUnavailableError(
                f"Resolved handler for task type '{task_type}' is not callable."
            )
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)

        def timed_callable(fn: Any, arg: Any) -> Any:
            started = time.perf_counter()
            record_diagnostic_event("handler_start", task_id=task_id)
            try:
                return fn(arg)
            finally:
                record_diagnostic_event("handler_end", task_id=task_id)
                record_task_execution_duration(task_type, time.perf_counter() - started)

        try:
            future = executor.submit(timed_callable, handler, payload)
            result_data = future.result(timeout=timeout_seconds)
        except concurrent.futures.TimeoutError:
            try:
                executor.shutdown(wait=False, cancel_futures=True)
            except TypeError:
                executor.shutdown(wait=False)
            raise
        finally:
            try:
                executor.shutdown(wait=False, cancel_futures=True)
            except TypeError:
                executor.shutdown(wait=False)

        # G11.4-06: Validate output schema if defined
        if output_schema:
            out_errors = validate_schema_instance(output_schema, result_data)
            if out_errors:
                raise OutputValidationError(f"Output schema validation failed: {out_errors[0]}")

        duration_ms = round((time.perf_counter() - start_time) * 1000, 2)

        # Atomic success path: result + SUCCESS status + attempt update in single transaction under fencing
        with SessionLocal() as db:
            repo = TaskRepository(db)
            repo.complete_task_success(
                task_id=task_id,
                lease_token=lease_token,
                return_value=result_data,
                execution_metadata={
                    "worker_id": worker_id,
                    "attempt": attempt_num,
                    "duration_ms": duration_ms,
                },
                attempt_id=attempt_id,
                duration_ms=duration_ms,
            )

        logger.info(
            "task_completed_successfully",
            task_id=task_id,
            attempt=attempt_num,
            duration_ms=duration_ms,
        )
        return ExecutionDisposition.COMPLETED

    except Exception as exc:
        if isinstance(exc, PermissionError):
            logger.warning(
                "stale_worker_fenced_aborting",
                task_id=task_id,
                worker_id=worker_id,
                error=str(exc),
            )
            return ExecutionDisposition.OWNERSHIP_REJECTED

        duration_ms = round((time.perf_counter() - start_time) * 1000, 2)
        is_timeout = isinstance(exc, concurrent.futures.TimeoutError)
        error_msg = f"Task timed out after {timeout_seconds}s" if is_timeout else str(exc)
        tb_str = traceback.format_exc()

        allow_retry = True
        error_type = None
        if isinstance(exc, TaskExecutionAdapterError):
            allow_retry = exc.allow_retry
            error_type = exc.error_type
        elif is_timeout:
            allow_retry = True
            error_type = "TIMEOUT"
        else:
            allow_retry = True
            error_type = "HANDLER_EXECUTION_ERROR"

        logger.warning(
            "task_execution_failed",
            task_id=task_id,
            attempt=attempt_num,
            error=error_msg,
            error_type=error_type,
            allow_retry=allow_retry,
            timeout=is_timeout,
        )

        try:
            # Atomic failure path: attempt update + retry policy + status change in single transaction under fencing
            with SessionLocal() as db:
                repo = TaskRepository(db)
                task_updated, is_retryable = repo.complete_task_failure(
                    task_id=task_id,
                    lease_token=lease_token,
                    error_message=error_msg,
                    error_traceback=tb_str,
                    is_timeout=is_timeout,
                    attempt_id=attempt_id,
                    duration_ms=duration_ms,
                    publish_to_outbox=use_outbox,
                    allow_retry=allow_retry,
                    error_type=error_type,
                )

            if is_retryable:
                if use_outbox:
                    return ExecutionDisposition.RETRY_DURABLY_SCHEDULED
                return ExecutionDisposition.RETRY_PENDING_PUBLICATION
            else:
                return ExecutionDisposition.TERMINAL_FAILURE_RECORDED

        except PermissionError as perm_err:
            logger.warning(
                "stale_worker_fenced_during_failure_path_aborting",
                task_id=task_id,
                worker_id=worker_id,
                error=str(perm_err),
            )
            return ExecutionDisposition.OWNERSHIP_REJECTED
        except Exception as handler_err:
            logger.error(
                "failure_recording_failed",
                task_id=task_id,
                worker_id=worker_id,
                error=str(handler_err),
            )
            return ExecutionDisposition.UNEXPECTED_FAILURE

        return ExecutionDisposition.UNEXPECTED_FAILURE

    finally:
        lease.release()


def execute_task_process_isolated(
    task_id: str,
    worker_id: str,
    timeout_seconds: int = 60,
    lease_manager: LeaseManager | None = None,
    use_outbox: bool = True,
) -> ExecutionDisposition:
    """Executes a task in a dedicated OS subprocess with hard timeout enforcement.
    If execution exceeds timeout_seconds, the OS subprocess is forcibly killed via proc.kill()
    (Win32 TerminateProcess / POSIX SIGKILL), ensuring complete CPU and memory reclamation.
    Lease fencing guarantees that any post-termination mutations are rejected.
    Returns an ExecutionDisposition indicating the outcome and ACK eligibility.
    """
    import subprocess
    import sys

    lease = lease_manager or LeaseManager(task_id, worker_id)
    if not lease.lease_token:
        if not lease.acquire():
            with SessionLocal() as db:
                repo = TaskRepository(db)
                cur_task = repo.get_by_id(task_id)
                if cur_task and is_terminal_state(cur_task.status):
                    return ExecutionDisposition.TERMINAL_DUPLICATE
            logger.warning(
                "process_isolated_lease_acquisition_failed",
                task_id=task_id,
                worker_id=worker_id,
            )
            return ExecutionDisposition.OWNERSHIP_REJECTED

    # Ensure task is marked RUNNING under lease fencing before launching child process
    with SessionLocal() as db:
        repo = TaskRepository(db)
        cur_task = repo.get_by_id(task_id)
        if not cur_task:
            return ExecutionDisposition.UNEXPECTED_FAILURE
        if is_terminal_state(cur_task.status):
            return ExecutionDisposition.TERMINAL_DUPLICATE
        if cur_task.status != TaskStatus.RUNNING.value:
            repo.update_status(
                task_id,
                TaskStatus.RUNNING,
                {"worker_id": worker_id, "attempt": cur_task.retry_count + 1},
                lease_token=lease.lease_token,
            )

    cmd = [
        sys.executable,
        "-m",
        "taskforge.workers.process_runner",
        "--task-id",
        task_id,
        "--worker-id",
        worker_id,
        "--timeout",
        str(timeout_seconds),
        "--lease-token",
        str(lease.lease_token),
    ]

    proc = subprocess.Popen(cmd)
    try:
        proc.wait(timeout=timeout_seconds)
        with SessionLocal() as db:
            repo = TaskRepository(db)
            final_t = repo.get_by_id(task_id)
            if final_t:
                if final_t.status == TaskStatus.SUCCESS.value:
                    return ExecutionDisposition.COMPLETED
                elif final_t.status == TaskStatus.DEAD_LETTER.value:
                    return ExecutionDisposition.TERMINAL_FAILURE_RECORDED
                elif final_t.status == TaskStatus.RETRY_WAIT.value:
                    return (
                        ExecutionDisposition.RETRY_DURABLY_SCHEDULED
                        if use_outbox
                        else ExecutionDisposition.RETRY_PENDING_PUBLICATION
                    )
                elif is_terminal_state(final_t.status):
                    return ExecutionDisposition.TERMINAL_DUPLICATE
        return ExecutionDisposition.UNEXPECTED_FAILURE
    except subprocess.TimeoutExpired:
        logger.warning(
            "process_isolated_task_timed_out_killing_os_process",
            task_id=task_id,
            worker_id=worker_id,
            pid=proc.pid,
            timeout=timeout_seconds,
        )
        proc.kill()
        try:
            proc.wait(timeout=5.0)
        except Exception:
            pass

        # Stop parent renewal immediately
        lease.stop_renewal()

        # Atomically record failure under lease fencing
        try:
            with SessionLocal() as db:
                repo = TaskRepository(db)
                task_updated, is_retryable = repo.complete_task_failure(
                    task_id=task_id,
                    lease_token=lease.lease_token,
                    error_message=f"Task timed out after {timeout_seconds}s (hard OS process kill)",
                    is_timeout=True,
                    duration_ms=timeout_seconds * 1000.0,
                    publish_to_outbox=use_outbox,
                )
                if is_retryable:
                    if use_outbox:
                        return ExecutionDisposition.RETRY_DURABLY_SCHEDULED
                    return ExecutionDisposition.RETRY_PENDING_PUBLICATION
                return ExecutionDisposition.TERMINAL_FAILURE_RECORDED
        except Exception as exc:
            logger.warning(
                "process_isolated_timeout_failure_recording_failed",
                task_id=task_id,
                error=str(exc),
            )
        return ExecutionDisposition.UNEXPECTED_FAILURE
    finally:
        lease.release()
