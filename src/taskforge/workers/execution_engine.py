import concurrent.futures
import random
import threading
import time
import traceback
from enum import StrEnum
from typing import Any

import taskforge.tasks  # noqa: F401
from taskforge.core.logging import get_logger
from taskforge.core.state_machine import TaskStatus, is_terminal_state
from taskforge.database.models import get_utc_now
from taskforge.database.repositories.task_repository import TaskRepository
from taskforge.database.session import SessionLocal
from taskforge.queue.broker import get_broker
from taskforge.tasks.registry import get_handler
from taskforge.workers.lease_manager import LeaseManager

logger = get_logger(__name__)


class ExecutionDisposition(StrEnum):
    """Explicit outcome contract for task execution to govern queue delivery ACK decisions."""

    COMPLETED = "completed"
    TERMINAL_DUPLICATE = "terminal_duplicate"
    TERMINAL_FAILURE_RECORDED = "terminal_failure_recorded"
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
) -> ExecutionDisposition:
    """Executes a single task run with lease management, timeout enforcement,
    attempt tracking, atomic result persistence, and retry backoff.
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

            attempt_num = task.retry_count + 1

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

            # Record attempt start
            attempt = repo.record_attempt(
                task_id=task_id,
                attempt_number=attempt_num,
                worker_id=worker_id,
                status="RUNNING",
                started_at=start_utc,
            )
            attempt_id = attempt.id
            task_type = task.task_type
            payload = dict(task.payload)
            payload["_current_attempt"] = attempt_num

            # Determine authoritative timeout
            if timeout_seconds is None:
                timeout_seconds = task.timeout_seconds
            else:
                timeout_seconds = min(timeout_seconds, task.timeout_seconds)

        # Execute handler with timeout enforcement without blocking on executor __exit__
        handler = get_handler(task_type)
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        try:
            future = executor.submit(handler, payload)
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

        logger.warning(
            "task_execution_failed",
            task_id=task_id,
            attempt=attempt_num,
            error=error_msg,
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
                )

            # Schedule retry re-enqueue if retryable
            if is_retryable:
                backoff_delay = calculate_backoff(task_updated.retry_count)

                def re_enqueue():
                    time.sleep(backoff_delay)
                    try:
                        with SessionLocal() as re_db:
                            re_repo = TaskRepository(re_db)
                            cur = re_repo.get_by_id(task_id)
                            if cur and cur.status == TaskStatus.RETRY_WAIT.value:
                                re_repo.update_status(
                                    task_id,
                                    TaskStatus.QUEUED,
                                    {"reason": "retry_backoff_elapsed"},
                                )
                                broker = get_broker()
                                broker.enqueue(task_id, cur.priority, cur.payload)
                    except Exception as err:
                        logger.error("retry_re_enqueue_error", task_id=task_id, error=str(err))

                threading.Thread(target=re_enqueue, daemon=True).start()
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

        return ExecutionDisposition.UNEXPECTED_FAILURE

    finally:
        lease.release()


def execute_task_process_isolated(
    task_id: str,
    worker_id: str,
    timeout_seconds: int = 60,
    lease_manager: LeaseManager | None = None,
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
        if proc.returncode == 0:
            with SessionLocal() as db:
                repo = TaskRepository(db)
                final_t = repo.get_by_id(task_id)
                if final_t and final_t.status == TaskStatus.SUCCESS.value:
                    return ExecutionDisposition.COMPLETED
                elif final_t and is_terminal_state(final_t.status):
                    return ExecutionDisposition.TERMINAL_DUPLICATE
            return ExecutionDisposition.COMPLETED
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
                )
                if is_retryable:
                    backoff_delay = calculate_backoff(task_updated.retry_count)

                    def re_enqueue():
                        time.sleep(backoff_delay)
                        try:
                            with SessionLocal() as re_db:
                                re_repo = TaskRepository(re_db)
                                cur = re_repo.get_by_id(task_id)
                                if cur and cur.status == TaskStatus.RETRY_WAIT.value:
                                    re_repo.update_status(
                                        task_id,
                                        TaskStatus.QUEUED,
                                        {"reason": "retry_backoff_elapsed"},
                                    )
                                    broker = get_broker()
                                    broker.enqueue(task_id, cur.priority, cur.payload)
                        except Exception as err:
                            logger.error("retry_re_enqueue_error", task_id=task_id, error=str(err))

                    threading.Thread(target=re_enqueue, daemon=True).start()
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
