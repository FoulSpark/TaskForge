import argparse
import concurrent.futures
import signal
import threading
import time
import uuid
from typing import Any

from taskforge.core.logging import get_logger
from taskforge.database.repositories.task_repository import TaskRepository
from taskforge.database.session import SessionLocal
from taskforge.queue.broker import MessageBroker, get_broker
from taskforge.workers.execution_engine import (
    ExecutionDisposition,
    execute_task,
    execute_task_process_isolated,
)
from taskforge.workers.heartbeat import WorkerHeartbeat
from taskforge.workers.lease_manager import LeaseManager
from taskforge.monitoring.profiler import record_diagnostic_event

logger = get_logger(__name__)


class WorkerDispatcher:
    """
    Concurrent worker dispatcher that pulls tasks from the broker,
    executes them concurrently, maintains worker heartbeat, and recovers expired leases.
    """

    def __init__(
        self,
        worker_id: str | None = None,
        concurrency: int = 4,
        broker: MessageBroker | None = None,
        poll_interval: float = 0.2,
        execution_mode: str = "thread",
    ):
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:6]}"
        self.concurrency = concurrency
        self.broker = broker or get_broker()
        self.poll_interval = poll_interval
        self.execution_mode = execution_mode
        self._stop_event = threading.Event()
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self.concurrency, thread_name_prefix=f"{self.worker_id}-thread"
        )
        self._heartbeat = WorkerHeartbeat(self.worker_id, concurrency=self.concurrency)
        self._active_tasks = 0
        self._active_lock = threading.Lock()
        self._active_leases: dict[str, LeaseManager] = {}
        self._leases_lock = threading.Lock()
        self._loop_thread: threading.Thread | None = None
        self._recovery_thread: threading.Thread | None = None

    def start(self) -> None:
        if self._loop_thread and self._loop_thread.is_alive():
            return
        self._stop_event.clear()
        self._heartbeat.start()

        self._loop_thread = threading.Thread(
            target=self._dispatch_loop, name=f"Dispatcher-{self.worker_id}", daemon=True
        )
        self._loop_thread.start()

        self._recovery_thread = threading.Thread(
            target=self._stale_lease_recovery_loop,
            name=f"Reaper-{self.worker_id}",
            daemon=True,
        )
        self._recovery_thread.start()

        logger.info(
            "worker_dispatcher_started",
            worker_id=self.worker_id,
            concurrency=self.concurrency,
        )

    def stop(self, grace_period_seconds: float = 10.0) -> None:
        """Gracefully stop dispatcher: stop pulling new tasks, allow active tasks
        to complete within grace_period_seconds, then clean up resources and revoke
        active leases on unfinished tasks to ensure immediate recoverability.
        """
        logger.info(
            "worker_dispatcher_stopping_graceful",
            worker_id=self.worker_id,
            grace_period=grace_period_seconds,
        )
        self._stop_event.set()
        if self._loop_thread:
            self._loop_thread.join(timeout=2.0)
        if self._recovery_thread:
            self._recovery_thread.join(timeout=1.0)

        # Allow in-progress tasks to finish up to grace period
        start_wait = time.time()
        while time.time() - start_wait < grace_period_seconds:
            with self._active_lock:
                if self._active_tasks == 0:
                    break
            time.sleep(0.1)

        # GRACE PERIOD EXPIRATION ENFORCEMENT:
        # If tasks are still running past grace period, forcefully revoke lease renewal
        # so they expire immediately in PostgreSQL, allowing replacement workers to recover them.
        with self._leases_lock:
            for task_id, active_lease in list(self._active_leases.items()):
                logger.warning(
                    "grace_period_expired_halting_lease_renewal",
                    task_id=task_id,
                    worker_id=self.worker_id,
                    lease_token=active_lease.lease_token,
                )
                active_lease.stop_renewal()
                active_lease.release()

        try:
            self._executor.shutdown(wait=False, cancel_futures=True)
        except TypeError:
            self._executor.shutdown(wait=False)

        self._heartbeat.stop()
        logger.info(
            "worker_dispatcher_stopped",
            worker_id=self.worker_id,
            remaining_active=self._active_tasks,
        )

    def _dispatch_loop(self) -> None:
        while not self._stop_event.is_set():
            with self._active_lock:
                if self._active_tasks >= self.concurrency:
                    time.sleep(self.poll_interval)
                    continue

            task_msg = self.broker.dequeue(timeout_seconds=self.poll_interval)
            if not task_msg:
                continue

            task_id = task_msg.get("task_id")
            if not task_id:
                continue

            record_diagnostic_event("redis_dequeue_complete", task_id=task_id)

            with self._active_lock:
                self._active_tasks += 1
                self._heartbeat.set_active_tasks(self._active_tasks)

            self._executor.submit(self._task_wrapper, task_msg)

    def _task_wrapper(self, task_msg: dict[str, Any]) -> None:
        task_id = task_msg.get("task_id", "")
        delivery_token = task_msg.get("delivery_token", "")
        lease = LeaseManager(task_id, self.worker_id)
        with self._leases_lock:
            self._active_leases[task_id] = lease

        disposition = ExecutionDisposition.UNEXPECTED_FAILURE
        try:
            if self.execution_mode == "process":
                timeout = task_msg.get("timeout_seconds")
                if not timeout and "payload" in task_msg and isinstance(task_msg["payload"], dict):
                    timeout = task_msg["payload"].get("timeout_seconds")
                if not timeout:
                    with SessionLocal() as db:
                        repo = TaskRepository(db)
                        t = repo.get_by_id(task_id)
                        timeout = t.timeout_seconds if t else 60
                disposition = execute_task_process_isolated(
                    task_id=task_id,
                    worker_id=self.worker_id,
                    timeout_seconds=timeout,
                    lease_manager=lease,
                )
            else:
                disposition = execute_task(task_id, self.worker_id, lease_manager=lease)

            # DISPATCHER ACK CONTRACT:
            # ONLY acknowledge delivery if work completed, was an existing terminal duplicate,
            # terminal failure was committed to PostgreSQL, or retry publication was durably committed to outbox.
            # STRICTLY REFUSE TO ACK if:
            # - OWNERSHIP_REJECTED (active lease belongs to another worker; preserves delivery)
            # - RETRY_PENDING_PUBLICATION (retry state committed, but durable retry outbox publication is pending)
            # - UNEXPECTED_FAILURE
            if disposition in (
                ExecutionDisposition.COMPLETED,
                ExecutionDisposition.TERMINAL_DUPLICATE,
                ExecutionDisposition.TERMINAL_FAILURE_RECORDED,
                ExecutionDisposition.RETRY_DURABLY_SCHEDULED,
            ):
                self.broker.ack(task_id, task_msg, delivery_token=delivery_token)
                record_diagnostic_event("redis_ack_complete", task_id=task_id)
            else:
                logger.warning(
                    "delivery_ack_withheld_for_disposition",
                    task_id=task_id,
                    worker_id=self.worker_id,
                    disposition=disposition.value,
                )
        except PermissionError as perm_err:
            # Stale/fenced worker: lost ownership, strictly do not ack so legitimate worker owns it
            logger.warning("stale_worker_fenced_cannot_ack", task_id=task_id, error=str(perm_err))
        except Exception as exc:
            # Unhandled worker crash/exception: leave in-flight so recovery loop can reclaim
            logger.error("worker_unexpected_task_error", task_id=task_id, error=str(exc))
        finally:
            with self._leases_lock:
                self._active_leases.pop(task_id, None)
            with self._active_lock:
                self._active_tasks = max(0, self._active_tasks - 1)
                self._heartbeat.set_active_tasks(self._active_tasks)

    def run_recovery_cycle(
        self,
        visibility_timeout_seconds: float = 30.0,
    ) -> tuple[int, int]:
        """Recover PostgreSQL task ownership first, then reclaim expired Redis deliveries.
        Redis reclamation must never run if PostgreSQL recovery fails to commit successfully.
        Returns (recovered_tasks_count, reclaimed_deliveries_count).
        """
        # Stage 1: PostgreSQL is authoritative for task ownership.
        with SessionLocal() as db:
            repo = TaskRepository(db)
            recovered_tasks = repo.recover_expired_leases()
            recovered_count = len(recovered_tasks)

        # Stage 2: Reclaim expired broker deliveries (only reached after DB commit and session closure).
        reclaimed_count = self.broker.reclaim_expired(
            timeout_seconds=visibility_timeout_seconds
        )
        return recovered_count, reclaimed_count

    def _stale_lease_recovery_loop(self) -> None:
        """Periodically check for crashed worker leases and recover unacked in-flight queue items.
        Authoritative Recovery Policy & Ordering:
        1. First, recover expired PostgreSQL leases (RUNNING -> RETRY_WAIT / DEAD_LETTER) and commit.
           This ensures database state is fully consistent before any message can be redelivered.
        2. Second, reclaim expired unacknowledged in-flight queue items back to ready queue.
           A replacement worker claiming the delivery will see a database state ready for lease acquisition.
        """
        while not self._stop_event.is_set():
            self._stop_event.wait(5.0)
            if self._stop_event.is_set():
                break
            try:
                recovered, reclaimed = self.run_recovery_cycle(visibility_timeout_seconds=30.0)
                if recovered:
                    logger.info(
                        "stale_task_recovery_completed",
                        recovered_count=recovered,
                    )
                if reclaimed:
                    logger.info(
                        "redis_delivery_reclaim_completed",
                        reclaimed_count=reclaimed,
                    )
            except Exception:
                logger.exception(
                    "stale_lease_recovery_cycle_failed",
                    worker_id=self.worker_id,
                )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TaskForge Worker Dispatcher")
    parser.add_argument("--worker-id", type=str, default=None)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--grace-period", type=float, default=10.0)
    parser.add_argument(
        "--execution-mode",
        type=str,
        default="thread",
        choices=["thread", "process"],
        help="Task execution model: thread (in-process ThreadPool) or process (dedicated OS subprocess)",
    )
    args = parser.parse_args()

    dispatcher = WorkerDispatcher(
        worker_id=args.worker_id,
        concurrency=args.concurrency,
        execution_mode=args.execution_mode,
    )

    def _handle_signal(sig, frame):
        logger.info("received_shutdown_signal", signal=sig, worker_id=dispatcher.worker_id)
        dispatcher.stop(grace_period_seconds=args.grace_period)
        raise SystemExit(0)

    signal.signal(signal.SIGINT, _handle_signal)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _handle_signal)

    dispatcher.start()
    print(
        f"TaskForge Worker '{dispatcher.worker_id}' is running with concurrency {dispatcher.concurrency}..."
    )
    try:
        while not dispatcher._stop_event.is_set():
            time.sleep(0.5)
    except KeyboardInterrupt:
        dispatcher.stop(grace_period_seconds=args.grace_period)
    print("Worker stopped.")
