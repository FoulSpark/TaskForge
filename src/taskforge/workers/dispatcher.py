import argparse
import concurrent.futures
import threading
import time
import uuid
from typing import Optional

from taskforge.core.logging import get_logger
from taskforge.database.repositories.task_repository import TaskRepository
from taskforge.database.session import SessionLocal
from taskforge.queue.broker import MessageBroker, get_broker
from taskforge.workers.execution_engine import execute_task
from taskforge.workers.heartbeat import WorkerHeartbeat

logger = get_logger(__name__)


class WorkerDispatcher:
    """
    Concurrent worker dispatcher that pulls tasks from the broker,
    executes them concurrently, maintains worker heartbeat, and recovers expired leases.
    """

    def __init__(
        self,
        worker_id: Optional[str] = None,
        concurrency: int = 4,
        broker: Optional[MessageBroker] = None,
        poll_interval: float = 0.2,
    ):
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:6]}"
        self.concurrency = concurrency
        self.broker = broker or get_broker()
        self.poll_interval = poll_interval
        self._stop_event = threading.Event()
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self.concurrency, thread_name_prefix=f"{self.worker_id}-thread"
        )
        self._heartbeat = WorkerHeartbeat(self.worker_id, concurrency=self.concurrency)
        self._active_tasks = 0
        self._active_lock = threading.Lock()
        self._loop_thread: Optional[threading.Thread] = None
        self._recovery_thread: Optional[threading.Thread] = None

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

    def stop(self) -> None:
        self._stop_event.set()
        if self._loop_thread:
            self._loop_thread.join(timeout=2.0)
        if self._recovery_thread:
            self._recovery_thread.join(timeout=1.0)
        self._executor.shutdown(wait=False)
        self._heartbeat.stop()
        logger.info("worker_dispatcher_stopped", worker_id=self.worker_id)

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

            with self._active_lock:
                self._active_tasks += 1
                self._heartbeat.set_active_tasks(self._active_tasks)

            future = self._executor.submit(self._task_wrapper, task_id)

    def _task_wrapper(self, task_id: str) -> None:
        try:
            execute_task(task_id, self.worker_id)
        except Exception as exc:
            logger.error("worker_unexpected_task_error", task_id=task_id, error=str(exc))
        finally:
            with self._active_lock:
                self._active_tasks = max(0, self._active_tasks - 1)
                self._heartbeat.set_active_tasks(self._active_tasks)

    def _stale_lease_recovery_loop(self) -> None:
        """Periodically check for crashed worker leases and recover them."""
        while not self._stop_event.is_set():
            self._stop_event.wait(10.0)
            if self._stop_event.is_set():
                break
            try:
                with SessionLocal() as db:
                    repo = TaskRepository(db)
                    recovered = repo.recover_expired_leases()
                    for t in recovered:
                        logger.info("stale_task_recovered_from_expired_lease", task_id=t.id)
                        if t.status == "RETRY_WAIT":
                            self.broker.enqueue(t.id, t.priority, t.payload)
            except Exception as exc:
                logger.debug("stale_lease_reaper_error", error=str(exc))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TaskForge Worker Dispatcher")
    parser.add_argument("--worker-id", type=str, default=None)
    parser.add_argument("--concurrency", type=int, default=4)
    args = parser.parse_args()

    dispatcher = WorkerDispatcher(worker_id=args.worker_id, concurrency=args.concurrency)
    dispatcher.start()
    print(f"TaskForge Worker '{dispatcher.worker_id}' is running with concurrency {dispatcher.concurrency}...")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        dispatcher.stop()
        print("Worker stopped.")
