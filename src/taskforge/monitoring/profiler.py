"""Stage 1B Diagnostic Lifecycle Profiler.

Implements the 16-event lifecycle attribution contract with:
1. Bounded diagnostic queue (queue.Queue(maxsize=50000)).
2. Non-blocking enqueue (put_nowait()).
3. Explicit drop accounting:
   - diagnostic_event_drop_count (total)
   - diagnostic_event_drop_count_by_type (per event type)
4. Monotonic nanosecond timestamps (time.perf_counter_ns()) and UTC epoch (time.time()).
5. Asynchronous JSONL flusher thread.
6. Bounded graceful shutdown with drain timeout (5.0s).
7. Strict zero overhead when disabled (is_active == False).
"""

from collections import defaultdict
from dataclasses import dataclass
import json
from pathlib import Path
import queue
import threading
import time
from typing import Any

from taskforge.core.logging import get_logger

logger = get_logger(__name__)


@dataclass(slots=True)
class DiagnosticEvent:
    event_name: str
    task_id: str | None
    batch_id: str | None
    t_mono_ns: int
    t_epoch_utc: float
    metadata: dict[str, Any] | None = None


class DiagnosticProfiler:
    _instance: "DiagnosticProfiler | None" = None
    _singleton_lock = threading.Lock()

    def __init__(self, max_queue_size: int = 50000):
        self.max_queue_size = max_queue_size
        self._queue: queue.Queue[DiagnosticEvent | None] = queue.Queue(maxsize=max_queue_size)
        self._active = False
        self._flusher_thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._artifact_path: Path | None = None
        self._persisted_count = 0
        self._drop_counter = 0
        self._drops_by_type: dict[str, int] = defaultdict(int)
        self._lock = threading.Lock()

    @classmethod
    def get_instance(cls) -> "DiagnosticProfiler":
        with cls._singleton_lock:
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

    @property
    def is_active(self) -> bool:
        return self._active

    def record_event(
        self,
        event_name: str,
        task_id: str | None = None,
        batch_id: str | None = None,
        t_mono_ns: int | None = None,
        t_epoch_utc: float | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        if not self._active:
            return

        mono_ts = t_mono_ns if t_mono_ns is not None else time.perf_counter_ns()
        epoch_ts = t_epoch_utc if t_epoch_utc is not None else time.time()

        event = DiagnosticEvent(
            event_name=event_name,
            task_id=task_id,
            batch_id=batch_id,
            t_mono_ns=mono_ts,
            t_epoch_utc=epoch_ts,
            metadata=metadata,
        )

        try:
            self._queue.put_nowait(event)
        except queue.Full:
            with self._lock:
                self._drop_counter += 1
                self._drops_by_type[event_name] += 1

    def start(self, artifact_path: Path) -> None:
        with self._lock:
            self._artifact_path = artifact_path
            self._artifact_path.parent.mkdir(parents=True, exist_ok=True)
            self._persisted_count = 0
            self._drop_counter = 0
            self._drops_by_type.clear()
            self._stop_event.clear()

            # Empty queue if any leftover
            while not self._queue.empty():
                try:
                    self._queue.get_nowait()
                except queue.Empty:
                    break

            self._active = True
            self._flusher_thread = threading.Thread(
                target=self._flusher_loop,
                name="DiagnosticFlusher",
                daemon=True,
            )
            self._flusher_thread.start()
            logger.info("diagnostic_profiler_started", path=str(artifact_path))

    def stop(self, drain_timeout: float = 5.0) -> dict[str, Any]:
        with self._lock:
            if not self._active:
                return {
                    "persisted_count": self._persisted_count,
                    "total_dropped": self._drop_counter,
                    "drops_by_type": dict(self._drops_by_type),
                    "unpersisted_count": self._queue.qsize(),
                    "drain_timeout_exceeded": False,
                }
            self._active = False
            self._stop_event.set()

        # Signal flusher via sentinel
        try:
            self._queue.put_nowait(None)
        except queue.Full:
            pass

        timeout_exceeded = False
        if self._flusher_thread and self._flusher_thread.is_alive():
            self._flusher_thread.join(timeout=drain_timeout)
            if self._flusher_thread.is_alive():
                timeout_exceeded = True
                logger.warning(
                    "diagnostic_flusher_drain_timeout",
                    drain_timeout=drain_timeout,
                    unpersisted_count=self._queue.qsize(),
                )

        unpersisted = self._queue.qsize()
        report = {
            "persisted_count": self._persisted_count,
            "total_dropped": self._drop_counter,
            "drops_by_type": dict(self._drops_by_type),
            "unpersisted_count": unpersisted,
            "drain_timeout_exceeded": timeout_exceeded,
        }
        logger.info("diagnostic_profiler_stopped", **report)
        return report

    def _flusher_loop(self) -> None:
        if not self._artifact_path:
            return

        with open(self._artifact_path, "a", encoding="utf-8") as f:
            while not self._stop_event.is_set() or not self._queue.empty():
                try:
                    item = self._queue.get(timeout=0.1)
                except queue.Empty:
                    continue

                if item is None:
                    if self._stop_event.is_set():
                        break
                    continue

                line = json.dumps({
                    "event": item.event_name,
                    "task_id": item.task_id,
                    "batch_id": item.batch_id,
                    "t_mono_ns": item.t_mono_ns,
                    "t_epoch_utc": item.t_epoch_utc,
                    "metadata": item.metadata,
                })
                f.write(line + "\n")
                self._persisted_count += 1
                f.flush()


def record_diagnostic_event(
    event_name: str,
    task_id: str | None = None,
    batch_id: str | None = None,
    t_mono_ns: int | None = None,
    t_epoch_utc: float | None = None,
    metadata: dict[str, Any] | None = None,
) -> None:
    profiler = DiagnosticProfiler.get_instance()
    if profiler.is_active:
        profiler.record_event(
            event_name=event_name,
            task_id=task_id,
            batch_id=batch_id,
            t_mono_ns=t_mono_ns,
            t_epoch_utc=t_epoch_utc,
            metadata=metadata,
        )

