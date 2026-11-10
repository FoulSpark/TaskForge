import json
import time
from pathlib import Path
import pytest

from taskforge.monitoring.profiler import DiagnosticProfiler, record_diagnostic_event
from taskforge.queue.broker import MemoryBroker
from taskforge.workers.dispatcher import WorkerDispatcher


def test_dispatcher_diagnostic_events_emission(tmp_path: Path):
    profiler = DiagnosticProfiler.get_instance()
    events_file = tmp_path / "dispatcher_events.jsonl"
    profiler.start(events_file)

    broker = MemoryBroker()
    dispatcher = WorkerDispatcher(
        worker_id="test-dispatcher",
        concurrency=1,
        broker=broker,
        poll_interval=0.05,
    )

    try:
        # Enqueue one task
        broker.enqueue("task-dispatch-1", 1, {"test": True})
        dispatcher.start()

        # Wait for task to be processed
        time.sleep(0.3)
    finally:
        dispatcher.stop(grace_period_seconds=1.0)
        report = profiler.stop(drain_timeout=2.0)

    assert events_file.exists()
    lines = [json.loads(l) for l in events_file.read_text(encoding="utf-8").splitlines() if l.strip()]
    assert len(lines) > 0

    event_names = [l["event"] for l in lines]
    assert "dispatcher_dequeue_start" in event_names
    assert "redis_dequeue_complete" in event_names
    assert "dispatcher_task_submitted" in event_names
    assert "worker_slot_released" in event_names

    # Check seq monotonicity
    seqs = [l["seq"] for l in lines]
    assert seqs == sorted(seqs)
    assert len(seqs) == len(set(seqs))

    # Check metadata fields
    submitted_event = next(l for l in lines if l["event"] == "dispatcher_task_submitted")
    assert submitted_event["task_id"] == "task-dispatch-1"
    assert "loop_iter" in submitted_event["metadata"]
    assert "iter_id" in submitted_event["metadata"]
    assert "active_tasks" in submitted_event["metadata"]
    assert submitted_event["metadata"]["concurrency"] == 1

    released_event = next(l for l in lines if l["event"] == "worker_slot_released")
    assert released_event["task_id"] == "task-dispatch-1"
    assert released_event["metadata"]["active_tasks"] == 0
    assert released_event["metadata"]["concurrency"] == 1

    assert report["total_dropped"] == 0


def test_dispatcher_sleep_diagnostic_events_when_saturated(tmp_path: Path):
    profiler = DiagnosticProfiler.get_instance()
    events_file = tmp_path / "dispatcher_sleep_events.jsonl"
    profiler.start(events_file)

    broker = MemoryBroker()
    dispatcher = WorkerDispatcher(
        worker_id="test-dispatcher-sleep",
        concurrency=1,
        broker=broker,
        poll_interval=0.05,
    )

    try:
        # Simulate saturation by holding active lock
        with dispatcher._active_lock:
            dispatcher._active_tasks = 1

        dispatcher.start()
        # Let dispatch loop encounter saturation and sleep
        time.sleep(0.15)

        with dispatcher._active_lock:
            dispatcher._active_tasks = 0
        time.sleep(0.05)
    finally:
        dispatcher.stop(grace_period_seconds=1.0)
        report = profiler.stop(drain_timeout=2.0)

    lines = [json.loads(l) for l in events_file.read_text(encoding="utf-8").splitlines() if l.strip()]
    event_names = [l["event"] for l in lines]

    assert "dispatcher_sleep_start" in event_names
    assert "dispatcher_sleep_end" in event_names

    sleep_start = next(l for l in lines if l["event"] == "dispatcher_sleep_start")
    sleep_end = next(l for l in lines if l["event"] == "dispatcher_sleep_end")

    assert "sleep_id" in sleep_start["metadata"]
    assert "sleep_id" in sleep_end["metadata"]
    assert sleep_start["metadata"]["active_tasks"] >= 1
    assert report["total_dropped"] == 0

