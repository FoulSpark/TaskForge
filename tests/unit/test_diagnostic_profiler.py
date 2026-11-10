import json
from pathlib import Path
import time
import pytest

from taskforge.monitoring.profiler import DiagnosticProfiler, record_diagnostic_event


def test_diagnostic_profiler_lifecycle(tmp_path: Path):
    profiler = DiagnosticProfiler(max_queue_size=100)
    artifact = tmp_path / "test_events.jsonl"

    assert not profiler.is_active

    profiler.start(artifact)
    assert profiler.is_active

    # Record sample events across different categories
    profiler.record_event("api_request_received", task_id="t-1")
    profiler.record_event("task_transaction_commit", task_id="t-1")
    profiler.record_event("outbox_claim_start", batch_id="b-1")
    profiler.record_event("handler_start", task_id="t-1")
    profiler.record_event("handler_end", task_id="t-1")

    report = profiler.stop(drain_timeout=2.0)
    assert not profiler.is_active
    assert report["persisted_count"] == 5
    assert report["total_dropped"] == 0
    assert report["unpersisted_count"] == 0
    assert not report["drain_timeout_exceeded"]

    # Verify file content
    lines = [json.loads(line) for line in artifact.read_text(encoding="utf-8").strip().splitlines()]
    assert len(lines) == 5
    events_recorded = [line["event"] for line in lines]
    assert events_recorded == [
        "api_request_received",
        "task_transaction_commit",
        "outbox_claim_start",
        "handler_start",
        "handler_end",
    ]
    for line in lines:
        assert "t_mono_ns" in line
        assert "t_epoch_utc" in line
        assert isinstance(line["t_mono_ns"], int)


def test_diagnostic_profiler_drop_accounting(tmp_path: Path):
    profiler = DiagnosticProfiler(max_queue_size=2)
    artifact = tmp_path / "drop_events.jsonl"

    profiler.start(artifact)
    # Pause flusher loop temporarily to simulate saturation
    profiler._queue.put_nowait(None)  # Sentinel to block or simulate full

    # Fill queue to capacity
    profiler.record_event("redis_enqueue_start", task_id="t-1")
    profiler.record_event("redis_enqueue_complete", task_id="t-1")

    # Overflow events
    profiler.record_event("redis_dequeue_complete", task_id="t-1")
    profiler.record_event("redis_dequeue_complete", task_id="t-2")
    profiler.record_event("lease_commit", task_id="t-1")

    report = profiler.stop(drain_timeout=2.0)
    assert report["total_dropped"] >= 3
    assert "redis_dequeue_complete" in report["drops_by_type"]
    assert "lease_commit" in report["drops_by_type"]

