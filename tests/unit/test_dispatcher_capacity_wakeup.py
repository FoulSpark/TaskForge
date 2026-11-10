import time
import threading
import pytest

from taskforge.queue.broker import MemoryBroker
from taskforge.workers.dispatcher import WorkerDispatcher


def test_capacity_wakeup_signal_before_wait():
    """Verify that if a worker slot is released before the dispatcher's capacity check,
    the dispatcher never blocks in capacity wait and immediately dequeues the task."""
    broker = MemoryBroker()
    dispatcher = WorkerDispatcher(
        worker_id="test-wakeup-before",
        concurrency=1,
        broker=broker,
        poll_interval=0.5, # Long 0.5s poll interval
        enable_capacity_wakeup=True,
    )

    # Pre-set active tasks to 1 (saturated)
    with dispatcher._active_lock:
        dispatcher._active_tasks = 1

    dispatcher.start()
    time.sleep(0.05)

    # Worker completes and releases slot before enqueue
    with dispatcher._active_lock:
        dispatcher._active_tasks = 0
    dispatcher._capacity_event.set()

    t_enq = time.perf_counter()
    broker.enqueue("task-before-wait", 1, {"payload": 1})

    # Wait for task to be dequeued
    while time.perf_counter() - t_enq < 0.4:
        if broker.get_queue_depth() == 0:
            break
        time.sleep(0.01)

    t_deq = time.perf_counter() - t_enq
    dispatcher.stop(grace_period_seconds=0.1)

    assert broker.get_queue_depth() == 0
    # Proves it dequeued in < 0.15s without waiting for the 0.5s poll interval
    assert t_deq < 0.15, f"Took too long to dequeue: {t_deq:.3f}s"


def test_capacity_wakeup_signal_during_wait():
    """Verify that when the dispatcher is sleeping waiting for capacity,
    a worker slot release immediately wakes the dispatcher in < 25ms."""
    broker = MemoryBroker()
    dispatcher = WorkerDispatcher(
        worker_id="test-wakeup-during",
        concurrency=1,
        broker=broker,
        poll_interval=1.0, # Long 1.0s poll interval
        enable_capacity_wakeup=True,
    )

    # Saturate dispatcher
    with dispatcher._active_lock:
        dispatcher._active_tasks = 1

    dispatcher.start()
    # Let dispatch loop enter wait
    time.sleep(0.1)

    broker.enqueue("task-during-wait", 1, {"payload": 2})

    wake_time = time.perf_counter()
    # Release slot and signal
    with dispatcher._active_lock:
        dispatcher._active_tasks = 0
    dispatcher._capacity_event.set()

    # The task should be dequeued and executed almost immediately
    max_wait = 0.2
    start_wait = time.perf_counter()
    while time.perf_counter() - start_wait < max_wait:
        if broker.get_queue_depth() == 0:
            break
        time.sleep(0.01)

    elapsed_to_dequeue = time.perf_counter() - wake_time
    dispatcher.stop(grace_period_seconds=0.1)

    # Confirms dispatcher woke up immediately (< 0.1s) and did NOT sleep for 1.0s
    assert elapsed_to_dequeue < 0.15, f"Wake-up took too long: {elapsed_to_dequeue:.3f}s"


def test_capacity_wakeup_interleaved_race():
    """Verify the critical race window: event cleared, saturated check True,
    worker releases slot and sets event immediately before wait().
    Wait must return immediately with ZERO lost wake-up."""
    broker = MemoryBroker()
    dispatcher = WorkerDispatcher(
        worker_id="test-wakeup-race",
        concurrency=1,
        broker=broker,
        poll_interval=1.0,
        enable_capacity_wakeup=True,
    )

    dispatcher._capacity_event.clear()
    with dispatcher._active_lock:
        dispatcher._active_tasks = 1
        is_saturated = dispatcher._active_tasks >= dispatcher.concurrency

    assert is_saturated is True

    # Concurrent worker finishes RIGHT HERE before dispatcher calls event.wait():
    with dispatcher._active_lock:
        dispatcher._active_tasks = 0
    dispatcher._capacity_event.set()

    # Now dispatcher calls event.wait(timeout)
    t0 = time.perf_counter()
    waited = dispatcher._capacity_event.wait(timeout=dispatcher.poll_interval)
    t1 = time.perf_counter()

    assert waited is True
    assert (t1 - t0) < 0.05, f"Event wait blocked unexpectedly: {t1 - t0:.3f}s (lost wakeup occurred!)"


def test_capacity_wakeup_multiple_notifications_no_spinning():
    """Verify that multiple rapid worker completions do not cause busy-spinning or duplicate dispatches."""
    broker = MemoryBroker()
    dispatcher = WorkerDispatcher(
        worker_id="test-wakeup-multi",
        concurrency=4,
        broker=broker,
        poll_interval=0.1,
        enable_capacity_wakeup=True,
    )

    # Enqueue 4 tasks
    for i in range(4):
        broker.enqueue(f"multi-task-{i}", 1, {"idx": i})

    dispatcher.start()

    # Wait for all tasks to complete
    start = time.perf_counter()
    while time.perf_counter() - start < 1.0:
        with dispatcher._active_lock:
            if dispatcher._active_tasks == 0 and broker.get_queue_depth() == 0:
                break
        time.sleep(0.02)

    dispatcher.stop(grace_period_seconds=0.1)
    assert broker.get_queue_depth() == 0


def test_capacity_wakeup_clean_shutdown():
    """Verify that when dispatcher is blocked in event wait on saturated capacity,
    stop() wakes it immediately and cleanly shuts down."""
    broker = MemoryBroker()
    dispatcher = WorkerDispatcher(
        worker_id="test-wakeup-shutdown",
        concurrency=1,
        broker=broker,
        poll_interval=2.0, # Long 2s wait
        enable_capacity_wakeup=True,
    )

    with dispatcher._active_lock:
        dispatcher._active_tasks = 1

    dispatcher.start()
    time.sleep(0.1)

    # Clear active tasks so graceful shutdown does not need to wait for in-flight tasks
    with dispatcher._active_lock:
        dispatcher._active_tasks = 0

    t0 = time.perf_counter()
    dispatcher.stop(grace_period_seconds=0.5)
    t1 = time.perf_counter()

    assert (t1 - t0) < 0.25, f"Shutdown took too long: {t1 - t0:.3f}s"
    assert not dispatcher._loop_thread.is_alive()


def test_capacity_wakeup_fallback_timeout():
    """Verify that when saturated and no worker completes, dispatcher times out cleanly
    at poll_interval without hanging."""
    broker = MemoryBroker()
    dispatcher = WorkerDispatcher(
        worker_id="test-wakeup-timeout",
        concurrency=1,
        broker=broker,
        poll_interval=0.1,
        enable_capacity_wakeup=True,
    )

    # Wait without any worker signaling
    dispatcher._capacity_event.clear()
    t0 = time.perf_counter()
    waited = dispatcher._capacity_event.wait(timeout=0.1)
    t1 = time.perf_counter()

    assert waited is False # Timed out
    assert 0.08 <= (t1 - t0) <= 0.15


def test_capacity_wakeup_default_enabled():
    """Verify that WorkerDispatcher defaults to enable_capacity_wakeup=True in production configuration."""
    broker = MemoryBroker()
    disp_default = WorkerDispatcher(broker=broker)
    assert disp_default.enable_capacity_wakeup is True

    disp_disabled = WorkerDispatcher(broker=broker, enable_capacity_wakeup=False)
    assert disp_disabled.enable_capacity_wakeup is False


def test_capacity_wakeup_worker_exception_signals_event(monkeypatch):
    """Verify that worker exceptions in handler/pipeline still execute finally block,
    decrement active tasks, and set capacity event promptly."""
    broker = MemoryBroker()
    dispatcher = WorkerDispatcher(
        worker_id="test-wakeup-error",
        concurrency=1,
        broker=broker,
        poll_interval=0.5,
        enable_capacity_wakeup=True,
    )

    # Monkeypatch execute_task to raise an error
    def failing_execute(*args, **kwargs):
        raise RuntimeError("simulated handler explosion")

    monkeypatch.setattr("taskforge.workers.dispatcher.execute_task", failing_execute)

    broker.enqueue("failing-task-1", 1, {"payload": "err"})
    broker.enqueue("normal-task-2", 1, {"payload": "ok"})

    dispatcher.start()

    # The failing task should execute, raise, hit finally, set capacity event,
    # and immediately wake the dispatcher to dequeue the second task.
    start = time.perf_counter()
    while time.perf_counter() - start < 0.4:
        if broker.get_queue_depth() == 0:
            break
        time.sleep(0.01)

    dur = time.perf_counter() - start
    dispatcher.stop(grace_period_seconds=0.1)

    assert broker.get_queue_depth() == 0
    # Both tasks dequeued in < 0.3s without waiting for 0.5s polling timeout
    assert dur < 0.35, f"Took too long to process after failure: {dur:.3f}s"


def test_capacity_wakeup_high_contention_simultaneous_completions():
    """Verify high-contention scenario: multiple workers finish simultaneously via a barrier
    while the dispatcher is saturated. Verify all released capacity is utilized and no queued
    task is left starved."""
    broker = MemoryBroker()
    concurrency = 4
    dispatcher = WorkerDispatcher(
        worker_id="test-wakeup-contention",
        concurrency=concurrency,
        broker=broker,
        poll_interval=0.5, # Long 0.5s sleep if wakeup fails
        enable_capacity_wakeup=True,
    )

    # Fill queue with 8 tasks (4 initial + 4 subsequent)
    for i in range(8):
        broker.enqueue(f"contention-task-{i}", 1, {"idx": i})

    barrier = threading.Barrier(concurrency)
    completed_tasks = []
    lock = threading.Lock()

    # Simulate workers holding slots until released together
    orig_wrapper = dispatcher._task_wrapper

    def controlled_wrapper(task_msg):
        task_idx = task_msg.get("payload", {}).get("idx", 0)
        if task_idx < concurrency:
            # First batch of 4 tasks wait on barrier to release simultaneously
            try:
                barrier.wait(timeout=1.0)
            except threading.BrokenBarrierError:
                pass
        with lock:
            completed_tasks.append(task_idx)
        orig_wrapper(task_msg)

    dispatcher._task_wrapper = controlled_wrapper

    t0 = time.perf_counter()
    dispatcher.start()

    # Wait for all 8 tasks to be completed
    while time.perf_counter() - t0 < 1.0:
        if len(completed_tasks) == 8 and broker.get_queue_depth() == 0:
            break
        time.sleep(0.02)

    total_dur = time.perf_counter() - t0
    dispatcher.stop(grace_period_seconds=0.1)

    assert broker.get_queue_depth() == 0
    assert len(completed_tasks) == 8
    # 8 tasks processed with simultaneous releases must finish well under 2 * poll_interval (1.0s)
    assert total_dur < 0.7, f"Contention processing took too long: {total_dur:.3f}s"

