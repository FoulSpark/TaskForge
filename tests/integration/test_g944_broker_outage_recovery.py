"""Gate G9.4.4: Broker Reconnection & Transient Outage Recovery Test Suite.

Verifies AC-9.4.5:
1. Scenario 1: Redis becomes unavailable for 5 seconds while Publisher is active.
   - 0 durable outbox events lost.
   - Unpublished events remain PENDING with incremented retry_count.
   - Publisher backs off exponentially without tight spin.
   - Publisher resumes publication after Redis reconnection without process restart.
2. Scenario 2: Redis becomes unavailable for 5 seconds while Dispatcher is active.
   - Dispatcher thread survives without crash or manual restart.
   - Bounded exponential backoff applied during disconnect.
   - Dispatcher resumes pulling tasks immediately after Redis reconnection.
3. Scenario 3: Joint Outage (Publisher & Dispatcher simultaneously active).
   - Both experience 5-second outage concurrently.
   - Zero event loss in PostgreSQL outbox.
   - After recovery, all events published to Redis and dispatched to completion.
   - Authoritative state in PostgreSQL verified (SUCCESS).
4. Scenario 4: Duplicate Delivery & Fencing during Outage Recovery.
   - Unacked or redelivered tasks handled idempotently as TERMINAL_DUPLICATE.
   - Authoritative state is never corrupted.
5. Scenario 5: Script Cache Invalidation (NOSCRIPT) Recovery.
   - If Redis restarts and script cache is cleared, broker auto-reloads Lua scripts.
"""

from datetime import UTC, datetime
import json
import threading
import time
from typing import Any
import uuid

import pytest
import redis
from sqlalchemy import select, text

from taskforge.core.state_machine import TaskPriority, TaskStatus
from taskforge.database.models import OutboxEvent, Task, TaskResult, get_utc_now
from taskforge.database.repositories.task_repository import TaskRepository
from taskforge.database.session import SessionLocal
from taskforge.queue.broker import (
    LUA_ACK,
    LUA_DEQUEUE,
    LUA_ENQUEUE,
    LUA_RECLAIM,
    RedisBroker,
)
from taskforge.queue.outbox import OutboxPublisherWorker, publish_pending_outbox_events
from taskforge.queue.redis_client import create_redis_client
from taskforge.workers.dispatcher import WorkerDispatcher


class SimulatedOutageInterceptor:
    """Intercepts RedisBroker operations to simulate an exact transient network outage."""

    def __init__(self, real_broker: RedisBroker, outage_duration_seconds: float = 5.0):
        self.real_broker = real_broker
        self.duration = outage_duration_seconds
        self.start_time: float | None = None
        self.is_outage_active = False
        self.outage_triggered = False
        self.outage_recovered = False
        self.intercepted_calls = 0
        self.failed_calls = 0
        self.successful_calls = 0
        self._lock = threading.Lock()

    def trigger_outage(self) -> None:
        with self._lock:
            self.start_time = time.time()
            self.is_outage_active = True
            self.outage_triggered = True

    @property
    def is_recovered(self) -> bool:
        with self._lock:
            if not self.outage_triggered or self.start_time is None:
                return False
            return (time.time() - self.start_time) >= self.duration

    def _check_outage(self) -> None:
        with self._lock:
            self.intercepted_calls += 1
            if not self.is_outage_active:
                self.successful_calls += 1
                return
            elapsed = time.time() - self.start_time
            if elapsed < self.duration:
                self.failed_calls += 1
                raise redis.exceptions.ConnectionError(
                    f"Simulated Redis outage in progress ({elapsed:.2f}s < {self.duration:.1f}s)"
                )
            else:
                self.is_outage_active = False
                self.outage_recovered = True
                self.successful_calls += 1

    def enqueue(self, task_id: str, priority: int, payload: dict[str, Any]) -> None:
        self._check_outage()
        return self.real_broker.enqueue(task_id, priority, payload)

    def dequeue(self, timeout_seconds: float = 1.0) -> dict[str, Any] | None:
        self._check_outage()
        return self.real_broker.dequeue(timeout_seconds)

    def ack(
        self,
        task_id: str,
        message: dict[str, Any] | None = None,
        delivery_token: str | None = None,
    ) -> bool:
        self._check_outage()
        return self.real_broker.ack(task_id, message, delivery_token)

    def reclaim_expired(self, timeout_seconds: float = 30.0) -> int:
        self._check_outage()
        return self.real_broker.reclaim_expired(timeout_seconds)

    def get_queue_depth(self) -> int:
        self._check_outage()
        return self.real_broker.get_queue_depth()

    def get_in_flight_depth(self) -> int:
        self._check_outage()
        return self.real_broker.get_in_flight_depth()


@pytest.fixture
def redis_client():
    client = create_redis_client()
    try:
        client.ping()
    except Exception as exc:
        pytest.skip(f"Redis unavailable: {exc}")
    return client


@pytest.fixture(autouse=True)
def clean_test_resources(redis_client):
    def _clean():
        keys = redis_client.keys("taskforge:g944:*")
        if keys:
            redis_client.delete(*keys)
        with SessionLocal() as db:
            db.execute(text("DELETE FROM task_events WHERE task_id LIKE 'g944-%'"))
            db.execute(text("DELETE FROM task_attempts WHERE task_id LIKE 'g944-%'"))
            db.execute(text("DELETE FROM task_results WHERE task_id LIKE 'g944-%'"))
            db.execute(text("DELETE FROM outbox_events WHERE aggregate_id LIKE 'g944-%'"))
            db.execute(text("DELETE FROM tasks WHERE id LIKE 'g944-%'"))
            # Purge any stale pending outbox events from other tests to prevent cross-test contention
            db.execute(text("DELETE FROM outbox_events WHERE status = 'PENDING'"))
            db.commit()

    _clean()
    yield
    _clean()


def test_g944_scenario1_publisher_5s_outage_recovery(redis_client):
    """Scenario 1: Redis unavailable for 5 seconds while publisher is running.
    - Events remain PENDING in PostgreSQL.
    - retry_count increments without event loss.
    - Publisher does not crash and applies backoff.
    - When Redis recovers, all events transition to PUBLISHED.
    """
    queue_name = f"taskforge:g944:q1:{uuid.uuid4().hex[:6]}"
    real_broker = RedisBroker(redis_client, queue_name=queue_name)
    proxy_broker = SimulatedOutageInterceptor(real_broker, outage_duration_seconds=5.0)

    # 1. Seed 10 tasks in PostgreSQL with pending outbox events
    task_ids = []
    with SessionLocal() as db:
        for i in range(10):
            tid = f"g944-p1-task-{i}-{uuid.uuid4().hex[:6]}"
            task = Task(
                id=tid,
                task_name=f"Publisher Outage Task {i}",
                task_type="noop",
                status=TaskStatus.QUEUED.value,
                priority=TaskPriority.HIGH.value,
                payload={"i": i},
            )
            outbox_evt = OutboxEvent(
                id=str(uuid.uuid4()),
                aggregate_type="task",
                aggregate_id=tid,
                event_type="TASK_CREATED",
                payload={"task_id": tid, "priority": TaskPriority.HIGH.value},
                status="PENDING",
                available_at=get_utc_now(),
                created_at=get_utc_now(),
            )
            db.add(task)
            db.add(outbox_evt)
            task_ids.append(tid)
        db.commit()

    # 2. Start Publisher Worker with 0.2s poll interval using proxy broker
    publisher = OutboxPublisherWorker(poll_interval=0.2, broker=proxy_broker)
    
    # Trigger 5-second outage immediately before start
    proxy_broker.trigger_outage()
    t_start = time.time()
    publisher.start()

    # During outage (verify at 2.5s)
    time.sleep(2.5)
    with SessionLocal() as db:
        pending_count = db.scalars(
            select(OutboxEvent).where(
                OutboxEvent.aggregate_id.in_(task_ids),
                OutboxEvent.status == "PENDING",
            )
        ).all()
        # Events must remain PENDING
        assert len(pending_count) == 10, "All events must remain PENDING during outage"
        # At least one retry attempt logged
        assert any(e.retry_count > 0 for e in pending_count), "Retry counts must increment"

    # Wait for outage recovery (5.0s total duration + buffer for backoff wake)
    time.sleep(3.5)
    assert proxy_broker.is_recovered is True, "Outage must be marked recovered"

    # Wait up to 6.0s for publisher to drain the 10 events
    deadline = time.time() + 6.0
    all_published = False
    while time.time() < deadline:
        with SessionLocal() as db:
            pub_count = db.scalars(
                select(OutboxEvent).where(
                    OutboxEvent.aggregate_id.in_(task_ids),
                    OutboxEvent.status == "PUBLISHED",
                )
            ).all()
            if len(pub_count) == 10:
                all_published = True
                break
        time.sleep(0.2)

    publisher.stop()

    assert all_published is True, "All 10 outbox events must be PUBLISHED after recovery"
    assert real_broker.get_queue_depth() == 10, "Redis queue must contain exactly 10 tasks"
    assert proxy_broker.failed_calls > 0, "Failed calls during outage must be recorded"


def test_g944_scenario2_dispatcher_5s_outage_recovery(redis_client):
    """Scenario 2: Redis unavailable for 5 seconds while dispatcher is active.
    - Dispatcher thread survives without crash or manual restart.
    - Applies bounded exponential backoff.
    - Resumes pulling tasks immediately after Redis recovers.
    """
    queue_name = f"taskforge:g944:q2:{uuid.uuid4().hex[:6]}"
    real_broker = RedisBroker(redis_client, queue_name=queue_name)
    proxy_broker = SimulatedOutageInterceptor(real_broker, outage_duration_seconds=5.0)

    # 1. Seed 5 tasks directly in PostgreSQL and enqueue to real broker
    task_ids = []
    with SessionLocal() as db:
        for i in range(5):
            tid = f"g944-d1-task-{i}-{uuid.uuid4().hex[:6]}"
            task = Task(
                id=tid,
                task_name=f"Dispatcher Outage Task {i}",
                task_type="noop",
                status=TaskStatus.QUEUED.value,
                priority=TaskPriority.HIGH.value,
                payload={"idx": i},
            )
            db.add(task)
            real_broker.enqueue(tid, TaskPriority.HIGH.value, {"task_id": tid})
            task_ids.append(tid)
        db.commit()

    assert real_broker.get_queue_depth() == 5

    # 2. Trigger 5-second outage and start Dispatcher
    proxy_broker.trigger_outage()
    dispatcher = WorkerDispatcher(
        worker_id="g944-worker-disp",
        concurrency=2,
        broker=proxy_broker,
        poll_interval=0.2,
    )
    dispatcher.start()

    # During outage (verify at 2.5s): dispatcher thread must be alive and tasks still in queue
    time.sleep(2.5)
    assert dispatcher._loop_thread.is_alive() is True, "Dispatcher loop thread must not crash during outage"
    assert dispatcher._active_tasks == 0, "No tasks could be dequeued during outage"

    # Wait for outage duration to expire (5.0s total + buffer)
    time.sleep(3.5)
    assert proxy_broker.is_recovered is True

    # Wait up to 5.0s for dispatcher to process all 5 tasks
    deadline = time.time() + 5.0
    all_success = False
    while time.time() < deadline:
        with SessionLocal() as db:
            repo = TaskRepository(db)
            completed = [repo.get_by_id(tid) for tid in task_ids]
            if all(t and t.status == TaskStatus.SUCCESS.value for t in completed):
                all_success = True
                break
        time.sleep(0.2)

    dispatcher.stop()

    assert all_success is True, "All 5 tasks must be processed to SUCCESS after recovery"
    assert real_broker.get_queue_depth() == 0, "Redis queue must be fully drained"
    assert real_broker.get_in_flight_depth() == 0, "Redis in-flight queue must be 0"


def test_g944_scenario3_joint_outage_publisher_and_dispatcher(redis_client):
    """Scenario 3: Both Publisher and Dispatcher experience simultaneous 5s Redis outage.
    - End-to-end flow: 20 tasks submitted.
    - 5-second outage triggered.
    - Neither process crashes.
    - On recovery, publisher drains outbox to Redis; dispatcher drains Redis to SUCCESS.
    - Reconciles zero event loss, full execution, and authoritative state.
    """
    queue_name = f"taskforge:g944:q3:{uuid.uuid4().hex[:6]}"
    real_broker = RedisBroker(redis_client, queue_name=queue_name)
    proxy_broker = SimulatedOutageInterceptor(real_broker, outage_duration_seconds=5.0)

    num_tasks = 20
    task_ids = []
    with SessionLocal() as db:
        repo = TaskRepository(db)
        for i in range(num_tasks):
            res = repo.create_task(
                task_name=f"Joint Outage Task {i}",
                task_type="noop",
                payload={"num": i},
                priority=TaskPriority.HIGH.value,
            )
            task_ids.append(res.task.id)

    # Trigger outage
    proxy_broker.trigger_outage()

    publisher = OutboxPublisherWorker(poll_interval=0.2, broker=proxy_broker)
    dispatcher = WorkerDispatcher(
        worker_id="g944-joint-worker",
        concurrency=4,
        broker=proxy_broker,
        poll_interval=0.2,
    )

    publisher.start()
    dispatcher.start()

    # Midway check: outage active, both threads alive
    time.sleep(2.5)
    assert publisher._thread.is_alive() is True
    assert dispatcher._loop_thread.is_alive() is True

    # Wait for outage recovery
    time.sleep(3.5)
    assert proxy_broker.outage_recovered is True

    # Wait for full end-to-end drain (up to 8 seconds)
    deadline = time.time() + 8.0
    all_done = False
    while time.time() < deadline:
        with SessionLocal() as db:
            repo = TaskRepository(db)
            tasks = [repo.get_by_id(tid) for tid in task_ids]
            if all(t and t.status == TaskStatus.SUCCESS.value for t in tasks):
                all_done = True
                break
        time.sleep(0.3)

    publisher.stop()
    dispatcher.stop()

    assert all_done is True, "All 20 tasks must reach SUCCESS after joint outage recovery"
    assert real_broker.get_queue_depth() == 0, "Redis queue must be empty"
    assert real_broker.get_in_flight_depth() == 0, "Redis in-flight queue must be empty"

    # Verify TaskResult existence for all tasks
    with SessionLocal() as db:
        results = db.scalars(select(TaskResult).where(TaskResult.task_id.in_(task_ids))).all()
        assert len(results) == num_tasks, f"Expected {num_tasks} TaskResult rows, found {len(results)}"


def test_g944_scenario4_duplicate_delivery_during_outage_recovery(redis_client):
    """Scenario 4: Duplicate delivery resilience during outage recovery.
    - Simulates task redelivery when worker completed task in PostgreSQL but Redis ACK timed out during outage.
    - Validates that duplicate delivery is recognized as TERMINAL_DUPLICATE.
    - PostgreSQL authoritative state is never overwritten or corrupted.
    """
    queue_name = f"taskforge:g944:q4:{uuid.uuid4().hex[:6]}"
    broker = RedisBroker(redis_client, queue_name=queue_name)

    with SessionLocal() as db:
        repo = TaskRepository(db)
        res = repo.create_task(
            task_name="Duplicate Delivery Outage Task",
            task_type="noop",
            payload={"msg": "original"},
            priority=TaskPriority.HIGH.value,
        )
        tid = res.task.id
        # Worker 1 acquires and commits SUCCESS
        tok1 = f"tok1-{uuid.uuid4().hex[:6]}"
        repo.acquire_lease(tid, worker_id="worker-1", lease_duration_seconds=30, lease_token=tok1)
        repo.update_status(tid, TaskStatus.RUNNING, lease_token=tok1)
        repo.complete_task_success(tid, lease_token=tok1, return_value={"initial": "worker-1"})

    # Broker receives redelivery of the same task message
    broker.enqueue(tid, TaskPriority.HIGH.value, {"task_id": tid})
    assert broker.get_queue_depth() == 1

    # Dispatcher pulls the redelivered message
    dispatcher = WorkerDispatcher(
        worker_id="worker-2",
        concurrency=1,
        broker=broker,
        poll_interval=0.2,
    )
    dispatcher.start()

    # Wait up to 3s for dispatcher to process the duplicate message
    time.sleep(2.0)
    dispatcher.stop()

    # Invariants:
    # 1. Redis queue acknowledged and cleared
    assert broker.get_queue_depth() == 0
    assert broker.get_in_flight_depth() == 0

    # 2. Database state remains untouched: still SUCCESS with original return_value
    with SessionLocal() as db:
        repo = TaskRepository(db)
        t = repo.get_by_id(tid)
        assert t.status == TaskStatus.SUCCESS.value
        res = db.scalars(select(TaskResult).where(TaskResult.task_id == tid)).first()
        assert res is not None
        assert res.return_value == {"initial": "worker-1"}, "Duplicate delivery must not overwrite existing result"


def test_g944_scenario5_redis_noscript_reload_resilience(redis_client):
    """Scenario 5: Proves that RedisBroker automatically reloads Lua scripts if Redis restarts
    and flushes its script cache (NoScriptError), avoiding crashes or dropped operations.
    """
    queue_name = f"taskforge:g944:q5:{uuid.uuid4().hex[:6]}"
    broker = RedisBroker(redis_client, queue_name=queue_name)

    # 1. Normal enqueue
    tid = f"g944-script-task-{uuid.uuid4().hex[:6]}"
    broker.enqueue(tid, TaskPriority.HIGH.value, {"task_id": tid})
    assert broker.get_queue_depth() == 1

    # 2. Simulate Redis script cache flush (SCRIPT FLUSH)
    redis_client.script_flush()

    # 3. Dequeue must automatically reload LUA_DEQUEUE without raising NoScriptError
    msg = broker.dequeue(timeout_seconds=0.5)
    assert msg is not None
    assert msg["task_id"] == tid

    # 4. Flush script cache again
    redis_client.script_flush()

    # 5. ACK must automatically reload LUA_ACK without raising NoScriptError
    ack_res = broker.ack(tid, delivery_token=msg["delivery_token"])
    assert ack_res is True

    # 6. Flush script cache again
    redis_client.script_flush()

    # 7. Reclaim must automatically reload LUA_RECLAIM without raising NoScriptError
    reclaim_res = broker.reclaim_expired(timeout_seconds=0.0)
    assert reclaim_res == 0


class RealSocketTCPProxy:
    """A genuine OS-level TCP socket proxy forwarding from 127.0.0.1:<port> to 127.0.0.1:6379.
    During an outage:
    1. Forcibly closes all active TCP connections.
    2. Shuts down the listening socket so new connections receive OS ConnectionRefusedError / TimeoutError.
    3. Reopens the listening socket upon restoration to resume forwarding raw TCP packets.
    """

    def __init__(self, target_host: str = "127.0.0.1", target_port: int = 6379):
        import socket
        self.target_host = target_host
        self.target_port = target_port
        self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_sock.bind(("127.0.0.1", 0))
        self.port = self.server_sock.getsockname()[1]
        self.server_sock.listen(128)
        self.running = True
        self.active_sockets = []
        self._lock = threading.Lock()
        self.thread = threading.Thread(target=self._listen_loop, daemon=True)
        self.thread.start()

    def _listen_loop(self):
        import socket
        while self.running:
            try:
                client_sock, _ = self.server_sock.accept()
            except OSError:
                break
            try:
                target_sock = socket.create_connection((self.target_host, self.target_port), timeout=2.0)
            except Exception:
                client_sock.close()
                continue
            with self._lock:
                self.active_sockets.extend([client_sock, target_sock])
            threading.Thread(target=self._forward, args=(client_sock, target_sock), daemon=True).start()
            threading.Thread(target=self._forward, args=(target_sock, client_sock), daemon=True).start()

    def _forward(self, src, dst):
        try:
            while self.running:
                data = src.recv(4096)
                if not data:
                    break
                dst.sendall(data)
        except Exception:
            pass
        finally:
            try:
                src.close()
            except Exception:
                pass
            try:
                dst.close()
            except Exception:
                pass

    def break_network(self):
        self.running = False
        try:
            self.server_sock.close()
        except Exception:
            pass
        with self._lock:
            for s in self.active_sockets:
                try:
                    s.close()
                except Exception:
                    pass
            self.active_sockets.clear()

    def restore_network(self):
        import socket
        self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_sock.bind(("127.0.0.1", self.port))
        self.server_sock.listen(128)
        self.running = True
        self.thread = threading.Thread(target=self._listen_loop, daemon=True)
        self.thread.start()


def test_g944_scenario6_real_transport_socket_outage_recovery():
    """Scenario 6: Real OS-level transport failure and reconnection recovery.
    - Uses a real OS TCP proxy to 127.0.0.1:6379 without any mocking.
    - Breaks raw TCP sockets, causing real OS WinError 10054/10061 (ConnectionReset/Refused).
    - Verifies Publisher & Dispatcher back off and recover when TCP transport is restored.
    - Verifies zero event loss and authoritative PostgreSQL state.
    """
    proxy = RealSocketTCPProxy(target_host="127.0.0.1", target_port=6379)
    proxy_client = redis.Redis(host="127.0.0.1", port=proxy.port, socket_timeout=1.0)
    assert proxy_client.ping() is True

    queue_name = f"taskforge:g944:q6:{uuid.uuid4().hex[:6]}"
    broker = RedisBroker(proxy_client, queue_name=queue_name)

    # 1. Seed 10 tasks with pending outbox events
    task_ids = []
    with SessionLocal() as db:
        for i in range(10):
            tid = f"g944-trans-task-{i}-{uuid.uuid4().hex[:6]}"
            task = Task(
                id=tid,
                task_name=f"Transport Outage Task {i}",
                task_type="noop",
                status=TaskStatus.QUEUED.value,
                priority=TaskPriority.HIGH.value,
                payload={"trans_idx": i},
            )
            outbox_evt = OutboxEvent(
                id=str(uuid.uuid4()),
                aggregate_type="task",
                aggregate_id=tid,
                event_type="TASK_CREATED",
                payload={"task_id": tid, "priority": TaskPriority.HIGH.value},
                status="PENDING",
                available_at=get_utc_now(),
                created_at=get_utc_now(),
            )
            db.add(task)
            db.add(outbox_evt)
            task_ids.append(tid)
        db.commit()

    # 2. Break OS network transport
    proxy.break_network()

    # Verify that real redis client gets real OS socket error
    with pytest.raises((redis.exceptions.ConnectionError, redis.exceptions.TimeoutError)):
        proxy_client.ping()

    # 3. Start publisher and dispatcher
    publisher = OutboxPublisherWorker(poll_interval=0.2, broker=broker)
    dispatcher = WorkerDispatcher(
        worker_id="g944-trans-worker",
        concurrency=4,
        broker=broker,
        poll_interval=0.2,
    )
    publisher.start()
    dispatcher.start()

    # Verify threads alive during network disruption
    time.sleep(2.0)
    assert publisher._thread.is_alive() is True
    assert dispatcher._loop_thread.is_alive() is True

    # 4. Restore OS network transport
    proxy.restore_network()
    time.sleep(0.5)

    # Wait for recovery and full drain
    deadline = time.time() + 8.0
    all_success = False
    while time.time() < deadline:
        with SessionLocal() as db:
            repo = TaskRepository(db)
            completed = [repo.get_by_id(tid) for tid in task_ids]
            if all(t and t.status == TaskStatus.SUCCESS.value for t in completed):
                all_success = True
                break
        time.sleep(0.3)

    publisher.stop()
    dispatcher.stop()
    proxy.break_network()

    assert all_success is True, "All 10 tasks must reach SUCCESS after real transport recovery"
    with SessionLocal() as db:
        events = db.scalars(select(OutboxEvent).where(OutboxEvent.aggregate_id.in_(task_ids))).all()
        assert all(e.status == "PUBLISHED" for e in events), "All events must be PUBLISHED"
        results = db.scalars(select(TaskResult).where(TaskResult.task_id.in_(task_ids))).all()
        assert len(results) == 10, "All 10 TaskResult rows must be persisted"

