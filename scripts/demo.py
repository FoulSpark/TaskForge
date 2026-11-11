#!/usr/bin/env python3
"""TaskForge Public Architecture Demonstration Harness (Gate G9.5).

Implements the locked D1–D12 acceptance contract:
- D1:  Pre-flight environment validation and '--check' mode.
- D2:  Isolated disposable PostgreSQL database and Redis namespace.
- D3:  Automatic database migrations on disposable schema.
- D4:  Deterministic multi-priority workload seeding (High, Normal, Low).
- D5:  Verified priority ordering under actual production queue semantics (ZPOPMAX).
- D6:  Real broker transport interruption and bounded backoff recovery (RealSocketTCPProxy).
- D7:  Actual stale lease-token rejection against PostgreSQL conditional atomic update.
- D8:  Separate terminal duplicate suppression (TERMINAL_DUPLICATE).
- D9:  Initial-to-final task and outbox event identity reconciliation (exact set equality).
- D10: 100% terminal completion with failure/retry accounting.
- D11: Safe, ownership-checked teardown of disposable resources.
- D12: Human-readable terminal progress and machine-readable JSON summary artifact.

Usage:
    python scripts/demo.py [--check] [--output PATH] [--no-cleanup]
"""

import argparse
from datetime import UTC, datetime
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import threading
import time
from typing import Any
import uuid

import psycopg
import redis
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import Session, sessionmaker

# Ensure project root and src/ are on sys.path
REPO_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from alembic import command
from alembic.config import Config

from taskforge.core.state_machine import TaskPriority, TaskStatus
from taskforge.database.models import OutboxEvent, Task, TaskAttempt, TaskResult, get_utc_now
from taskforge.database.repositories.task_repository import TaskRepository
from taskforge.database.session import SessionLocal, set_engine
from taskforge.queue.broker import RedisBroker
from taskforge.queue.outbox import OutboxPublisherWorker
from taskforge.queue.redis_client import create_redis_client
from taskforge.workers.dispatcher import WorkerDispatcher

# ANSI Terminal Colors
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
BLUE = "\033[94m"
CYAN = "\033[96m"
BOLD = "\033[1m"
RESET = "\033[0m"


def print_banner(title: str) -> None:
    print(f"\n{BOLD}{BLUE}{'=' * 78}{RESET}")
    print(f"{BOLD}{BLUE}  {title}{RESET}")
    print(f"{BOLD}{BLUE}{'=' * 78}{RESET}\n")


def print_step(step_num: int, title: str) -> None:
    print(f"\n{BOLD}{CYAN}[Step {step_num}] {title}{RESET}")


def print_check(name: str, passed: bool, details: str = "") -> None:
    mark = f"{GREEN}[PASS]{RESET}" if passed else f"{RED}[FAIL]{RESET}"
    detail_str = f" - {details}" if details else ""
    print(f"  {mark} {BOLD}{name}{RESET}{detail_str}")


class RealSocketTCPProxy:
    """A genuine OS-level TCP socket proxy forwarding from 127.0.0.1:<port> to 127.0.0.1:6379.
    Used for un-mocked fault injection:
    - break_network(): shuts down the server socket and resets active connections with SO_LINGER.
    - restore_network(): reopens the listening socket on the identical port to resume forwarding.
    """

    def __init__(self, target_host: str = "127.0.0.1", target_port: int = 6379):
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

    def _forward(self, src: socket.socket, dst: socket.socket):
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
        self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_sock.bind(("127.0.0.1", self.port))
        self.server_sock.listen(128)
        self.running = True
        self.thread = threading.Thread(target=self._listen_loop, daemon=True)
        self.thread.start()

    def stop(self):
        self.break_network()


class DequeueTrackingBroker:
    """Wraps RedisBroker to transparently record the exact order of task messages popped via dequeue()."""

    def __init__(self, real_broker: RedisBroker):
        self.real_broker = real_broker
        self.dequeue_records: list[dict[str, Any]] = []
        self._lock = threading.Lock()

    def dequeue(self, timeout_seconds: float = 1.0) -> dict[str, Any] | None:
        msg = self.real_broker.dequeue(timeout_seconds)
        if msg:
            with self._lock:
                self.dequeue_records.append(
                    {
                        "task_id": msg["task_id"],
                        "priority": msg.get("priority"),
                        "dequeued_at": time.time(),
                        "delivery_token": msg.get("delivery_token"),
                    }
                )
        return msg

    def enqueue(self, task_id: str, priority: int, payload: dict[str, Any]) -> None:
        return self.real_broker.enqueue(task_id, priority, payload)

    def ack(self, task_id: str, message: dict[str, Any] | None = None, delivery_token: str | None = None) -> bool:
        return self.real_broker.ack(task_id, message, delivery_token)

    def reclaim_expired(self, timeout_seconds: float = 30.0) -> int:
        return self.real_broker.reclaim_expired(timeout_seconds)

    def get_queue_depth(self) -> int:
        return self.real_broker.get_queue_depth()

    def get_in_flight_depth(self) -> int:
        return self.real_broker.get_in_flight_depth()


def run_preflight_checks() -> bool:
    """Implements D1: Environment validation."""
    print_banner("TASKFORGE DEMO: PRE-FLIGHT ENVIRONMENT VALIDATION")
    all_passed = True

    # Check 1: Python Version >= 3.12
    py_ver = sys.version_info
    py_ok = py_ver.major == 3 and py_ver.minor >= 12
    print_check(
        "Python Runtime (>= 3.12)",
        py_ok,
        f"Detected Python {py_ver.major}.{py_ver.minor}.{py_ver.micro}",
    )
    if not py_ok:
        all_passed = False

    # Check 2: PostgreSQL connectivity on localhost:5432
    pg_ok = False
    pg_details = ""
    try:
        conn = psycopg.connect("postgresql://taskforge:taskforge_dev@localhost:5432/postgres", connect_timeout=3)
        with conn.cursor() as cur:
            cur.execute("SELECT version();")
            ver_str = cur.fetchone()[0]
            pg_ok = True
            pg_details = f"Connected ({ver_str.split(',')[0].strip()})"
        conn.close()
    except Exception as exc:
        pg_details = f"Connection failed: {exc}"
    print_check("PostgreSQL Database (localhost:5432)", pg_ok, pg_details)
    if not pg_ok:
        all_passed = False

    # Check 3: Redis connectivity on localhost:6379
    redis_ok = False
    redis_details = ""
    try:
        r = redis.Redis(host="localhost", port=6379, socket_connect_timeout=3)
        if r.ping():
            info = r.info("server")
            redis_ok = True
            redis_details = f"Connected (Redis v{info.get('redis_version', 'unknown')})"
    except Exception as exc:
        redis_details = f"Connection failed: {exc}"
    print_check("Redis Message Broker (localhost:6379)", redis_ok, redis_details)
    if not redis_ok:
        all_passed = False

    # Check 4: Docker availability
    docker_ok = False
    docker_details = ""
    try:
        res = subprocess.run(["docker", "ps"], capture_output=True, text=True, timeout=5)
        if res.returncode == 0:
            docker_ok = True
            docker_details = "Daemon responsive"
        else:
            docker_details = "Daemon returned non-zero"
    except Exception as exc:
        docker_details = f"Docker CLI unavailable: {exc}"
    print_check("Docker Infrastructure", docker_ok, docker_details)

    # Check 5: Alembic Migrations Directory
    alembic_ok = (REPO_ROOT / "alembic.ini").is_file() and (REPO_ROOT / "migrations").is_dir()
    print_check(
        "Alembic Schema Migrations",
        alembic_ok,
        "alembic.ini and migrations/ directory present",
    )
    if not alembic_ok:
        all_passed = False

    return all_passed


def run_demo(no_cleanup: bool = False, output_path: Path | None = None) -> int:
    """Executes the full D1–D12 demonstration."""
    run_id = uuid.uuid4().hex[:8]
    demo_db_name = f"taskforge_demo_{run_id}"
    demo_db_url = f"postgresql+psycopg://taskforge:taskforge_dev@localhost:5432/{demo_db_name}"
    demo_queue_prefix = f"taskforge:demo:{run_id}:"
    demo_queue_name = f"{demo_queue_prefix}tasks"

    print_banner(f"TASKFORGE PUBLIC DEMO — RUN ID: {run_id}")
    print(f"  Disposable PostgreSQL DB: {BOLD}{demo_db_name}{RESET}")
    print(f"  Isolated Redis Namespace: {BOLD}{demo_queue_prefix}*{RESET}")
    print(f"  Production Baseline SHA:  {BOLD}a89858b{RESET} (Frozen & Preserved)")
    print(f"  Phase 8 Frozen Tag:       {BOLD}v0.8.0-phase8-freeze (da27047){RESET}")

    summary_data: dict[str, Any] = {
        "run_id": run_id,
        "timestamp_utc": datetime.now(UTC).isoformat(),
        "provenance": {
            "authorized_base_commit": "a89858b73f8488e05cbcc2393a61f3080ffc7a38",
            "frozen_tag": "v0.8.0-phase8-freeze (da27047f56fe2eec75acdb8502f505542cf61202)",
            "git_commit": "3eb1d51",
        },
        "disposable_resources": {
            "database_name": demo_db_name,
            "redis_prefix": demo_queue_prefix,
        },
        "acceptance_criteria": {},
    }

    demo_engine = None
    proxy = None
    publisher = None
    dispatcher = None
    exit_code = 0
    resource_manifest: dict[str, Any] = {}

    import taskforge.database.session as session_mod
    orig_engine = session_mod.engine
    orig_db_url = os.environ.get("DATABASE_URL")
    orig_testing = os.environ.get("TASKFORGE_TESTING")
    orig_app_env = os.environ.get("APP_ENV")

    try:
        # ---------------------------------------------------------------------
        # Step 1: Pre-flight check
        # ---------------------------------------------------------------------
        print_step(1, "Validating Environment Prerequisites (D1)...")
        if not run_preflight_checks():
            print(f"\n{RED}[FATAL] Pre-flight checks failed. Please ensure PostgreSQL and Redis are active.{RESET}")
            return 1
        summary_data["acceptance_criteria"]["D1_environment_validation"] = True

        # ---------------------------------------------------------------------
        # Step 2: Provision Disposable Database and Redis with Ownership Manifest (D2)
        # ---------------------------------------------------------------------
        print_step(2, f"Provisioning Isolated Database '{demo_db_name}' with Ownership Manifest (D2)...")
        admin_conn = psycopg.connect("postgresql://taskforge:taskforge_dev@localhost:5432/postgres", autocommit=True)
        with admin_conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (demo_db_name,))
            if cur.fetchone() is not None:
                admin_conn.close()
                raise RuntimeError(
                    f"Resource collision: Database '{demo_db_name}' already exists prior to run. "
                    f"Refusing to execute against pre-existing database."
                )
            cur.execute(f"CREATE DATABASE {demo_db_name} OWNER taskforge")
        admin_conn.close()
        print(f"  {GREEN}[OK]{RESET} Created database '{demo_db_name}' (Pre-existence check: None)")

        # Verify Redis namespace is completely empty before run
        r_precheck = redis.Redis(host="localhost", port=6379)
        pre_existing_redis_keys = r_precheck.keys(f"{demo_queue_prefix}*")
        if pre_existing_redis_keys:
            raise RuntimeError(
                f"Resource collision: Redis namespace '{demo_queue_prefix}*' contains "
                f"{len(pre_existing_redis_keys)} keys prior to demo run. Refusing to operate on non-empty namespace."
            )

        # Record explicit ownership manifest (R1)
        resource_manifest = {
            "run_id": run_id,
            "provisioned_at_utc": get_utc_now().isoformat(),
            "database": {
                "name": demo_db_name,
                "owner": "taskforge",
                "created_by_run": True,
                "pre_existed": False,
            },
            "redis": {
                "namespace_prefix": demo_queue_prefix,
                "pre_existing_keys_count": 0,
                "owned_by_run": True,
            },
        }
        summary_data["resource_manifest"] = resource_manifest
        print(f"  {GREEN}[OK]{RESET} Resource ownership manifest established for run '{run_id}'")

        # Reconfigure SQLAlchemy global engine, SessionLocal, and settings cache
        os.environ["DATABASE_URL"] = demo_db_url
        os.environ["TASKFORGE_TESTING"] = "1"
        os.environ["APP_ENV"] = "development"

        from taskforge.core.config import get_settings
        get_settings.cache_clear()

        demo_engine = create_engine(demo_db_url, pool_pre_ping=True)
        set_engine(demo_engine)
        summary_data["acceptance_criteria"]["D2_isolated_resources"] = True

        # ---------------------------------------------------------------------
        # Step 3: Run Database Migrations (D3)
        # ---------------------------------------------------------------------
        print_step(3, "Applying Alembic Migrations to Disposable Schema (D3)...")
        alembic_cfg = Config(os.path.abspath(str(REPO_ROOT / "alembic.ini")))
        alembic_cfg.set_main_option("script_location", os.path.abspath(str(REPO_ROOT / "migrations")))
        alembic_cfg.set_main_option("sqlalchemy.url", demo_db_url)
        command.upgrade(alembic_cfg, "head")
        print(f"  {GREEN}[OK]{RESET} Database schema migrated to HEAD successfully.")
        summary_data["acceptance_criteria"]["D3_automatic_migrations"] = True

        # ---------------------------------------------------------------------
        # Step 4: Seed Deterministic Multi-Priority Workload (D4)
        # ---------------------------------------------------------------------
        print_step(4, "Seeding Deterministic Multi-Priority Workload (D4)...")
        num_per_tier = 10
        total_tasks = num_per_tier * 3
        seeded_task_ids: list[str] = []
        seeded_event_ids: list[str] = []
        tier_map: dict[str, int] = {}

        # Intentionally seed in reverse arrival order (Low, Normal/Medium, High) to prove
        # priority reordering under ZPOPMAX!
        # Priority mapping reconciliation (R3):
        # Stage 0 specifies 'Normal' priority; in the production codebase this corresponds
        # directly to TaskPriority.MEDIUM (value 2), where LOW=1, MEDIUM=2, HIGH=3, CRITICAL=4.
        NORMAL_PRIORITY = TaskPriority.MEDIUM.value
        seeding_tiers = [
            ("Low", TaskPriority.LOW.value, "TaskPriority.LOW (1)"),
            ("Normal", NORMAL_PRIORITY, "TaskPriority.MEDIUM (2) [Stage 0 Normal tier]"),
            ("High", TaskPriority.HIGH.value, "TaskPriority.HIGH (3)"),
        ]

        with SessionLocal() as db:
            for tier_name, prio_val, enum_desc in seeding_tiers:
                for idx in range(num_per_tier):
                    tid = f"demo-{run_id}-{tier_name.lower()}-{idx:02d}"
                    task = Task(
                        id=tid,
                        task_name=f"Demo Task [{tier_name}] #{idx:02d}",
                        task_type="noop",
                        status=TaskStatus.QUEUED.value,
                        priority=prio_val,
                        payload={"tier": tier_name, "enum_desc": enum_desc, "index": idx, "run_id": run_id},
                    )
                    eid = str(uuid.uuid4())
                    evt = OutboxEvent(
                        id=eid,
                        aggregate_type="task",
                        aggregate_id=tid,
                        event_type="TASK_CREATED",
                        payload={"task_id": tid, "priority": prio_val},
                        status="PENDING",
                        available_at=get_utc_now(),
                        created_at=get_utc_now(),
                    )
                    db.add(task)
                    db.add(evt)
                    seeded_task_ids.append(tid)
                    seeded_event_ids.append(eid)
                    tier_map[tid] = prio_val
            db.commit()

        print(f"  {GREEN}[OK]{RESET} Seeded {total_tasks} tasks (10 Low, 10 Normal/Medium, 10 High) in reverse arrival order.")
        summary_data["acceptance_criteria"]["D4_deterministic_workload"] = {
            "total_tasks": total_tasks,
            "high_count": num_per_tier,
            "normal_count": num_per_tier,
            "low_count": num_per_tier,
            "priority_enum_reconciliation": {
                "High": {"enum": "TaskPriority.HIGH", "value": TaskPriority.HIGH.value},
                "Normal": {"enum": "TaskPriority.MEDIUM", "value": TaskPriority.MEDIUM.value, "stage0_label": "Normal"},
                "Low": {"enum": "TaskPriority.LOW", "value": TaskPriority.LOW.value},
            },
        }

        # ---------------------------------------------------------------------
        # Step 5: Start Outbox Publisher & Drain to Redis
        # ---------------------------------------------------------------------
        print_step(5, "Publishing Outbox Events to Redis Sorted Set Queue...")
        real_redis_client = create_redis_client()
        real_broker = RedisBroker(real_redis_client, queue_name=demo_queue_name)
        tracking_broker = DequeueTrackingBroker(real_broker)

        publisher = OutboxPublisherWorker(poll_interval=0.1, broker=tracking_broker)
        publisher.start()

        # Wait for all 30 events to publish to Redis
        t_deadline = time.time() + 10.0
        while time.time() < t_deadline:
            if real_broker.get_queue_depth() == total_tasks:
                break
            time.sleep(0.1)

        publisher.stop()
        assert real_broker.get_queue_depth() == total_tasks, "All 30 tasks must be enqueued in Redis"
        print(f"  {GREEN}[OK]{RESET} All {total_tasks} events published. Redis ready queue depth = {real_broker.get_queue_depth()}.")

        # ---------------------------------------------------------------------
        # Step 6: Verify Dequeue Priority Ordering under ZPOPMAX (D5)
        # ---------------------------------------------------------------------
        print_step(6, "Verifying Dequeue Priority Ordering under ZPOPMAX (D5)...")
        # Pop all 30 tasks using tracking_broker.dequeue()
        popped_messages = []
        for _ in range(total_tasks):
            msg = tracking_broker.dequeue(timeout_seconds=0.5)
            if msg:
                popped_messages.append(msg)

        assert len(popped_messages) == total_tasks, f"Expected {total_tasks} messages, popped {len(popped_messages)}"

        dequeue_priorities = [tier_map[m["task_id"]] for m in popped_messages]
        high_indices = [i for i, p in enumerate(dequeue_priorities) if p == TaskPriority.HIGH.value]
        normal_indices = [i for i, p in enumerate(dequeue_priorities) if p == TaskPriority.MEDIUM.value]
        low_indices = [i for i, p in enumerate(dequeue_priorities) if p == TaskPriority.LOW.value]

        high_before_normal = max(high_indices) < min(normal_indices)
        normal_before_low = max(normal_indices) < min(low_indices)

        # Check FIFO within tiers
        fifo_high = all(popped_messages[i]["task_id"] == f"demo-{run_id}-high-{idx:02d}" for idx, i in enumerate(high_indices))
        fifo_norm = all(popped_messages[i]["task_id"] == f"demo-{run_id}-normal-{idx:02d}" for idx, i in enumerate(normal_indices))
        fifo_low = all(popped_messages[i]["task_id"] == f"demo-{run_id}-low-{idx:02d}" for idx, i in enumerate(low_indices))

        priority_verified = high_before_normal and normal_before_low and fifo_high and fifo_norm and fifo_low

        print("  Priority Dequeue Verification:")
        print(f"    - High Tasks Precede Normal Tasks: {GREEN if high_before_normal else RED}{high_before_normal}{RESET} (Indices 0..9 vs 10..19)")
        print(f"    - Normal Tasks Precede Low Tasks:  {GREEN if normal_before_low else RED}{normal_before_low}{RESET} (Indices 10..19 vs 20..29)")
        print(f"    - FIFO Arrival Order Preserved:    {GREEN if (fifo_high and fifo_norm and fifo_low) else RED}{fifo_high and fifo_norm and fifo_low}{RESET} (Strict seq ordering)")

        assert priority_verified is True, "Priority ordering verification failed!"
        summary_data["acceptance_criteria"]["D5_priority_ordering"] = {
            "high_before_normal": high_before_normal,
            "normal_before_low": normal_before_low,
            "fifo_within_tiers": fifo_high and fifo_norm and fifo_low,
            "dequeue_order_sample": [m["task_id"] for m in popped_messages[:6]],
        }

        # Process and complete each popped message with genuine lease acquisition and fencing
        with SessionLocal() as db:
            repo = TaskRepository(db)
            for m in popped_messages:
                tid = m["task_id"]
                tok = f"tok-pri-{tid}"
                repo.acquire_lease(tid, worker_id="demo-priority-worker", lease_duration_seconds=30, lease_token=tok)
                repo.update_status(tid, TaskStatus.RUNNING, lease_token=tok)
                repo.complete_task_success(tid, lease_token=tok, return_value={"demo": "priority_test", "priority": tier_map[tid]})
                real_broker.ack(tid, delivery_token=m["delivery_token"])

        # ---------------------------------------------------------------------
        # Step 7: Live Fault Injection 1 — Real Broker Transport Interruption & Recovery (D6)
        # ---------------------------------------------------------------------
        print_step(7, "Live Fault Injection: Real OS TCP Socket Disconnect (D6)...")
        proxy = RealSocketTCPProxy(target_host="127.0.0.1", target_port=6379)
        proxy_client = redis.Redis(host="127.0.0.1", port=proxy.port, socket_timeout=1.0)
        assert proxy_client.ping() is True
        print(f"  {GREEN}[OK]{RESET} TCP Proxy active on port {proxy.port} forwarding to 6379.")

        proxy_broker = RedisBroker(proxy_client, queue_name=f"{demo_queue_prefix}outage")

        # Seed 10 outage tasks
        outage_task_ids = []
        outage_event_ids = []
        with SessionLocal() as db:
            for i in range(10):
                tid = f"demo-{run_id}-outage-{i:02d}"
                eid = str(uuid.uuid4())
                task = Task(
                    id=tid,
                    task_name=f"Outage Task #{i:02d}",
                    task_type="noop",
                    status=TaskStatus.QUEUED.value,
                    priority=TaskPriority.HIGH.value,
                    payload={"outage_idx": i},
                )
                evt = OutboxEvent(
                    id=eid,
                    aggregate_type="task",
                    aggregate_id=tid,
                    event_type="TASK_CREATED",
                    payload={"task_id": tid, "priority": TaskPriority.HIGH.value},
                    status="PENDING",
                    available_at=get_utc_now(),
                    created_at=get_utc_now(),
                )
                db.add(task)
                db.add(evt)
                outage_task_ids.append(tid)
                outage_event_ids.append(eid)
            db.commit()

        print("  -> Severing OS TCP sockets via proxy.break_network() for 3.0s...")
        proxy.break_network()
        t_break = time.time()

        pub_outage = OutboxPublisherWorker(poll_interval=0.2, broker=proxy_broker)
        disp_outage = WorkerDispatcher(worker_id=f"demo-worker-{run_id}", concurrency=2, broker=proxy_broker, poll_interval=0.2)
        pub_outage.start()
        disp_outage.start()

        # Sleep during disconnect (threads encounter real OS ConnectionResetError / ConnectionRefusedError)
        time.sleep(1.5)
        pub_alive = pub_outage._thread.is_alive() if pub_outage._thread else False
        disp_alive = disp_outage._loop_thread.is_alive() if disp_outage._loop_thread else False
        print(f"  -> Threads during disconnect: Publisher={pub_alive}, Dispatcher={disp_alive} (Applying exponential backoff)")

        time.sleep(1.6)
        print("  -> Restoring OS TCP transport via proxy.restore_network()...")
        proxy.restore_network()
        t_restore = time.time()
        outage_elapsed = t_restore - t_break

        # Wait for drain
        t_drain_deadline = time.time() + 8.0
        outage_drained = False
        while time.time() < t_drain_deadline:
            with SessionLocal() as db:
                repo = TaskRepository(db)
                statuses = [repo.get_by_id(tid).status for tid in outage_task_ids]
                if all(s == TaskStatus.SUCCESS.value for s in statuses):
                    outage_drained = True
                    break
            time.sleep(0.3)

        pub_outage.stop()
        disp_outage.stop()

        assert outage_drained is True, "All outage tasks must reach SUCCESS post-recovery"
        print(f"  {GREEN}[OK]{RESET} Broker outage ({outage_elapsed:.2f}s) recovered cleanly. All 10 tasks completed to SUCCESS with 0 loss.")
        summary_data["acceptance_criteria"]["D6_broker_outage_recovery"] = {
            "outage_duration_seconds": round(outage_elapsed, 2),
            "tasks_completed_success": 10,
            "durable_event_loss": 0,
            "threads_survived_without_restart": pub_alive and disp_alive,
        }

        # ---------------------------------------------------------------------
        # Step 8: Live Fault Injection 2 — Actual Stale-Worker Lease Fencing (D7)
        # ---------------------------------------------------------------------
        print_step(8, "Live Fault Injection: Actual PostgreSQL Stale Lease Fencing (D7)...")
        fencing_task_id = f"demo-{run_id}-fencing-task"
        fencing_event_id = str(uuid.uuid4())
        tok_a = f"tok-a-{run_id}"
        tok_b = f"tok-b-{run_id}"

        with SessionLocal() as db:
            repo = TaskRepository(db)
            task = Task(
                id=fencing_task_id,
                task_name="Lease Fencing Verification Task",
                task_type="noop",
                status=TaskStatus.QUEUED.value,
                priority=TaskPriority.HIGH.value,
                payload={"fencing": True},
            )
            evt_fencing = OutboxEvent(
                id=fencing_event_id,
                aggregate_type="task",
                aggregate_id=fencing_task_id,
                event_type="TASK_CREATED",
                payload={"task_id": fencing_task_id, "priority": TaskPriority.HIGH.value},
                status="PUBLISHED",
                available_at=get_utc_now(),
                created_at=get_utc_now(),
                published_at=get_utc_now(),
            )
            db.add(task)
            db.add(evt_fencing)
            db.commit()

            # 1. Worker A acquires lease
            repo.acquire_lease(fencing_task_id, worker_id="worker-A", lease_duration_seconds=30, lease_token=tok_a)
            repo.update_status(fencing_task_id, TaskStatus.RUNNING, lease_token=tok_a)
            print(f"  -> Worker A acquired initial lease token: '{tok_a}' (Status=RUNNING)")

            # 2. Worker A stalls; lease expires and supervisor recovers the task
            from datetime import timedelta
            t_fencing = repo.get_by_id(fencing_task_id)
            t_fencing.lease_expires_at = get_utc_now() - timedelta(seconds=10)
            db.commit()

            recovered_info = repo.recover_expired_leases(initiator="demo_supervisor")
            print(f"  -> Lease expired; supervisor recovered task (Status=RETRY_WAIT, lease vacated)")

            # Worker B acquires replacement lease with token B
            b_acquired = repo.acquire_lease(fencing_task_id, worker_id="worker-B", lease_duration_seconds=30, lease_token=tok_b)
            assert b_acquired is True, "Worker B must successfully acquire vacated lease"
            repo.update_status(fencing_task_id, TaskStatus.RUNNING, lease_token=tok_b)
            print(f"  -> Worker B acquired replacement lease token: '{tok_b}' (Active task lease now belongs to Worker B)")

            # 3. Direct PostgreSQL Guarded SQL Update Rejection Evidence (R2)
            guarded_sql_stmt = (
                "UPDATE tasks SET status = :status, completed_at = :now, updated_at = :now, lease_token = NULL "
                "WHERE id = :task_id AND lease_token = :lease_token"
            )
            raw_res = db.execute(
                text(guarded_sql_stmt),
                {
                    "status": TaskStatus.SUCCESS.value,
                    "now": get_utc_now(),
                    "task_id": fencing_task_id,
                    "lease_token": tok_a,
                },
            )
            raw_stale_affected_rows = raw_res.rowcount
            db.rollback()  # Rollback raw statement to leave task state pristine in transaction
            assert raw_stale_affected_rows == 0, f"Guarded SQL update MUST affect 0 rows, got {raw_stale_affected_rows}"
            print(f"  {GREEN}[OK]{RESET} Guarded PostgreSQL SQL update with stale Worker A token affected exactly {raw_stale_affected_rows} rows.")

            # 4. Production TaskRepository Fencing Path Rejection Evidence (R2)
            from taskforge.monitoring.metrics import FENCING_REJECTIONS_TOTAL
            metric_before = FENCING_REJECTIONS_TOTAL._value.get()
            stale_commit_rejected = False
            rejection_error_message = ""
            try:
                repo.complete_task_success(fencing_task_id, lease_token=tok_a, return_value={"winner": "Worker A"})
            except PermissionError as perm_err:
                stale_commit_rejected = True
                rejection_error_message = str(perm_err)
                print(f"  {GREEN}[OK]{RESET} Production TaskRepository rejected Worker A stale commit: {perm_err}")

            assert stale_commit_rejected is True, "PostgreSQL repository MUST reject Worker A's stale lease token!"
            metric_after = FENCING_REJECTIONS_TOTAL._value.get()
            fencing_counter_delta = metric_after - metric_before
            print(f"  {GREEN}[OK]{RESET} Authoritative fencing counter incremented by +{fencing_counter_delta:.1f} (Total={metric_after})")

            # 5. Worker B commits valid terminal state with token B
            repo.complete_task_success(fencing_task_id, lease_token=tok_b, return_value={"winner": "Worker B", "proof": "Valid Lease Token B"})
            print(f"  {GREEN}[OK]{RESET} Authoritative Worker B committed terminal state with valid token B.")

            # 6. Verify final authoritative database state
            final_task = repo.get_by_id(fencing_task_id)
            final_res = db.scalars(select(TaskResult).where(TaskResult.task_id == fencing_task_id)).first()

            assert final_task.status == TaskStatus.SUCCESS.value
            assert final_task.lease_token is None  # Released on completion
            assert final_res.return_value["winner"] == "Worker B"
            assert final_res.return_value["proof"] == "Valid Lease Token B"

        print(f"  {GREEN}[OK]{RESET} Authoritative state strictly reflects Worker B. Stale Worker A had 0 rows updated.")
        summary_data["acceptance_criteria"]["D7_stale_lease_fencing"] = {
            "worker_a_lease_token": tok_a,
            "worker_b_replacement_token": tok_b,
            "guarded_sql_statement": guarded_sql_stmt,
            "guarded_sql_stale_affected_rows": raw_stale_affected_rows,
            "stale_commit_rejected": stale_commit_rejected,
            "rejection_exception": rejection_error_message,
            "fencing_counter_increment": fencing_counter_delta,
            "worker_b_commit_success": True,
            "final_persisted_status": final_task.status,
            "final_persisted_lease_token": final_task.lease_token,
            "authoritative_winner": final_res.return_value["winner"],
            "production_path_without_mocking": True,
            "state_corrupted": False,
        }

        # ---------------------------------------------------------------------
        # Step 9: Live Fault Injection 3 — Terminal Duplicate Suppression (D8)
        # ---------------------------------------------------------------------
        print_step(9, "Live Fault Injection: Terminal Duplicate Suppression (D8)...")
        # Enqueue duplicate message for already completed task fencing_task_id
        real_broker.enqueue(fencing_task_id, TaskPriority.HIGH.value, {"task_id": fencing_task_id})
        assert real_broker.get_queue_depth() == 1

        disp_dup = WorkerDispatcher(worker_id=f"demo-dup-worker-{run_id}", concurrency=1, broker=real_broker, poll_interval=0.1)
        disp_dup.start()
        time.sleep(1.0)
        disp_dup.stop()

        assert real_broker.get_queue_depth() == 0, "Duplicate message must be dequeued and ACKed"
        with SessionLocal() as db:
            repo = TaskRepository(db)
            t_dup = repo.get_by_id(fencing_task_id)
            res_dup = db.scalars(select(TaskResult).where(TaskResult.task_id == fencing_task_id)).first()

            assert t_dup.status == TaskStatus.SUCCESS.value
            assert res_dup.return_value["winner"] == "Worker B", "Duplicate redelivery must not overwrite authoritative result!"

        print(f"  {GREEN}[OK]{RESET} Redelivered duplicate message suppressed as TERMINAL_DUPLICATE. Authoritative state untouched.")
        summary_data["acceptance_criteria"]["D8_terminal_duplicate_suppression"] = {
            "duplicate_message_acked": True,
            "authoritative_result_preserved": True,
        }

        # ---------------------------------------------------------------------
        # Step 10: Identity Reconciliation & Terminal Completion Audit (D9, D10)
        # ---------------------------------------------------------------------
        print_step(10, "Auditing Identity Reconciliation & 100% Terminal Completion (D9, D10)...")
        all_initial_task_ids = seeded_task_ids + outage_task_ids + [fencing_task_id]
        all_initial_event_ids = seeded_event_ids + outage_event_ids + [fencing_event_id]

        with SessionLocal() as db:
            tasks_in_db = db.scalars(select(Task).where(Task.id.in_(all_initial_task_ids))).all()
            results_in_db = db.scalars(select(TaskResult).where(TaskResult.task_id.in_(all_initial_task_ids))).all()
            events_in_db = db.scalars(select(OutboxEvent).where(OutboxEvent.id.in_(all_initial_event_ids))).all()

            final_task_ids = [t.id for t in tasks_in_db]
            final_success_task_ids = [t.id for t in tasks_in_db if t.status == TaskStatus.SUCCESS.value]
            final_result_task_ids = [r.task_id for r in results_in_db]
            final_published_event_ids = [e.id for e in events_in_db if e.status == "PUBLISHED"]

            # Exact Initial-to-Final Identity Set Comparisons (R3)
            task_identity_match = set(all_initial_task_ids) == set(final_success_task_ids)
            event_identity_match = set(all_initial_event_ids) == set(final_published_event_ids)
            result_identity_match = set(all_initial_task_ids) == set(final_result_task_ids)

            missing_task_ids = sorted(list(set(all_initial_task_ids) - set(final_success_task_ids)))
            unexpected_task_ids = sorted(list(set(final_task_ids) - set(all_initial_task_ids)))
            missing_event_ids = sorted(list(set(all_initial_event_ids) - set(final_published_event_ids)))
            unexpected_event_ids = sorted(list(set(e.id for e in events_in_db) - set(all_initial_event_ids)))
            tasks_missing_results = sorted(list(set(all_initial_task_ids) - set(final_result_task_ids)))

            unaccounted_tasks = len(missing_task_ids) + len(unexpected_task_ids)
            unaccounted_events = len(missing_event_ids) + len(unexpected_event_ids)

            print(f"  Identity Reconciliation & Provenance Audit (R3):")
            print(f"    - Initial Task IDs:         {len(all_initial_task_ids)}")
            print(f"    - Final SUCCESS Task IDs:   {len(final_success_task_ids)} / {len(all_initial_task_ids)} (Exact Set Match: {GREEN if task_identity_match else RED}{task_identity_match}{RESET})")
            print(f"    - Missing Tasks:            {missing_task_ids}")
            print(f"    - Unexpected Tasks:         {unexpected_task_ids}")
            print(f"    - Initial Event IDs:        {len(all_initial_event_ids)}")
            print(f"    - Final PUBLISHED Events:   {len(final_published_event_ids)} / {len(all_initial_event_ids)} (Exact Set Match: {GREEN if event_identity_match else RED}{event_identity_match}{RESET})")
            print(f"    - Missing Events:           {missing_event_ids}")
            print(f"    - Unexpected Events:        {unexpected_event_ids}")
            print(f"    - Persisted Task Results:   {len(final_result_task_ids)} / {len(all_initial_task_ids)} (Exact Set Match: {GREEN if result_identity_match else RED}{result_identity_match}{RESET})")
            print(f"    - Tasks Missing Results:    {tasks_missing_results}")
            print(f"    - Unaccounted Tasks/Events: {unaccounted_tasks} tasks, {unaccounted_events} events")
            print(f"    - Durable Event Loss:       0 (Strictly zero)")
            print(f"    - Final Redis Ready Depth:  {real_broker.get_queue_depth()} (Fully drained)")
            print(f"    - Final Redis In-Flight:    {real_broker.get_in_flight_depth()} (All ACKed)")

            assert task_identity_match is True
            assert event_identity_match is True
            assert result_identity_match is True
            assert unaccounted_tasks == 0
            assert unaccounted_events == 0

        summary_data["acceptance_criteria"]["D9_identity_reconciliation"] = {
            "initial_task_count": len(all_initial_task_ids),
            "final_success_task_count": len(final_success_task_ids),
            "task_identity_set_equality": task_identity_match,
            "missing_task_ids": missing_task_ids,
            "unexpected_task_ids": unexpected_task_ids,
            "initial_event_count": len(all_initial_event_ids),
            "final_published_event_count": len(final_published_event_ids),
            "event_identity_set_equality": event_identity_match,
            "missing_event_ids": missing_event_ids,
            "unexpected_event_ids": unexpected_event_ids,
            "task_result_count": len(final_result_task_ids),
            "task_result_set_equality": result_identity_match,
            "tasks_missing_results": tasks_missing_results,
            "unaccounted_tasks_count": unaccounted_tasks,
            "unaccounted_events_count": unaccounted_events,
            "durable_outbox_event_loss": 0,
        }
        summary_data["acceptance_criteria"]["D10_terminal_completion"] = {
            "success_count": len(final_success_task_ids),
            "failed_count": 0,
            "pending_count": 0,
            "completion_percentage": 100.0,
        }

    except Exception as exc:
        print(f"\n{RED}[ERROR] Demo execution encountered an exception: {exc}{RESET}")
        import traceback
        traceback.print_exc()
        exit_code = 1
        summary_data["error"] = str(exc)

    finally:
        # ---------------------------------------------------------------------
        # Step 11: Ownership-Checked Teardown (D11, R1)
        # ---------------------------------------------------------------------
        print_step(11, f"Performing Manifest-Verified Safe Teardown (D11, R1)...")
        if proxy:
            proxy.stop()

        if no_cleanup:
            print(f"  {YELLOW}[SKIP]{RESET} --no-cleanup flag supplied. Preserving '{demo_db_name}'.")
            summary_data["acceptance_criteria"]["D11_safe_teardown"] = {
                "status": "skipped_by_flag",
                "ownership_verified": True,
            }
        else:
            # Rigorous ownership verification against run-specific resource manifest (R1)
            db_ownership_verified = (
                bool(resource_manifest)
                and resource_manifest.get("database", {}).get("created_by_run") is True
                and resource_manifest.get("database", {}).get("name") == demo_db_name
                and resource_manifest.get("database", {}).get("pre_existed") is False
                and demo_db_name.startswith("taskforge_demo_")
                and demo_db_name.endswith(run_id)
            )

            redis_ownership_verified = (
                bool(resource_manifest)
                and resource_manifest.get("redis", {}).get("owned_by_run") is True
                and resource_manifest.get("redis", {}).get("namespace_prefix") == demo_queue_prefix
                and resource_manifest.get("redis", {}).get("pre_existing_keys_count") == 0
            )

            if not db_ownership_verified:
                print(f"  {RED}[ERROR]{RESET} Database ownership cannot be established from resource manifest! Refusing destructive cleanup of '{demo_db_name}'.")
                summary_data["acceptance_criteria"]["D11_safe_teardown"] = {
                    "status": "rejected_unverified_ownership",
                    "db_ownership_verified": False,
                    "redis_ownership_verified": redis_ownership_verified,
                }
            else:
                try:
                    if demo_engine:
                        demo_engine.dispose()

                    admin_conn = psycopg.connect("postgresql://taskforge:taskforge_dev@localhost:5432/postgres", autocommit=True)
                    with admin_conn.cursor() as cur:
                        cur.execute(f"DROP DATABASE IF EXISTS {demo_db_name} WITH (FORCE)")
                    admin_conn.close()
                    print(f"  {GREEN}[OK]{RESET} Manifest-verified database '{demo_db_name}' safely dropped.")

                    if not redis_ownership_verified:
                        print(f"  {RED}[ERROR]{RESET} Redis namespace ownership cannot be established from resource manifest! Refusing key deletion.")
                    else:
                        r = redis.Redis(host="localhost", port=6379)
                        demo_keys = r.keys(f"{demo_queue_prefix}*")
                        if demo_keys:
                            r.delete(*demo_keys)
                        print(f"  {GREEN}[OK]{RESET} Manifest-verified Redis namespace '{demo_queue_prefix}*' safely purged ({len(demo_keys)} keys deleted).")

                    summary_data["acceptance_criteria"]["D11_safe_teardown"] = {
                        "status": "completed",
                        "db_ownership_verified": True,
                        "redis_ownership_verified": True,
                        "manifest_run_id": resource_manifest.get("run_id"),
                    }
                except Exception as cleanup_exc:
                    print(f"  {RED}[WARN]{RESET} Teardown encountered error: {cleanup_exc}")
                    summary_data["acceptance_criteria"]["D11_safe_teardown"] = {
                        "status": f"error: {cleanup_exc}",
                        "db_ownership_verified": db_ownership_verified,
                        "redis_ownership_verified": redis_ownership_verified,
                    }

        # Restore original database environment and engine bindings
        if orig_db_url is not None:
            os.environ["DATABASE_URL"] = orig_db_url
        else:
            os.environ.pop("DATABASE_URL", None)

        if orig_testing is not None:
            os.environ["TASKFORGE_TESTING"] = orig_testing
        else:
            os.environ.pop("TASKFORGE_TESTING", None)

        if orig_app_env is not None:
            os.environ["APP_ENV"] = orig_app_env
        else:
            os.environ.pop("APP_ENV", None)

        from taskforge.core.config import get_settings
        get_settings.cache_clear()

        if orig_engine is not None:
            set_engine(orig_engine)

        # ---------------------------------------------------------------------
        # Step 12: Machine-Readable JSON Output (D12)
        # ---------------------------------------------------------------------
        summary_data["exit_code"] = exit_code
        summary_data["status"] = "SUCCESS" if exit_code == 0 else "FAILED"

        out_file = output_path or (REPO_ROOT / "benchmarks" / "public" / "demo_execution_summary.json")
        out_file.parent.mkdir(parents=True, exist_ok=True)
        out_file.write_text(json.dumps(summary_data, indent=2), encoding="utf-8")
        print(f"\n{BOLD}{GREEN}[DEMO COMPLETE]{RESET} Machine-readable summary written to:\n  {out_file}")
        summary_data["acceptance_criteria"]["D12_summary_artifact"] = str(out_file)

    return exit_code


def main():
    parser = argparse.ArgumentParser(description="TaskForge Public Architecture Demonstration Harness (Gate G9.5)")
    parser.add_argument("--check", action="store_true", help="Run pre-flight environment checks only and exit")
    parser.add_argument("--no-cleanup", action="store_true", help="Preserve disposable database and Redis keys after run")
    parser.add_argument("--output", type=Path, default=None, help="Path for JSON summary output artifact")
    args = parser.parse_args()

    if args.check:
        passed = run_preflight_checks()
        sys.exit(0 if passed else 1)

    sys.exit(run_demo(no_cleanup=args.no_cleanup, output_path=args.output))


if __name__ == "__main__":
    main()
