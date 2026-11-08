"""Helper worker script for Milestone G8.3 OS process crash drills.

Spawns a worker and injects an abrupt OS process termination (via os._exit or sys.exit)
at a specific crash point passed via CLI argument.
"""

import argparse
import os
import time

from taskforge.core.state_machine import TaskStatus
from taskforge.database.repositories.task_repository import TaskRepository
from taskforge.database.session import SessionLocal
from taskforge.queue.broker import RedisBroker
from taskforge.queue.redis_client import create_redis_client
from taskforge.workers.execution_engine import execute_task
from taskforge.workers.lease_manager import LeaseManager


def run_crash_drill(crash_point: str, queue_name: str, task_id: str, visibility_timeout: int = 2):
    redis_client = create_redis_client()
    broker = RedisBroker(
        redis_client, queue_name=queue_name, visibility_timeout_seconds=visibility_timeout
    )

    if crash_point == "CP-01":
        # CP-01: Worker killed before claiming/dequeueing from the queue
        # Process halts immediately without touching the queue
        os._exit(41)

    elif crash_point == "CP-02":
        # CP-02: Dequeue into in_flight, then abruptly crash before lease acquisition
        msg = broker.dequeue(timeout_seconds=1.0)
        if msg:
            # Immediate hard OS crash (no finally, no atexit)
            os._exit(42)

    elif crash_point == "CP-03":
        # CP-03: Dequeue, acquire lease and mark RUNNING, then abruptly crash before handler
        msg = broker.dequeue(timeout_seconds=1.0)
        if msg:
            lease = LeaseManager(task_id, "crash-worker-cp03", lease_duration=2)
            if lease.acquire():
                with SessionLocal() as db:
                    repo = TaskRepository(db)
                    repo.update_status(
                        task_id,
                        TaskStatus.RUNNING,
                        {"worker": "crash-worker-cp03"},
                        lease_token=lease.lease_token,
                    )
                # Hard crash before handler execution
                os._exit(43)

    elif crash_point == "CP-04":
        # CP-04: Killed mid-handler execution while holding lease and in-flight
        msg = broker.dequeue(timeout_seconds=1.0)
        if msg:
            lease = LeaseManager(task_id, "crash-worker-cp04", lease_duration=2)
            if lease.acquire():
                with SessionLocal() as db:
                    repo = TaskRepository(db)
                    repo.update_status(
                        task_id,
                        TaskStatus.RUNNING,
                        {"worker": "crash-worker-cp04"},
                        lease_token=lease.lease_token,
                    )
                # Simulating computation, then hard crash
                time.sleep(0.1)
                os._exit(44)

    elif crash_point == "CP-05":
        # CP-05: Handler returned value, but process killed before record_result() commit
        msg = broker.dequeue(timeout_seconds=1.0)
        if msg:
            lease = LeaseManager(task_id, "crash-worker-cp05", lease_duration=2)
            if lease.acquire():
                with SessionLocal() as db:
                    repo = TaskRepository(db)
                    repo.update_status(
                        task_id,
                        TaskStatus.RUNNING,
                        {"worker": "crash-worker-cp05"},
                        lease_token=lease.lease_token,
                    )
                # Handler finishes computation, then process abruptly killed before repo.record_result():
                os._exit(45)

    elif crash_point == "CP-06":
        # CP-06: Result committed in DB, but process killed before broker.ack()
        msg = broker.dequeue(timeout_seconds=1.0)
        if msg:
            # Execute task to completion (persisting result in DB)
            execute_task(task_id, "crash-worker-cp06")
            # Crash without calling broker.ack()!
            os._exit(46)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--crash-point", required=True)
    parser.add_argument("--queue-name", required=True)
    parser.add_argument("--task-id", required=True)
    parser.add_argument("--visibility-timeout", type=int, default=2)
    args = parser.parse_args()

    run_crash_drill(args.crash_point, args.queue_name, args.task_id, args.visibility_timeout)
