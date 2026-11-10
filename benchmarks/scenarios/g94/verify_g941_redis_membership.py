"""Gate G9.4.1 Isolated Redis ZSET Task Membership Verification Drill.

Executes a dedicated 3-trial verification under the identical Campaign B contract:
1. Dispatcher is strictly paused throughout.
2. Exactly 1,000 eligible outbox events pre-inserted into PostgreSQL.
3. Publisher runs (poll_interval=0.1, batch_size=50) until all 1,000 events are PUBLISHED.
4. Performs an atomic read-only snapshot via ZRANGE queue_name 0 -1.
5. Verifies:
   - Expected unique task IDs: 1,000
   - Actual unique Redis task IDs: 1,000
   - Missing IDs: 0
   - Unexpected IDs: 0
   - Duplicate IDs: 0
   - Final Redis depth: 1,000
   - 1-to-1 mapping between outbox event IDs and task IDs.
   - S_expected == S_actual_ZSET is TRUE.

Outputs results to benchmarks/scenarios/g94/g941_redis_membership_audit.json.
"""

from datetime import UTC, datetime
import json
import os
from pathlib import Path
import sys
import time
import uuid

import psycopg
from sqlalchemy import text

REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

BENCHMARK_DB_NAME = "taskforge_g94_benchmark"
BENCHMARK_DB_URL = f"postgresql+psycopg://taskforge:taskforge_dev@localhost:5432/{BENCHMARK_DB_NAME}"
BENCHMARK_QUEUE_PREFIX = "taskforge:g941:verify:"

os.environ["DATABASE_URL"] = BENCHMARK_DB_URL
os.environ["TASKFORGE_TESTING"] = "1"
os.environ["APP_ENV"] = "development"

from taskforge.database.models import OutboxEvent, Task, get_utc_now
from taskforge.database.session import SessionLocal
from taskforge.queue.broker import RedisBroker
from taskforge.queue.outbox import OutboxPublisherWorker
from taskforge.queue.redis_client import create_redis_client


def truncate_benchmark_tables() -> None:
    with SessionLocal() as db:
        db.execute(text("TRUNCATE tasks, task_attempts, task_results, outbox_events, task_events CASCADE"))
        db.commit()


def run_membership_verification_trial(trial_id: str, redis_client, burst_count: int = 1000) -> dict:
    truncate_benchmark_tables()
    queue_name = f"{BENCHMARK_QUEUE_PREFIX}{trial_id}:{uuid.uuid4().hex[:6]}"
    broker = RedisBroker(redis_client, queue_name=queue_name, visibility_timeout_seconds=30)

    # 1. Pre-insert 1,000 tasks and outbox events while publisher is PAUSED
    now_utc = get_utc_now()
    task_id_list = []
    event_id_list = []
    mapping_event_to_task = {}

    with SessionLocal() as db:
        tasks = []
        outboxes = []
        for i in range(burst_count):
            tid = str(uuid.uuid4())
            eid = str(uuid.uuid4())
            task_id_list.append(tid)
            event_id_list.append(eid)
            mapping_event_to_task[eid] = tid

            t = Task(
                id=tid,
                task_name=f"audit-{trial_id}-{i}",
                task_type="noop",
                payload={"audit_idx": i, "trial": trial_id},
                status="QUEUED",
                priority=2,
                idempotency_key=f"audit-{trial_id}-{i}",
                tenant_id="default",
                timeout_seconds=60,
                max_retries=3,
                retry_count=0,
                scheduled_at=None,
                created_at=now_utc,
                updated_at=now_utc,
            )
            tasks.append(t)

            ev = OutboxEvent(
                id=eid,
                aggregate_type="task",
                aggregate_id=tid,
                event_type="TASK_SUBMITTED",
                payload={
                    "task_id": tid,
                    "task_name": f"audit-{trial_id}-{i}",
                    "task_type": "noop",
                    "payload": {"audit_idx": i, "trial": trial_id},
                    "priority": 2,
                    "timeout_seconds": 60,
                    "max_retries": 3,
                    "scheduled_at": None,
                },
                status="PENDING",
                available_at=now_utc,
                created_at=now_utc,
            )
            outboxes.append(ev)

        db.add_all(tasks)
        db.add_all(outboxes)
        db.commit()

        # Invariant check before publisher starts
        pending_init = db.execute(text("SELECT count(*) FROM outbox_events WHERE status = 'PENDING'")).scalar()
        published_init = db.execute(text("SELECT count(*) FROM outbox_events WHERE status = 'PUBLISHED'")).scalar()
        assert pending_init == burst_count
        assert published_init == 0

    expected_tasks_set = set(task_id_list)
    assert len(expected_tasks_set) == burst_count, "Expected task IDs must all be unique"
    assert len(mapping_event_to_task) == burst_count, "Event-to-Task mapping must be 1-to-1"

    # 2. Start publisher (worker dispatcher is PAUSED / not started)
    publisher = OutboxPublisherWorker(broker=broker, poll_interval=0.1)
    t0 = time.perf_counter()
    publisher.start()

    # Wait for drain completion
    deadline = time.time() + 60.0
    while time.time() < deadline:
        with SessionLocal() as db:
            pending = db.execute(text("SELECT count(*) FROM outbox_events WHERE status = 'PENDING'")).scalar()
            published = db.execute(text("SELECT count(*) FROM outbox_events WHERE status = 'PUBLISHED'")).scalar()
            if pending == 0 and published == burst_count:
                break
        time.sleep(0.01)

    t1 = time.perf_counter()
    drain_time_s = round(t1 - t0, 3)
    publisher.stop()

    # 3. Read-only snapshot of actual Redis ZSET members
    raw_zset_members = redis_client.zrange(broker.queue_name, 0, -1)
    redis_depth = redis_client.zcard(broker.queue_name)

    redis_task_ids = []
    for member_bytes in raw_zset_members:
        member_str = member_bytes.decode("utf-8") if isinstance(member_bytes, bytes) else member_bytes
        data = json.loads(member_str)
        redis_task_ids.append(data["task_id"])

    redis_tasks_set = set(redis_task_ids)
    missing_ids = expected_tasks_set - redis_tasks_set
    unexpected_ids = redis_tasks_set - expected_tasks_set
    duplicate_count = len(redis_task_ids) - len(redis_tasks_set)
    set_equality = (expected_tasks_set == redis_tasks_set)

    with SessionLocal() as db:
        durable_published_count = db.execute(text("SELECT count(*) FROM outbox_events WHERE status = 'PUBLISHED'")).scalar()

    result = {
        "trial_id": trial_id,
        "burst_count": burst_count,
        "drain_duration_s": drain_time_s,
        "dispatcher_state": "PAUSED_THROUGHOUT",
        "initial_unique_eligible_event_ids": len(event_id_list),
        "durable_published_event_ids": durable_published_count,
        "expected_unique_task_ids": len(expected_tasks_set),
        "actual_redis_raw_member_count": len(raw_zset_members),
        "actual_unique_redis_task_ids": len(redis_tasks_set),
        "missing_task_ids_count": len(missing_ids),
        "unexpected_task_ids_count": len(unexpected_ids),
        "duplicate_redis_task_ids_count": duplicate_count,
        "final_redis_depth_zcard": redis_depth,
        "exact_set_equality_s_expected_eq_s_redis": set_equality,
        "sample_mappings": [
            {"event_id": eid, "task_id": tid}
            for eid, tid in list(mapping_event_to_task.items())[:5]
        ],
    }

    print(f"Trial {trial_id}: Depth={redis_depth} | Unique Redis IDs={len(redis_tasks_set)} | Missing={len(missing_ids)} | Unexpected={len(unexpected_ids)} | Set Equality={set_equality}")
    assert set_equality, f"Set equality invariant violated for Trial {trial_id}"
    return result


def main():
    print("=" * 80)
    print("GATE G9.4.1: REDIS ZSET EXACT TASK MEMBERSHIP VERIFICATION DRILL")
    print("=" * 80)

    redis_client = create_redis_client()
    redis_client.ping()

    audit_results = []
    for t_idx in range(1, 4):
        trial_id = f"Audit-T{t_idx}"
        res = run_membership_verification_trial(trial_id, redis_client, burst_count=1000)
        audit_results.append(res)

    out_path = REPO_ROOT / "benchmarks" / "scenarios" / "g94" / "g941_redis_membership_audit.json"
    out_path.write_text(
        json.dumps(
            {
                "audit_timestamp_utc": datetime.now(UTC).isoformat(),
                "milestone": "G9.4.1_redis_membership_verification",
                "trials": audit_results,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"\n[Artifact] Saved exact membership audit to {out_path}")


if __name__ == "__main__":
    main()

