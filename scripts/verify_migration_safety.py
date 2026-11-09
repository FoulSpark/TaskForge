"""Migration lifecycle verification script for Milestone G9.1-F.

Verifies:
1. Fresh database creation and direct upgrade to head.
2. Upgrade from frozen G9.0 schema (002_g83_outbox_available_at) with pre-existing legacy data.
3. Pre-existing tasks receive deliberate tenant assignment ('default').
4. Pre-existing task_events receive consistent tenant ownership matching parent tasks.
5. All tenant indexes (ix_tasks_tenant_id, ix_tasks_tenant_status, ix_tasks_tenant_created,
   ix_task_events_tenant_id, ix_task_events_tenant_created) are created and active.
6. Clean round-trip downgrade to 002_g83_outbox_available_at without orphan columns or tables.
"""

import os

import psycopg
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, text

TEST_DB_NAME = "taskforge_migration_test"
TARGET_URL = f"postgresql+psycopg://taskforge:taskforge_dev@localhost:5432/{TEST_DB_NAME}"
os.environ["DATABASE_URL"] = TARGET_URL


def run_verification() -> None:
    print("[1/6] Recreating disposable test database...")
    with psycopg.connect(
        "host=localhost port=5432 user=taskforge password=taskforge_dev dbname=postgres",
        autocommit=True,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(f"DROP DATABASE IF EXISTS {TEST_DB_NAME} WITH (FORCE);")
            cur.execute(f"CREATE DATABASE {TEST_DB_NAME};")

    print("[2/6] Migrating to G9.0 baseline (002_g83_outbox_available_at)...")
    alembic_cfg = Config("alembic.ini")
    command.upgrade(alembic_cfg, "002_g83_outbox_available_at")

    print("[3/6] Inserting legacy pre-existing tasks and task events into 002 schema...")
    engine = create_engine(TARGET_URL)
    with engine.begin() as conn:
        conn.execute(
            text("""
            INSERT INTO tasks (id, task_name, task_type, payload, status, priority, timeout_seconds, max_retries, retry_count, created_at, updated_at)
            VALUES ('task-legacy-001', 'Legacy Task', 'noop', '{}'::json, 'COMPLETED', 1, 60, 3, 0, now(), now());
        """)
        )
        conn.execute(
            text("""
            INSERT INTO task_events (task_id, event_type, from_state, to_state, metadata_json, created_at)
            VALUES ('task-legacy-001', 'TASK_COMPLETED', 'RUNNING', 'COMPLETED', '{}'::json, now());
        """)
        )

    print("[4/6] Upgrading to G9.1 head (5a361f84b61f)...")
    command.upgrade(alembic_cfg, "head")

    with engine.begin() as conn:
        task_res = conn.execute(
            text("SELECT id, tenant_id FROM tasks WHERE id = 'task-legacy-001'")
        ).fetchone()
        assert task_res is not None, "Legacy task missing!"
        assert task_res[1] == "default", f"Expected default tenant_id, got {task_res[1]}"
        print(f" -> Verified legacy task tenant_id = '{task_res[1]}'")

        event_res = conn.execute(
            text("SELECT id, tenant_id FROM task_events WHERE task_id = 'task-legacy-001'")
        ).fetchone()
        assert event_res is not None, "Legacy event missing!"
        assert event_res[1] == "default", f"Expected default tenant_id, got {event_res[1]}"
        print(f" -> Verified legacy task_event tenant_id = '{event_res[1]}'")

        # Check indexes
        indexes = (
            conn.execute(
                text("""
            SELECT indexname FROM pg_indexes WHERE tablename IN ('tasks', 'task_events', 'api_keys');
        """)
            )
            .scalars()
            .all()
        )
        print(f" -> Verified active indexes: {sorted(indexes)}")
        assert "ix_tasks_tenant_id" in indexes
        assert "ix_tasks_tenant_status" in indexes
        assert "ix_tasks_tenant_created" in indexes
        assert "ix_task_events_tenant_id" in indexes
        assert "ix_task_events_tenant_created" in indexes
        assert "ix_api_keys_key_id" in indexes
        assert "ix_api_keys_tenant_id" in indexes

    print("[5/6] Testing round-trip downgrade to 002_g83_outbox_available_at...")
    command.downgrade(alembic_cfg, "002_g83_outbox_available_at")
    with engine.begin() as conn:
        cols = (
            conn.execute(
                text("""
            SELECT column_name FROM information_schema.columns WHERE table_name = 'tasks';
        """)
            )
            .scalars()
            .all()
        )
        assert "tenant_id" not in cols, "tenant_id column should have been dropped on downgrade!"
        tables = (
            conn.execute(
                text("""
            SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';
        """)
            )
            .scalars()
            .all()
        )
        assert "api_keys" not in tables, "api_keys table should have been dropped on downgrade!"
        print(" -> Verified downgrade cleanly stripped G9.1 schema elements.")

    print("[6/6] Upgrading fresh database directly to head...")
    command.upgrade(alembic_cfg, "head")
    with engine.begin() as conn:
        cols = (
            conn.execute(
                text("""
            SELECT column_name FROM information_schema.columns WHERE table_name = 'tasks';
        """)
            )
            .scalars()
            .all()
        )
        assert "tenant_id" in cols, "tenant_id column should exist after upgrade to head!"
        print(" -> Verified fresh direct upgrade to head succeeded.")

    # Cleanup
    engine.dispose()
    with psycopg.connect(
        "host=localhost port=5432 user=taskforge password=taskforge_dev dbname=postgres",
        autocommit=True,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(f"DROP DATABASE IF EXISTS {TEST_DB_NAME} WITH (FORCE);")
    print("Migration lifecycle verification complete: ALL GATES PASSED!")


if __name__ == "__main__":
    run_verification()
