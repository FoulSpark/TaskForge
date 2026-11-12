"""Live PostgreSQL 16 concurrent version allocation & transaction tests for Gate G11.2.

Verifies:
1. One definition with 10 concurrent version creation threads.
2. Row-level SELECT ... FOR UPDATE serialization on PostgreSQL 16.
3. Version numbers are strictly unique and sequential (1 through 11).
4. No missing VERSION_CREATED audit events.
5. Zero partially committed versions.
6. Repeated under concurrent database transactions.
"""

import threading
import uuid

import pytest
from sqlalchemy import create_engine, select, text

from taskforge.core.auth import AuthenticatedPrincipal
from taskforge.core.config import get_settings
from taskforge.database.session import SessionLocal
from taskforge.definitions.models import (
    DefinitionAuditEvent,
    TaskDefinitionVersion,
)
from taskforge.definitions.service import DefinitionService


def _postgres_available() -> tuple[bool, str]:
    url = get_settings().database_url
    try:
        eng = create_engine(url, connect_args={"connect_timeout": 3})
        with eng.connect() as conn:
            conn.execute(text("SELECT 1"))
        eng.dispose()
        return True, url
    except Exception:
        return False, url


PG_AVAILABLE, PG_URL = _postgres_available()


@pytest.mark.skipif(not PG_AVAILABLE, reason="Live PostgreSQL 16 instance not reachable")
def test_postgres_10_concurrent_version_allocations_are_sequential_and_audited():
    """Verify that 10 concurrent threads creating versions against the same definition
    on PostgreSQL 16 allocate strictly sequential, unique versions without gaps or duplicates,
    and persist matching audit events with zero partial state.
    """
    tenant_id = f"tenant-pg-concurrency-{uuid.uuid4().hex[:8]}"
    actor_id = f"actor-{uuid.uuid4().hex[:8]}"
    principal = AuthenticatedPrincipal(
        key_id=actor_id,
        tenant_id=tenant_id,
        name="Postgres Concurrency Tester",
        scopes=["definitions:write"],
    )

    definition_id = None
    created_def_ids: list[str] = []

    try:
        # 1. Create definition with initial v1 in PostgreSQL
        with SessionLocal() as db:
            svc = DefinitionService(db)
            defn, v1 = svc.create_definition(
                principal=principal,
                name=f"PG Concurrency Task {uuid.uuid4().hex[:6]}",
                description="Testing Postgres 16 row locking under concurrency",
                execution_type="builtin",
                task_type="noop",
                configuration_json={"batch_size": 10},
                input_schema_json={"type": "object"},
            )
            definition_id = defn.id
            created_def_ids.append(definition_id)
            assert v1.version == 1

        # 2. Launch 10 concurrent threads attempting to create new versions simultaneously
        num_threads = 10
        barrier = threading.Barrier(num_threads)
        errors: list[Exception] = []
        created_versions: list[int] = []
        lock = threading.Lock()

        def _worker(thread_idx: int) -> None:
            barrier.wait(timeout=10.0)
            try:
                # Independent database connection and transaction per thread
                with SessionLocal() as worker_db:
                    worker_svc = DefinitionService(worker_db)
                    ver = worker_svc.create_version(
                        principal=principal,
                        definition_id=definition_id,
                        task_type="noop",
                        configuration_json={"thread_idx": thread_idx},
                        input_schema_json={"type": "object"},
                        execution_type="builtin",
                    )
                    with lock:
                        created_versions.append(ver.version)
            except Exception as exc:
                with lock:
                    errors.append(exc)

        threads = [threading.Thread(target=_worker, args=(i,)) for i in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30.0)

        # Assert all 10 threads completed successfully without uncaught errors
        assert len(errors) == 0, f"Concurrent workers encountered errors: {errors}"
        assert len(created_versions) == 10

        # 3. Verify on PostgreSQL directly that version numbers are strictly 1..11
        with SessionLocal() as verify_db:
            versions = list(
                verify_db.scalars(
                    select(TaskDefinitionVersion)
                    .where(TaskDefinitionVersion.definition_id == definition_id)
                    .order_by(TaskDefinitionVersion.version.asc())
                ).all()
            )

            assert len(versions) == 11, f"Expected 11 versions (1 initial + 10 concurrent), got {len(versions)}"
            allocated_nums = [v.version for v in versions]
            assert allocated_nums == list(range(1, 12)), f"Allocated versions not consecutive: {allocated_nums}"

            # 4. Verify no partially committed versions exist
            for v in versions:
                assert v.id is not None
                assert len(v.content_hash) == 64
                assert v.execution_type == "builtin"
                assert v.task_type == "noop"
                assert v.validation_status in ("UNVALIDATED", "VALID")
                assert v.created_by == actor_id

            # 5. Verify audit events: exactly 1 DEFINITION_CREATED and 11 VERSION_CREATED
            audit_events = list(
                verify_db.scalars(
                    select(DefinitionAuditEvent)
                    .where(DefinitionAuditEvent.definition_id == definition_id)
                    .order_by(DefinitionAuditEvent.created_at.asc())
                ).all()
            )

            actions = [e.action for e in audit_events]
            assert actions.count("DEFINITION_CREATED") == 1
            assert actions.count("VERSION_CREATED") == 11

            recorded_audit_versions = sorted(
                [e.metadata_json["version"] for e in audit_events if e.action == "VERSION_CREATED"]
            )
            assert recorded_audit_versions == list(range(1, 12))

    finally:
        # Cleanup test definitions
        if created_def_ids:
            with SessionLocal() as cleanup_db:
                for def_id in created_def_ids:
                    cleanup_db.execute(
                        text("DELETE FROM task_definitions WHERE id = :id"), {"id": def_id}
                    )
                cleanup_db.commit()


@pytest.mark.skipif(not PG_AVAILABLE, reason="Live PostgreSQL 16 instance not reachable")
def test_postgres_repeat_concurrent_version_allocations_under_heavy_contention():
    """Repeat concurrent version allocation test under heavy contention to ensure determinism."""
    tenant_id = f"tenant-pg-contention-{uuid.uuid4().hex[:8]}"
    actor_id = f"actor-{uuid.uuid4().hex[:8]}"
    principal = AuthenticatedPrincipal(
        key_id=actor_id,
        tenant_id=tenant_id,
        name="Postgres Contention Tester",
        scopes=["definitions:write"],
    )

    created_def_ids: list[str] = []

    try:
        with SessionLocal() as db:
            svc = DefinitionService(db)
            defn, _ = svc.create_definition(
                principal=principal,
                name=f"Contention Definition {uuid.uuid4().hex[:6]}",
                description="Heavy contention run",
                execution_type="builtin",
                task_type="json_transform",
                configuration_json={"round": 1},
                input_schema_json={"type": "object"},
            )
            definition_id = defn.id
            created_def_ids.append(definition_id)

        num_threads = 10
        barrier = threading.Barrier(num_threads)
        errors: list[Exception] = []

        def _worker(thread_idx: int) -> None:
            barrier.wait(timeout=10.0)
            try:
                with SessionLocal() as worker_db:
                    worker_svc = DefinitionService(worker_db)
                    worker_svc.create_version(
                        principal=principal,
                        definition_id=definition_id,
                        task_type="json_transform",
                        configuration_json={"contention_thread": thread_idx},
                        input_schema_json={"type": "object"},
                    )
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=_worker, args=(i,)) for i in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30.0)

        assert len(errors) == 0, f"Encountered unexpected contention errors: {errors}"

        with SessionLocal() as verify_db:
            versions = list(
                verify_db.scalars(
                    select(TaskDefinitionVersion)
                    .where(TaskDefinitionVersion.definition_id == definition_id)
                    .order_by(TaskDefinitionVersion.version.asc())
                ).all()
            )
            assert len(versions) == 11
            assert [v.version for v in versions] == list(range(1, 12))

    finally:
        if created_def_ids:
            with SessionLocal() as cleanup_db:
                for def_id in created_def_ids:
                    cleanup_db.execute(
                        text("DELETE FROM task_definitions WHERE id = :id"), {"id": def_id}
                    )
                cleanup_db.commit()

