"""Live PostgreSQL 16 Alembic migration & composite ownership FK tests for G11.1."""

import subprocess
import sys
import uuid

import pytest
from sqlalchemy import create_engine, delete, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from taskforge.core.config import get_settings
from taskforge.database.models import Task
from taskforge.definitions.models import (
    DefinitionAuditEvent,
    TaskDefinition,
    TaskDefinitionVersion,
)
from taskforge.definitions.repository import DefinitionRepository


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
def test_postgres_alembic_upgrade_downgrade_and_ownership_constraints():
    """Verify Alembic upgrade/downgrade cycle and PostgreSQL composite ownership constraints."""
    # 1. Run Alembic upgrade -> downgrade -> upgrade
    res_up1 = subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert res_up1.returncode == 0, res_up1.stderr

    res_down = subprocess.run(
        [sys.executable, "-m", "alembic", "downgrade", "5a361f84b61f"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert res_down.returncode == 0, res_down.stderr

    res_up2 = subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert res_up2.returncode == 0, res_up2.stderr

    engine = create_engine(PG_URL)
    SessionPG = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    tenant = f"tenant-pg-g11-{uuid.uuid4().hex[:8]}"

    try:
        with SessionPG() as db:
            repo = DefinitionRepository(db)
            def_a = repo.create_definition(
                tenant_id=tenant,
                name="PG Def A",
                description="PG constraint test",
                execution_type="builtin",
                created_by="pg-actor",
            )
            def_b = repo.create_definition(
                tenant_id=tenant,
                name="PG Def B",
                description=None,
                execution_type="builtin",
                created_by="pg-actor",
            )
            ver_a1 = repo.create_version(
                definition_id=def_a.id,
                version=1,
                execution_type="builtin",
                task_type="noop",
                configuration_json={},
                input_schema_json={"type": "object"},
                output_schema_json=None,
                execution_policy_json={},
                content_hash="1" * 64,
                created_by="pg-actor",
            )
            ver_b1 = repo.create_version(
                definition_id=def_b.id,
                version=1,
                execution_type="builtin",
                task_type="noop",
                configuration_json={},
                input_schema_json={"type": "object"},
                output_schema_json=None,
                execution_policy_json={},
                content_hash="2" * 64,
                created_by="pg-actor",
            )

            # Invalid active_version_id ownership rejected by PostgreSQL composite FK
            with pytest.raises(IntegrityError):
                db.execute(
                    text(
                        "UPDATE task_definitions SET active_version_id = :vid WHERE id = :did"
                    ),
                    {"vid": ver_b1.id, "did": def_a.id},
                )
                db.commit()
            db.rollback()

            # Invalid task-version cross-definition ownership rejected by PostgreSQL composite FK
            db.add(
                Task(
                    id=str(uuid.uuid4()),
                    task_name="PG Bad Cross Pin",
                    task_type="noop",
                    payload={},
                    tenant_id=tenant,
                    definition_id=def_a.id,
                    definition_version_id=ver_b1.id,
                )
            )
            with pytest.raises(IntegrityError):
                db.commit()
            db.rollback()

            # Half-populated task provenance rejected by CheckConstraint
            db.add(
                Task(
                    id=str(uuid.uuid4()),
                    task_name="PG Half Ref",
                    task_type="noop",
                    payload={},
                    tenant_id=tenant,
                    definition_id=def_a.id,
                    definition_version_id=None,
                )
            )
            with pytest.raises(IntegrityError):
                db.commit()
            db.rollback()

            # Valid built-in null-provenance + valid pinned task both persist cleanly
            builtin_task = Task(
                id=str(uuid.uuid4()),
                task_name="PG Builtin Compat",
                task_type="noop",
                payload={},
                tenant_id=tenant,
            )
            pinned_task = Task(
                id=str(uuid.uuid4()),
                task_name="PG Pinned Task",
                task_type="noop",
                payload={},
                tenant_id=tenant,
                definition_id=def_a.id,
                definition_version_id=ver_a1.id,
            )
            db.add(builtin_task)
            db.add(pinned_task)
            db.commit()

            repo.set_active_version(
                definition_id=def_a.id,
                tenant_id=tenant,
                version_id=ver_a1.id,
                status="ACTIVE",
            )
            def_a_id = def_a.id
            ver_a1_id = ver_a1.id

        # Reconnect fresh session to verify durability
        with SessionPG() as db:
            loaded = DefinitionRepository(db).get_definition(def_a_id, tenant)
            assert loaded is not None
            assert loaded.active_version_id == ver_a1_id
    finally:
        with SessionPG() as db:
            db.execute(delete(Task).where(Task.tenant_id == tenant))
            db.execute(
                text(
                    "UPDATE task_definitions SET active_version_id = NULL WHERE tenant_id = :t"
                ),
                {"t": tenant},
            )
            db.execute(
                delete(DefinitionAuditEvent).where(
                    DefinitionAuditEvent.tenant_id == tenant
                )
            )
            def_ids = [
                r[0]
                for r in db.query(TaskDefinition.id)
                .filter(TaskDefinition.tenant_id == tenant)
                .all()
            ]
            if def_ids:
                db.execute(
                    delete(TaskDefinitionVersion).where(
                        TaskDefinitionVersion.definition_id.in_(def_ids)
                    )
                )
            db.execute(delete(TaskDefinition).where(TaskDefinition.tenant_id == tenant))
            db.commit()
        engine.dispose()

