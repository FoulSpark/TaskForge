"""SQLite (with PRAGMA foreign_keys=ON) persistence, ownership FK, and repository tests for G11.1."""

import uuid

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from taskforge.database.models import Task
from taskforge.database.session import Base
from taskforge.definitions.models import TaskDefinitionVersion
from taskforge.definitions.repository import DefinitionRepository, ImmutableVersionError

sqlite_engine = create_engine(
    "sqlite:///:memory:",
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)


@event.listens_for(sqlite_engine, "connect")
def _enable_sqlite_fks(dbapi_connection, _connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


TestingSessionLocal = sessionmaker(
    bind=sqlite_engine, autoflush=False, expire_on_commit=False
)


@pytest.fixture(autouse=True)
def setup_sqlite_schema():
    Base.metadata.create_all(bind=sqlite_engine)
    yield
    with sqlite_engine.begin() as conn:
        conn.execute(text("PRAGMA foreign_keys=OFF"))
        Base.metadata.drop_all(bind=conn)
        conn.execute(text("PRAGMA foreign_keys=ON"))


def test_sqlite_fk_enforcement_active():
    with TestingSessionLocal() as db:
        fk_on = db.execute(text("PRAGMA foreign_keys")).scalar()
        assert fk_on == 1


def test_existing_builtin_task_backward_compatibility():
    with TestingSessionLocal() as db:
        task = Task(
            id=str(uuid.uuid4()),
            task_name="Builtin Noop",
            task_type="noop",
            payload={"ok": True},
            tenant_id="tenant-compat",
        )
        db.add(task)
        db.commit()

        loaded = db.get(Task, task.id)
        assert loaded is not None
        assert loaded.definition_id is None
        assert loaded.definition_version_id is None


def test_definition_version_audit_persistence_and_immutability():
    with TestingSessionLocal() as db:
        repo = DefinitionRepository(db)
        defn = repo.create_definition(
            tenant_id="tenant-a",
            name="Prime Calc",
            description="Compute primes",
            execution_type="builtin",
            created_by="key-a",
        )
        v1 = repo.create_version(
            definition_id=defn.id,
            version=repo.get_next_version_number(defn.id),
            execution_type="builtin",
            task_type="compute_prime",
            configuration_json={"n": 1000},
            input_schema_json={"type": "object"},
            output_schema_json=None,
            execution_policy_json={},
            content_hash="a" * 64,
            validation_status="VALID",
            validation_report_json={"valid": True},
            created_by="key-a",
        )
        repo.set_active_version(
            definition_id=defn.id,
            tenant_id="tenant-a",
            version_id=v1.id,
            status="ACTIVE",
        )
        repo.record_audit_event(
            tenant_id="tenant-a",
            definition_id=defn.id,
            version_id=v1.id,
            actor_id="key-a",
            action="VERSION_ACTIVATED",
            metadata_json={"version": 1},
        )
        defn_id = defn.id
        v1_id = v1.id

    # Re-open a fresh session (simulating restart) and verify persistence + immutability
    with TestingSessionLocal() as db:
        repo = DefinitionRepository(db)
        loaded_def = repo.get_definition(defn_id, "tenant-a")
        assert loaded_def is not None
        assert loaded_def.status == "ACTIVE"
        assert loaded_def.active_version_id == v1_id

        versions = repo.list_versions(defn_id, "tenant-a")
        assert len(versions) == 1
        assert versions[0].version == 1

        audits = repo.list_audit_events(defn_id, "tenant-a")
        assert len(audits) == 1
        assert audits[0].action == "VERSION_ACTIVATED"

        with pytest.raises(ImmutableVersionError):
            repo.update_version(v1_id, configuration_json={"n": 9999})


def test_cross_tenant_repository_lookup_isolated():
    with TestingSessionLocal() as db:
        repo = DefinitionRepository(db)
        defn = repo.create_definition(
            tenant_id="tenant-a",
            name="Secret A",
            description=None,
            execution_type="builtin",
            created_by="key-a",
        )
        v1 = repo.create_version(
            definition_id=defn.id,
            version=1,
            execution_type="builtin",
            task_type="noop",
            configuration_json={},
            input_schema_json={"type": "object"},
            output_schema_json=None,
            execution_policy_json={},
            content_hash="b" * 64,
            created_by="key-a",
        )
        repo.record_audit_event(
            tenant_id="tenant-a",
            definition_id=defn.id,
            version_id=v1.id,
            actor_id="key-a",
            action="DEFINITION_CREATED",
        )

        # Tenant B cannot read, list, activate, or inspect versions/audits
        assert repo.get_definition(defn.id, "tenant-b") is None
        items_b, total_b = repo.list_definitions(tenant_id="tenant-b")
        assert total_b == 0 and items_b == []
        assert repo.get_version(defn.id, 1, "tenant-b") is None
        assert repo.get_version_by_id(v1.id, "tenant-b") is None
        assert repo.list_versions(defn.id, "tenant-b") == []
        assert repo.list_audit_events(defn.id, "tenant-b") == []
        assert (
            repo.set_active_version(
                definition_id=defn.id, tenant_id="tenant-b", version_id=v1.id
            )
            is None
        )


def test_db_constraints_duplicate_version_ownership_and_task_pinning():
    with TestingSessionLocal() as db:
        repo = DefinitionRepository(db)
        def_a = repo.create_definition(
            tenant_id="tenant-a",
            name="Def A",
            description=None,
            execution_type="builtin",
            created_by="key-a",
        )
        def_b = repo.create_definition(
            tenant_id="tenant-a",
            name="Def B",
            description=None,
            execution_type="builtin",
            created_by="key-a",
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
            content_hash="c" * 64,
            created_by="key-a",
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
            content_hash="d" * 64,
            created_by="key-a",
        )

        # 1. Duplicate version number rejected
        dup = TaskDefinitionVersion(
            id=str(uuid.uuid4()),
            definition_id=def_a.id,
            version=1,
            execution_type="builtin",
            task_type="noop",
            configuration_json={},
            input_schema_json={},
            execution_policy_json={},
            content_hash="e" * 64,
            created_by="key-a",
        )
        db.add(dup)
        with pytest.raises(IntegrityError):
            db.commit()
        db.rollback()

        # 2. Invalid active_version_id ownership (Def A pointing to Def B's version) rejected by composite FK
        with pytest.raises(IntegrityError):
            db.execute(
                text(
                    "UPDATE task_definitions SET active_version_id = :vid WHERE id = :did"
                ),
                {"vid": ver_b1.id, "did": def_a.id},
            )
            db.commit()
        db.rollback()

        # 3. Invalid task-version ownership (Task referencing Def A + Ver B1) rejected by composite FK
        bad_task_cross = Task(
            id=str(uuid.uuid4()),
            task_name="Cross Def Pin",
            task_type="noop",
            payload={},
            tenant_id="tenant-a",
            definition_id=def_a.id,
            definition_version_id=ver_b1.id,
        )
        db.add(bad_task_cross)
        with pytest.raises(IntegrityError):
            db.commit()
        db.rollback()

        # 4. Half-populated task provenance fields rejected by CheckConstraint
        bad_task_half_1 = Task(
            id=str(uuid.uuid4()),
            task_name="Half Ref 1",
            task_type="noop",
            payload={},
            tenant_id="tenant-a",
            definition_id=def_a.id,
            definition_version_id=None,
        )
        db.add(bad_task_half_1)
        with pytest.raises(IntegrityError):
            db.commit()
        db.rollback()

        bad_task_half_2 = Task(
            id=str(uuid.uuid4()),
            task_name="Half Ref 2",
            task_type="noop",
            payload={},
            tenant_id="tenant-a",
            definition_id=None,
            definition_version_id=ver_a1.id,
        )
        db.add(bad_task_half_2)
        with pytest.raises(IntegrityError):
            db.commit()
        db.rollback()

        # 5. Valid task provenance pair accepted
        good_task = Task(
            id=str(uuid.uuid4()),
            task_name="Valid Pinned Task",
            task_type="noop",
            payload={},
            tenant_id="tenant-a",
            definition_id=def_a.id,
            definition_version_id=ver_a1.id,
        )
        db.add(good_task)
        db.commit()
        assert db.get(Task, good_task.id).definition_version_id == ver_a1.id
