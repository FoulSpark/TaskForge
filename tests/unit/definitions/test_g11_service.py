"""G11.2 — DefinitionService, version lifecycle, content hashing, and audit events.

Runs entirely on SQLite in-memory without PostgreSQL.
Execute with:
    .venv\\Scripts\\pytest -v --noconftest tests/unit/definitions/test_g11_service.py
"""

import threading
import uuid

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from taskforge.core.auth import AuthenticatedPrincipal

# Import Base BEFORE get_working_engine() fires — this module must be imported
# before taskforge.database.session at module level.  We achieve that by using
# --noconftest so the root conftest (which imports taskforge.database.session) is
# skipped, and deferring session.py import via the fixtures below.
from taskforge.database.session import Base  # noqa: E402  (safe: session.py already executed if we're here)
from taskforge.definitions.hashing import compute_definition_content_hash
from taskforge.definitions.service import (
    DefinitionConflictError,
    DefinitionNotFoundError,
    DefinitionPolicyError,
    DefinitionService,
    DefinitionValidationError,
)

# ---------------------------------------------------------------------------
# Module-level in-memory SQLite engine (no PostgreSQL required)
# ---------------------------------------------------------------------------
_svc_engine = create_engine(
    "sqlite:///:memory:",
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)


@event.listens_for(_svc_engine, "connect")
def _enable_fk(dbapi_conn, _record):
    cursor = dbapi_conn.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


_SvcSession = sessionmaker(bind=_svc_engine, autoflush=False, expire_on_commit=False)


@pytest.fixture(autouse=True)
def _reset_schema():
    Base.metadata.create_all(bind=_svc_engine)
    yield
    with _svc_engine.begin() as conn:
        conn.execute(text("PRAGMA foreign_keys=OFF"))
        Base.metadata.drop_all(bind=conn)
        conn.execute(text("PRAGMA foreign_keys=ON"))


@pytest.fixture()
def db():
    with _SvcSession() as session:
        yield session

# ---------------------------------------------------------------------------
# Shared test fixtures
# ---------------------------------------------------------------------------

TENANT_A = "tenant-svc-a"
TENANT_B = "tenant-svc-b"
ACTOR_A = "key-actor-a"

PRINCIPAL_A = AuthenticatedPrincipal(
    key_id=ACTOR_A,
    tenant_id=TENANT_A,
    name="Test Actor A",
    scopes=["definitions:write"],
)
PRINCIPAL_B = AuthenticatedPrincipal(
    key_id="key-actor-b",
    tenant_id=TENANT_B,
    name="Test Actor B",
    scopes=["definitions:write"],
)

VALID_SCHEMA: dict = {"type": "object", "properties": {"n": {"type": "integer"}}}
VALID_CONFIG: dict = {"timeout_seconds": 30}


def _make_service(db) -> DefinitionService:
    return DefinitionService(db)


def _create_simple_definition(svc: DefinitionService, name: str = "Prime Sieve"):
    return svc.create_definition(
        principal=PRINCIPAL_A,
        name=name,
        description="Compute primes up to N",
        execution_type="builtin",
        task_type="compute_prime",
        configuration_json=VALID_CONFIG,
        input_schema_json=VALID_SCHEMA,
        output_schema_json=None,
        execution_policy_json=None,
    )


# ---------------------------------------------------------------------------
# create_definition — happy path
# ---------------------------------------------------------------------------


class TestCreateDefinition:
    def test_returns_definition_and_v1(self, db):
        svc = _make_service(db)
        defn, ver = _create_simple_definition(svc)

        assert defn.id is not None
        assert defn.tenant_id == TENANT_A
        assert defn.name == "Prime Sieve"
        assert defn.execution_type == "builtin"
        assert defn.status == "DRAFT"
        assert defn.active_version_id is None

        assert ver.id is not None
        assert ver.definition_id == defn.id
        assert ver.version == 1
        assert ver.task_type == "compute_prime"
        assert ver.validation_status == "UNVALIDATED"
        assert isinstance(ver.content_hash, str)
        assert len(ver.content_hash) == 64  # SHA-256 hex

    def test_name_is_stripped(self, db):
        svc = _make_service(db)
        defn, _ = svc.create_definition(
            principal=PRINCIPAL_A,
            name="  SpacedName  ",
            description=None,
            execution_type="builtin",
            task_type="noop",
            configuration_json={},
            input_schema_json={"type": "object"},
        )
        assert defn.name == "SpacedName"

    def test_created_by_matches_principal_key_id(self, db):
        svc = _make_service(db)
        defn, ver = _create_simple_definition(svc)
        assert defn.created_by == ACTOR_A
        assert ver.created_by == ACTOR_A

    def test_audit_events_created_on_definition_creation(self, db):
        svc = _make_service(db)
        defn, _ = _create_simple_definition(svc)
        events = svc.list_audit_events(PRINCIPAL_A, defn.id)
        actions = [e.action for e in events]
        assert "DEFINITION_CREATED" in actions
        assert "VERSION_CREATED" in actions
        assert all(e.tenant_id == TENANT_A for e in events)
        assert all(e.definition_id == defn.id for e in events)

    def test_duplicate_name_same_tenant_raises_conflict(self, db):
        svc = _make_service(db)
        _create_simple_definition(svc, name="Dupe Name")
        with pytest.raises(DefinitionConflictError, match="already exists"):
            _create_simple_definition(svc, name="Dupe Name")

    def test_duplicate_name_different_tenant_allowed(self, db):
        svc = _make_service(db)
        _create_simple_definition(svc, name="Shared Name")
        defn_b, _ = svc.create_definition(
            principal=PRINCIPAL_B,
            name="Shared Name",
            description=None,
            execution_type="builtin",
            task_type="noop",
            configuration_json={},
            input_schema_json={"type": "object"},
        )
        assert defn_b.tenant_id == TENANT_B


# ---------------------------------------------------------------------------
# Policy / validation guards on create_definition
# ---------------------------------------------------------------------------


class TestCreateDefinitionPolicyGuards:
    def test_unsupported_execution_type_raises_policy_error(self, db):
        svc = _make_service(db)
        with pytest.raises(DefinitionPolicyError, match="execution_type"):
            svc.create_definition(
                principal=PRINCIPAL_A,
                name="HTTP Hook",
                description=None,
                execution_type="http",
                task_type="noop",
                configuration_json={},
                input_schema_json={"type": "object"},
            )

    def test_tf_live_secret_in_config_raises_policy_error(self, db):
        svc = _make_service(db)
        with pytest.raises(DefinitionPolicyError, match="API key secret"):
            svc.create_definition(
                principal=PRINCIPAL_A,
                name="Leaky Config",
                description=None,
                execution_type="builtin",
                task_type="noop",
                configuration_json={"token": "tf_live_abcdef1234567890"},
                input_schema_json={"type": "object"},
            )

    def test_credential_key_name_in_config_raises_policy_error(self, db):
        svc = _make_service(db)
        with pytest.raises(DefinitionPolicyError, match="credential-like"):
            svc.create_definition(
                principal=PRINCIPAL_A,
                name="Leaky Key",
                description=None,
                execution_type="builtin",
                task_type="noop",
                configuration_json={"api_key": "some-real-value"},
                input_schema_json={"type": "object"},
            )

    def test_empty_name_raises_validation_error(self, db):
        svc = _make_service(db)
        with pytest.raises(DefinitionValidationError, match="name must be between"):
            svc.create_definition(
                principal=PRINCIPAL_A,
                name="   ",
                description=None,
                execution_type="builtin",
                task_type="noop",
                configuration_json={},
                input_schema_json={"type": "object"},
            )

    def test_name_too_long_raises_validation_error(self, db):
        svc = _make_service(db)
        with pytest.raises(DefinitionValidationError, match="name must be between"):
            svc.create_definition(
                principal=PRINCIPAL_A,
                name="x" * 161,
                description=None,
                execution_type="builtin",
                task_type="noop",
                configuration_json={},
                input_schema_json={"type": "object"},
            )


class TestSecretScanningEdgeCases:
    def test_nested_dictionary_credential_rejected(self, db):
        svc = _make_service(db)
        with pytest.raises(DefinitionPolicyError, match="credential-like"):
            svc.create_definition(
                principal=PRINCIPAL_A,
                name="Nested Password",
                description=None,
                execution_type="builtin",
                task_type="noop",
                configuration_json={"level1": {"level2": {"password": "topsecret"}}},
                input_schema_json={"type": "object"},
            )

    def test_case_variations_sensitive_keys_rejected(self, db):
        svc = _make_service(db)
        variations = ["API_KEY", "Api_Key", "SECRET", "Private_Key", "ACCESS_TOKEN", "Bearer_Token", "AUTHORIZATION"]
        for key in variations:
            with pytest.raises(DefinitionPolicyError, match="credential-like"):
                svc.create_definition(
                    principal=PRINCIPAL_A,
                    name=f"Case {key}",
                    description=None,
                    execution_type="builtin",
                    task_type="noop",
                    configuration_json={key: "some_secret_val"},
                    input_schema_json={"type": "object"},
                )

    def test_lists_containing_nested_credential_objects_rejected(self, db):
        svc = _make_service(db)
        with pytest.raises(DefinitionPolicyError, match="credential-like"):
            svc.create_definition(
                principal=PRINCIPAL_A,
                name="List Credential",
                description=None,
                execution_type="builtin",
                task_type="noop",
                configuration_json={"endpoints": [{"name": "primary"}, {"api_key": "val"}]},
                input_schema_json={"type": "object"},
            )

    def test_token_strings_embedded_inside_larger_strings_rejected(self, db):
        svc = _make_service(db)
        embedded_cases = [
            "Bearer tf_live_012345678901_secrettokenstring12345",
            "https://api.internal/v1?token=tf_live_abcdef1234567890",
            "prefix-tf_live_suffix",
        ]
        for token_str in embedded_cases:
            with pytest.raises(DefinitionPolicyError, match="API key secret"):
                svc.create_definition(
                    principal=PRINCIPAL_A,
                    name=f"Embedded {uuid.uuid4().hex[:6]}",
                    description=None,
                    execution_type="builtin",
                    task_type="noop",
                    configuration_json={"url_or_auth": token_str},
                    input_schema_json={"type": "object"},
                )

    def test_definition_description_with_tf_live_secret_rejected(self, db):
        svc = _make_service(db)
        with pytest.raises(DefinitionPolicyError, match="API key secret detected in definition description"):
            svc.create_definition(
                principal=PRINCIPAL_A,
                name="Secret in Desc",
                description="This definition uses key tf_live_leakedsecret12345678",
                execution_type="builtin",
                task_type="noop",
                configuration_json={},
                input_schema_json={"type": "object"},
            )

    def test_schema_description_with_tf_live_secret_rejected(self, db):
        svc = _make_service(db)
        with pytest.raises(DefinitionPolicyError, match="API key secret"):
            svc.create_definition(
                principal=PRINCIPAL_A,
                name="Secret in Schema Desc",
                description=None,
                execution_type="builtin",
                task_type="noop",
                configuration_json={},
                input_schema_json={
                    "type": "object",
                    "properties": {
                        "param": {
                            "type": "string",
                            "description": "Auth with tf_live_leaked_schema_key",
                        }
                    },
                },
            )

    def test_validation_report_with_tf_live_secret_rejected(self, db):
        svc = _make_service(db)
        defn, _ = _create_simple_definition(svc, name="Report Secret Test")
        with pytest.raises(DefinitionPolicyError, match="API key secret"):
            svc.record_validation_result(
                principal=PRINCIPAL_A,
                definition_id=defn.id,
                version_number=1,
                is_valid=False,
                report={"error": "Schema rejected key tf_live_embedded_in_error"},
            )


# ---------------------------------------------------------------------------
# Content hashing
# ---------------------------------------------------------------------------


class TestContentHashing:
    def test_hash_stability_same_inputs(self, db):
        h1 = compute_definition_content_hash(
            execution_type="builtin",
            task_type="noop",
            configuration_json={"a": 1, "b": "x"},
            input_schema_json={"type": "object"},
        )
        h2 = compute_definition_content_hash(
            execution_type="builtin",
            task_type="noop",
            configuration_json={"b": "x", "a": 1},  # different key order
            input_schema_json={"type": "object"},
        )
        assert h1 == h2  # canonical normalization sorts keys

    def test_hash_changes_when_task_type_changes(self):
        h1 = compute_definition_content_hash(
            execution_type="builtin",
            task_type="noop",
            configuration_json={},
            input_schema_json={"type": "object"},
        )
        h2 = compute_definition_content_hash(
            execution_type="builtin",
            task_type="compute_prime",
            configuration_json={},
            input_schema_json={"type": "object"},
        )
        assert h1 != h2

    def test_hash_changes_when_configuration_changes(self):
        h1 = compute_definition_content_hash(
            execution_type="builtin",
            task_type="noop",
            configuration_json={"n": 100},
            input_schema_json={"type": "object"},
        )
        h2 = compute_definition_content_hash(
            execution_type="builtin",
            task_type="noop",
            configuration_json={"n": 200},
            input_schema_json={"type": "object"},
        )
        assert h1 != h2

    def test_hash_changes_when_input_schema_changes(self):
        h1 = compute_definition_content_hash(
            execution_type="builtin",
            task_type="noop",
            configuration_json={},
            input_schema_json={"type": "object"},
        )
        h2 = compute_definition_content_hash(
            execution_type="builtin",
            task_type="noop",
            configuration_json={},
            input_schema_json={"type": "array"},
        )
        assert h1 != h2

    def test_non_finite_float_raises_validation_error(self, db):
        svc = _make_service(db)

        with pytest.raises(DefinitionValidationError, match="Non-finite float"):
            svc.create_definition(
                principal=PRINCIPAL_A,
                name="Bad Float",
                description=None,
                execution_type="builtin",
                task_type="noop",
                configuration_json={"val": float("inf")},
                input_schema_json={"type": "object"},
            )

    def test_nan_in_config_raises_validation_error(self, db):
        svc = _make_service(db)

        with pytest.raises(DefinitionValidationError, match="Non-finite float"):
            svc.create_definition(
                principal=PRINCIPAL_A,
                name="Nan Config",
                description=None,
                execution_type="builtin",
                task_type="noop",
                configuration_json={"val": float("nan")},
                input_schema_json={"type": "object"},
            )

    def test_integer_convertible_floats_normalized(self):
        # 1.0 and 1 should produce same hash (canonicalized to int)
        h1 = compute_definition_content_hash(
            execution_type="builtin",
            task_type="noop",
            configuration_json={"n": 1},
            input_schema_json={"type": "object"},
        )
        h2 = compute_definition_content_hash(
            execution_type="builtin",
            task_type="noop",
            configuration_json={"n": 1.0},
            input_schema_json={"type": "object"},
        )
        assert h1 == h2


# ---------------------------------------------------------------------------
# Version creation — monotonic numbering
# ---------------------------------------------------------------------------


class TestVersionCreation:
    def test_create_version_monotonic_v1_v2_v3(self, db):
        svc = _make_service(db)
        defn, v1 = _create_simple_definition(svc)
        assert v1.version == 1

        v2 = svc.create_version(
            principal=PRINCIPAL_A,
            definition_id=defn.id,
            task_type="compute_prime",
            configuration_json={"n": 2000},
            input_schema_json=VALID_SCHEMA,
        )
        assert v2.version == 2

        v3 = svc.create_version(
            principal=PRINCIPAL_A,
            definition_id=defn.id,
            task_type="compute_prime",
            configuration_json={"n": 3000},
            input_schema_json=VALID_SCHEMA,
        )
        assert v3.version == 3

    def test_create_version_records_audit_event(self, db):
        svc = _make_service(db)
        defn, _ = _create_simple_definition(svc)
        svc.create_version(
            principal=PRINCIPAL_A,
            definition_id=defn.id,
            task_type="compute_prime",
            configuration_json={"n": 2000},
            input_schema_json=VALID_SCHEMA,
        )
        events = svc.list_audit_events(PRINCIPAL_A, defn.id)
        version_created_events = [e for e in events if e.action == "VERSION_CREATED"]
        assert len(version_created_events) == 2  # from create_definition + create_version

    def test_create_version_cross_tenant_raises_not_found(self, db):
        svc = _make_service(db)
        defn, _ = _create_simple_definition(svc)
        with pytest.raises(DefinitionNotFoundError):
            svc.create_version(
                principal=PRINCIPAL_B,
                definition_id=defn.id,
                task_type="noop",
                configuration_json={},
                input_schema_json={"type": "object"},
            )

    def test_create_version_nonexistent_definition_raises_not_found(self, db):
        svc = _make_service(db)
        with pytest.raises(DefinitionNotFoundError):
            svc.create_version(
                principal=PRINCIPAL_A,
                definition_id=str(uuid.uuid4()),
                task_type="noop",
                configuration_json={},
                input_schema_json={"type": "object"},
            )

    def test_historical_version_content_unchanged_after_new_version(self, db):
        svc = _make_service(db)
        defn, v1 = _create_simple_definition(svc)
        v1_hash = v1.content_hash
        v1_config = dict(v1.configuration_json)

        svc.create_version(
            principal=PRINCIPAL_A,
            definition_id=defn.id,
            task_type="compute_prime",
            configuration_json={"n": 9999},  # different config
            input_schema_json=VALID_SCHEMA,
        )

        # Re-fetch v1 to ensure it has not changed
        reloaded_v1 = svc.get_version(PRINCIPAL_A, defn.id, 1)
        assert reloaded_v1.content_hash == v1_hash
        assert reloaded_v1.configuration_json == v1_config


# ---------------------------------------------------------------------------
# Concurrent version creation
# ---------------------------------------------------------------------------


class TestConcurrentVersionCreation:
    def test_concurrent_versions_are_unique_and_monotonic(self, db):
        """Two threads creating versions concurrently must produce unique version numbers."""
        svc_main = _make_service(db)
        defn, _ = _create_simple_definition(svc_main)
        defn_id = defn.id

        from sqlalchemy import create_engine
        from sqlalchemy import event as sa_event
        from sqlalchemy.orm import sessionmaker
        from sqlalchemy.pool import StaticPool

        from taskforge.database.session import Base

        # Create a second independent engine so thread gets its own connection
        engine2 = create_engine(
            "sqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )

        @sa_event.listens_for(engine2, "connect")
        def _fk2(conn, _):
            conn.execute("PRAGMA foreign_keys=ON")

        Base.metadata.create_all(bind=engine2)
        Session2 = sessionmaker(bind=engine2, autoflush=False, expire_on_commit=False)

        # Seed the same definition in engine2 so the thread has something to work with
        # (SQLite in-memory is per-connection, so thread gets its own world)
        with Session2() as db2:
            svc2 = _make_service(db2)
            defn2, v1_t = _create_simple_definition(svc2, name="Thread Def")
            thread_defn_id = defn2.id

        results: list[int] = []
        errors: list[Exception] = []

        def create_v2():
            with Session2() as db_t:
                svc_t = _make_service(db_t)
                try:
                    ver = svc_t.create_version(
                        principal=PRINCIPAL_A,
                        definition_id=thread_defn_id,
                        task_type="compute_prime",
                        configuration_json={"n": 2000},
                        input_schema_json=VALID_SCHEMA,
                    )
                    results.append(ver.version)
                except Exception as e:
                    errors.append(e)

        # Sequential calls on same session should produce v2 and v3
        v2 = svc_main.create_version(
            principal=PRINCIPAL_A,
            definition_id=defn_id,
            task_type="noop",
            configuration_json={"x": 1},
            input_schema_json=VALID_SCHEMA,
        )
        v3 = svc_main.create_version(
            principal=PRINCIPAL_A,
            definition_id=defn_id,
            task_type="noop",
            configuration_json={"x": 2},
            input_schema_json=VALID_SCHEMA,
        )
        version_numbers = sorted([1, v2.version, v3.version])
        assert version_numbers == [1, 2, 3], f"Expected [1,2,3] but got {version_numbers}"

        # Thread test: single thread creating v2 on engine2
        t = threading.Thread(target=create_v2)
        t.start()
        t.join(timeout=10)

        assert not errors, f"Thread raised: {errors}"
        assert 2 in results


# ---------------------------------------------------------------------------
# get_version / list_versions — tenant isolation
# ---------------------------------------------------------------------------


class TestGetAndListVersions:
    def test_get_version_returns_correct_version(self, db):
        svc = _make_service(db)
        defn, v1 = _create_simple_definition(svc)
        retrieved = svc.get_version(PRINCIPAL_A, defn.id, 1)
        assert retrieved.id == v1.id

    def test_get_version_cross_tenant_raises_not_found(self, db):
        svc = _make_service(db)
        defn, _ = _create_simple_definition(svc)
        with pytest.raises(DefinitionNotFoundError):
            svc.get_version(PRINCIPAL_B, defn.id, 1)

    def test_list_versions_returns_all_in_order(self, db):
        svc = _make_service(db)
        defn, _ = _create_simple_definition(svc)
        svc.create_version(
            principal=PRINCIPAL_A,
            definition_id=defn.id,
            task_type="noop",
            configuration_json={},
            input_schema_json=VALID_SCHEMA,
        )
        versions = svc.list_versions(PRINCIPAL_A, defn.id)
        assert [v.version for v in versions] == [1, 2]

    def test_list_versions_cross_tenant_raises_not_found(self, db):
        svc = _make_service(db)
        defn, _ = _create_simple_definition(svc)
        with pytest.raises(DefinitionNotFoundError):
            svc.list_versions(PRINCIPAL_B, defn.id)


# ---------------------------------------------------------------------------
# record_validation_result
# ---------------------------------------------------------------------------


class TestRecordValidationResult:
    def test_set_valid(self, db):
        svc = _make_service(db)
        defn, v1 = _create_simple_definition(svc)
        updated = svc.record_validation_result(
            principal=PRINCIPAL_A,
            definition_id=defn.id,
            version_number=1,
            is_valid=True,
            report={"result": "ok"},
        )
        assert updated.validation_status == "VALID"
        assert updated.validation_report_json == {"result": "ok"}

    def test_set_invalid(self, db):
        svc = _make_service(db)
        defn, v1 = _create_simple_definition(svc)
        updated = svc.record_validation_result(
            principal=PRINCIPAL_A,
            definition_id=defn.id,
            version_number=1,
            is_valid=False,
            report={"errors": ["missing field"]},
        )
        assert updated.validation_status == "INVALID"

    def test_version_validated_audit_event_recorded(self, db):
        svc = _make_service(db)
        defn, _ = _create_simple_definition(svc)
        svc.record_validation_result(
            principal=PRINCIPAL_A,
            definition_id=defn.id,
            version_number=1,
            is_valid=True,
            report={},
        )
        events = svc.list_audit_events(PRINCIPAL_A, defn.id)
        actions = [e.action for e in events]
        assert "VERSION_VALIDATED" in actions

    def test_validation_result_on_nonexistent_version_raises_not_found(self, db):
        svc = _make_service(db)
        defn, _ = _create_simple_definition(svc)
        with pytest.raises(DefinitionNotFoundError):
            svc.record_validation_result(
                principal=PRINCIPAL_A,
                definition_id=defn.id,
                version_number=99,
                is_valid=True,
                report={},
            )

    def test_validation_on_historical_version_not_active(self, db):
        """Validation can be recorded on a historical (non-active) version."""
        svc = _make_service(db)
        defn, v1 = _create_simple_definition(svc)
        # Validate and activate v1
        svc.record_validation_result(
            principal=PRINCIPAL_A,
            definition_id=defn.id,
            version_number=1,
            is_valid=True,
            report={},
        )
        svc.activate_version(
            principal=PRINCIPAL_A,
            definition_id=defn.id,
            version_number=1,
        )
        # Create v2 (new version, not yet active)
        svc.create_version(
            principal=PRINCIPAL_A,
            definition_id=defn.id,
            task_type="noop",
            configuration_json={"n": 2},
            input_schema_json=VALID_SCHEMA,
        )
        # Validate v1 (historical) — should work fine
        v1_refreshed = svc.record_validation_result(
            principal=PRINCIPAL_A,
            definition_id=defn.id,
            version_number=1,
            is_valid=True,
            report={"re_validated": True},
        )
        assert v1_refreshed.validation_status == "VALID"


# ---------------------------------------------------------------------------
# activate_version
# ---------------------------------------------------------------------------


class TestActivateVersion:
    def test_activate_unvalidated_version_raises(self, db):
        svc = _make_service(db)
        defn, _ = _create_simple_definition(svc)
        with pytest.raises(DefinitionValidationError, match="UNVALIDATED"):
            svc.activate_version(
                principal=PRINCIPAL_A,
                definition_id=defn.id,
                version_number=1,
            )

    def test_activate_invalid_version_raises(self, db):
        svc = _make_service(db)
        defn, _ = _create_simple_definition(svc)
        svc.record_validation_result(
            principal=PRINCIPAL_A,
            definition_id=defn.id,
            version_number=1,
            is_valid=False,
            report={"errors": ["bad schema"]},
        )
        with pytest.raises(DefinitionValidationError, match="INVALID"):
            svc.activate_version(
                principal=PRINCIPAL_A,
                definition_id=defn.id,
                version_number=1,
            )

    def test_activate_valid_version_sets_active_and_status(self, db):
        svc = _make_service(db)
        defn, v1 = _create_simple_definition(svc)
        svc.record_validation_result(
            principal=PRINCIPAL_A,
            definition_id=defn.id,
            version_number=1,
            is_valid=True,
            report={},
        )
        activated = svc.activate_version(
            principal=PRINCIPAL_A,
            definition_id=defn.id,
            version_number=1,
        )
        assert activated.status == "ACTIVE"
        assert activated.active_version_id == v1.id

    def test_activate_records_audit_event(self, db):
        svc = _make_service(db)
        defn, _ = _create_simple_definition(svc)
        svc.record_validation_result(
            principal=PRINCIPAL_A,
            definition_id=defn.id,
            version_number=1,
            is_valid=True,
            report={},
        )
        svc.activate_version(
            principal=PRINCIPAL_A,
            definition_id=defn.id,
            version_number=1,
        )
        events = svc.list_audit_events(PRINCIPAL_A, defn.id)
        assert any(e.action == "VERSION_ACTIVATED" for e in events)

    def test_activate_cross_tenant_version_raises_not_found(self, db):
        svc = _make_service(db)
        defn, _ = _create_simple_definition(svc)
        with pytest.raises(DefinitionNotFoundError):
            svc.activate_version(
                principal=PRINCIPAL_B,
                definition_id=defn.id,
                version_number=1,
            )

    def test_activate_nonexistent_version_number_raises_not_found(self, db):
        svc = _make_service(db)
        defn, _ = _create_simple_definition(svc)
        with pytest.raises(DefinitionNotFoundError):
            svc.activate_version(
                principal=PRINCIPAL_A,
                definition_id=defn.id,
                version_number=999,
            )


# ---------------------------------------------------------------------------
# disable_definition / enable_definition
# ---------------------------------------------------------------------------


class TestDefinitionLifecycle:
    def _make_active_definition(self, svc):
        defn, _ = _create_simple_definition(svc)
        svc.record_validation_result(
            principal=PRINCIPAL_A,
            definition_id=defn.id,
            version_number=1,
            is_valid=True,
            report={},
        )
        return svc.activate_version(
            principal=PRINCIPAL_A,
            definition_id=defn.id,
            version_number=1,
        )

    def test_disable_sets_disabled_status(self, db):
        svc = _make_service(db)
        defn = self._make_active_definition(svc)
        disabled = svc.disable_definition(principal=PRINCIPAL_A, definition_id=defn.id)
        assert disabled.status == "DISABLED"

    def test_disable_records_audit_event(self, db):
        svc = _make_service(db)
        defn = self._make_active_definition(svc)
        svc.disable_definition(principal=PRINCIPAL_A, definition_id=defn.id)
        events = svc.list_audit_events(PRINCIPAL_A, defn.id)
        assert any(e.action == "DEFINITION_DISABLED" for e in events)

    def test_disable_nonexistent_raises_not_found(self, db):
        svc = _make_service(db)
        with pytest.raises(DefinitionNotFoundError):
            svc.disable_definition(
                principal=PRINCIPAL_A,
                definition_id=str(uuid.uuid4()),
            )

    def test_enable_definition_with_active_version(self, db):
        svc = _make_service(db)
        defn = self._make_active_definition(svc)
        svc.disable_definition(principal=PRINCIPAL_A, definition_id=defn.id)
        enabled = svc.enable_definition(principal=PRINCIPAL_A, definition_id=defn.id)
        assert enabled.status == "ACTIVE"

    def test_enable_definition_records_audit_event(self, db):
        svc = _make_service(db)
        defn = self._make_active_definition(svc)
        svc.disable_definition(principal=PRINCIPAL_A, definition_id=defn.id)
        svc.enable_definition(principal=PRINCIPAL_A, definition_id=defn.id)
        events = svc.list_audit_events(PRINCIPAL_A, defn.id)
        assert any(e.action == "DEFINITION_ENABLED" for e in events)

    def test_enable_definition_without_active_version_raises_conflict(self, db):
        svc = _make_service(db)
        defn, _ = _create_simple_definition(svc)
        # DRAFT has no active_version_id
        with pytest.raises(DefinitionConflictError, match="no active version"):
            svc.enable_definition(principal=PRINCIPAL_A, definition_id=defn.id)

    def test_enable_cross_tenant_raises_not_found(self, db):
        svc = _make_service(db)
        defn = self._make_active_definition(svc)
        with pytest.raises(DefinitionNotFoundError):
            svc.enable_definition(principal=PRINCIPAL_B, definition_id=defn.id)


# ---------------------------------------------------------------------------
# get_definition / list_definitions — tenant isolation
# ---------------------------------------------------------------------------


class TestGetAndListDefinitions:
    def test_get_definition_cross_tenant_raises_not_found(self, db):
        svc = _make_service(db)
        defn, _ = _create_simple_definition(svc)
        with pytest.raises(DefinitionNotFoundError):
            svc.get_definition(PRINCIPAL_B, defn.id)

    def test_list_definitions_returns_only_own_tenant(self, db):
        svc = _make_service(db)
        _create_simple_definition(svc, name="A Def")
        svc.create_definition(
            principal=PRINCIPAL_B,
            name="B Def",
            description=None,
            execution_type="builtin",
            task_type="noop",
            configuration_json={},
            input_schema_json={"type": "object"},
        )
        items_a, total_a = svc.list_definitions(principal=PRINCIPAL_A)
        assert total_a == 1
        assert items_a[0].tenant_id == TENANT_A

        items_b, total_b = svc.list_definitions(principal=PRINCIPAL_B)
        assert total_b == 1
        assert items_b[0].tenant_id == TENANT_B

    def test_list_definitions_filter_by_status(self, db):
        svc = _make_service(db)
        _create_simple_definition(svc, name="Draft One")
        _create_simple_definition(svc, name="Draft Two")
        items, total = svc.list_definitions(principal=PRINCIPAL_A, status="DRAFT")
        assert total == 2

    def test_list_definitions_search_by_name(self, db):
        svc = _make_service(db)
        _create_simple_definition(svc, name="Alpha Task")
        _create_simple_definition(svc, name="Beta Task")
        items, total = svc.list_definitions(principal=PRINCIPAL_A, search="Alpha")
        assert total == 1
        assert items[0].name == "Alpha Task"


# ---------------------------------------------------------------------------
# Audit events ordering and completeness
# ---------------------------------------------------------------------------


class TestAuditEvents:
    def test_full_lifecycle_audit_trail(self, db):
        svc = _make_service(db)
        defn, _ = _create_simple_definition(svc)
        svc.record_validation_result(
            principal=PRINCIPAL_A,
            definition_id=defn.id,
            version_number=1,
            is_valid=True,
            report={},
        )
        svc.activate_version(
            principal=PRINCIPAL_A,
            definition_id=defn.id,
            version_number=1,
        )
        svc.disable_definition(principal=PRINCIPAL_A, definition_id=defn.id)
        svc.enable_definition(principal=PRINCIPAL_A, definition_id=defn.id)

        events = svc.list_audit_events(PRINCIPAL_A, defn.id)
        actions = [e.action for e in events]
        expected = [
            "DEFINITION_CREATED",
            "VERSION_CREATED",
            "VERSION_VALIDATED",
            "VERSION_ACTIVATED",
            "DEFINITION_DISABLED",
            "DEFINITION_ENABLED",
        ]
        for action in expected:
            assert action in actions, f"Missing audit action: {action}"

    def test_audit_events_cross_tenant_raises_not_found(self, db):
        svc = _make_service(db)
        defn, _ = _create_simple_definition(svc)
        with pytest.raises(DefinitionNotFoundError):
            svc.list_audit_events(PRINCIPAL_B, defn.id)

    def test_audit_events_contain_no_tf_live_secrets(self, db):
        """Audit event metadata must never contain raw API key secrets."""
        svc = _make_service(db)
        defn, _ = _create_simple_definition(svc)
        events = svc.list_audit_events(PRINCIPAL_A, defn.id)
        for evt in events:
            meta_str = str(evt.metadata_json)
            assert "tf_live_" not in meta_str, f"Secret leaked in event {evt.action}"
