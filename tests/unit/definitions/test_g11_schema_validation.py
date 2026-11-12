"""Gate G11.3 — JSON Schema Draft 2020-12 Validation Test Suite.

Covers 60 unit tests across 9 target categories:
1. Valid schemas (10)
2. Invalid schemas (10)
3. Local $ref resolution (5)
4. Remote reference rejection (5)
5. Resource limits (8)
6. TaskRegistry compatibility (6)
7. Deterministic reporting (5)
8. Historical immutability (4)
9. Failure & security handling (7)

Runs entirely in-memory without requiring external services.
"""

from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from taskforge.core.auth import AuthenticatedPrincipal
from taskforge.database.session import Base
from taskforge.definitions.service import (
    DefinitionService,
    DefinitionValidationError,
)
from taskforge.definitions.validation import (
    ERR_CIRCULAR_REFERENCE_DETECTED,
    ERR_INTERNAL_VALIDATOR_ERROR,
    ERR_INVALID_SCHEMA_SYNTAX,
    ERR_INVALID_SCHEMA_TYPE,
    ERR_PROHIBITED_SECRET_DETECTED,
    ERR_REFERENCE_DEPTH_EXCEEDED,
    ERR_REMOTE_REFERENCE_PROHIBITED,
    ERR_SCHEMA_DEPTH_EXCEEDED,
    ERR_SCHEMA_SIZE_EXCEEDED,
    ERR_UNKNOWN_TASK_TYPE,
    ERR_UNRESOLVED_REFERENCE,
    ERR_UNSUPPORTED_EXECUTION_TYPE,
    ERR_UNSUPPORTED_SCHEMA_DRAFT,
    MAX_VALIDATION_ERRORS,
    compile_schema,
    validate_definition_version,
    validate_schema_instance,
)

# ---------------------------------------------------------------------------
# Module-level in-memory SQLite engine
# ---------------------------------------------------------------------------
_val_engine = create_engine(
    "sqlite:///:memory:",
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)


@event.listens_for(_val_engine, "connect")
def _enable_fk(dbapi_conn, _record):
    cursor = dbapi_conn.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


_ValSession = sessionmaker(bind=_val_engine, autoflush=False, expire_on_commit=False)


@pytest.fixture(autouse=True)
def _reset_schema():
    Base.metadata.create_all(bind=_val_engine)
    yield
    with _val_engine.begin() as conn:
        conn.execute(text("PRAGMA foreign_keys=OFF"))
        Base.metadata.drop_all(bind=conn)
        conn.execute(text("PRAGMA foreign_keys=ON"))


@pytest.fixture()
def db():
    with _ValSession() as session:
        yield session


PRINCIPAL_A = AuthenticatedPrincipal(
    key_id="key-g113-test",
    tenant_id="tenant-g113-test",
    name="G113 Tester",
    scopes=["definitions:write"],
)


# ===========================================================================
# Category 1: Valid Schemas (10 tests)
# ===========================================================================
class TestValidSchemas:
    def test_simple_object_with_string_property(self):
        res = validate_definition_version(
            task_type="noop",
            execution_type="builtin",
            configuration_json={},
            input_schema_json={"type": "object", "properties": {"name": {"type": "string"}}},
        )
        assert res.is_valid is True
        assert res.status == "VALID"
        assert len(res.errors) == 0

    def test_primitive_number_and_integer_types(self):
        res = validate_definition_version(
            task_type="compute_prime",
            execution_type="builtin",
            configuration_json={},
            input_schema_json={
                "type": "object",
                "properties": {"n": {"type": "integer"}, "ratio": {"type": "number"}},
            },
        )
        assert res.is_valid is True

    def test_array_schema_with_items_definition(self):
        res = validate_definition_version(
            task_type="json_transform",
            execution_type="builtin",
            configuration_json={},
            input_schema_json={
                "type": "object",
                "properties": {"records": {"type": "array", "items": {"type": "integer"}}},
            },
        )
        assert res.is_valid is True

    def test_complex_nested_object_with_required_and_defaults(self):
        schema = {
            "type": "object",
            "required": ["user_id"],
            "properties": {
                "user_id": {"type": "string"},
                "metadata": {
                    "type": "object",
                    "properties": {"active": {"type": "boolean", "default": True}},
                    "additionalProperties": False,
                },
            },
        }
        res = validate_definition_version(
            task_type="noop",
            execution_type="builtin",
            configuration_json={},
            input_schema_json=schema,
        )
        assert res.is_valid is True

    def test_explicit_draft_2020_12_schema_identifier(self):
        schema = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": {"task": {"type": "string"}},
        }
        res = validate_definition_version(
            task_type="noop",
            execution_type="builtin",
            configuration_json={},
            input_schema_json=schema,
        )
        assert res.is_valid is True

    def test_both_input_and_output_schemas_valid(self):
        in_schema = {"type": "object", "properties": {"n": {"type": "integer"}}}
        out_schema = {"type": "object", "properties": {"prime_count": {"type": "integer"}}}
        res = validate_definition_version(
            task_type="compute_prime",
            execution_type="builtin",
            configuration_json={},
            input_schema_json=in_schema,
            output_schema_json=out_schema,
        )
        assert res.is_valid is True
        assert res.summary["input_schema"] == "PASSED"
        assert res.summary["output_schema"] == "PASSED"

    def test_boolean_schema_true(self):
        res = validate_definition_version(
            task_type="noop",
            execution_type="builtin",
            configuration_json={},
            input_schema_json=True,
            output_schema_json=True,
        )
        assert res.is_valid is True

    def test_enum_and_const_keywords(self):
        schema = {
            "type": "object",
            "properties": {
                "action": {"const": "EXECUTE"},
                "mode": {"enum": ["FAST", "THOROUGH", "DRY_RUN"]},
            },
        }
        res = validate_definition_version(
            task_type="noop",
            execution_type="builtin",
            configuration_json={},
            input_schema_json=schema,
        )
        assert res.is_valid is True

    def test_combinator_keywords_one_of_and_any_of(self):
        schema = {
            "type": "object",
            "oneOf": [
                {"properties": {"by_id": {"type": "integer"}}, "required": ["by_id"]},
                {"properties": {"by_name": {"type": "string"}}, "required": ["by_name"]},
            ],
        }
        res = validate_definition_version(
            task_type="noop",
            execution_type="builtin",
            configuration_json={},
            input_schema_json=schema,
        )
        assert res.is_valid is True

    def test_numeric_range_and_multiple_of_constraints(self):
        schema = {
            "type": "object",
            "properties": {
                "batch_size": {"type": "integer", "minimum": 1, "maximum": 500, "multipleOf": 5}
            },
        }
        res = validate_definition_version(
            task_type="noop",
            execution_type="builtin",
            configuration_json={},
            input_schema_json=schema,
        )
        assert res.is_valid is True


# ===========================================================================
# Category 2: Invalid Schemas (10 tests)
# ===========================================================================
class TestInvalidSchemas:
    def test_non_dict_non_bool_schema_rejected(self):
        res = validate_definition_version(
            task_type="noop",
            execution_type="builtin",
            configuration_json={},
            input_schema_json="not-a-valid-schema",  # type: ignore
        )
        assert res.is_valid is False
        assert any(e.code == ERR_INVALID_SCHEMA_TYPE for e in res.errors)

    def test_invalid_type_keyword_syntax(self):
        res = validate_definition_version(
            task_type="noop",
            execution_type="builtin",
            configuration_json={},
            input_schema_json={"type": "unrecognized_primitive"},
        )
        assert res.is_valid is False
        assert any(e.code == ERR_INVALID_SCHEMA_SYNTAX for e in res.errors)

    def test_invalid_items_syntax(self):
        res = validate_definition_version(
            task_type="noop",
            execution_type="builtin",
            configuration_json={},
            input_schema_json={"type": "array", "items": "not-a-schema"},
        )
        assert res.is_valid is False
        assert any(e.code == ERR_INVALID_SCHEMA_SYNTAX for e in res.errors)

    def test_invalid_required_keyword_not_array(self):
        res = validate_definition_version(
            task_type="noop",
            execution_type="builtin",
            configuration_json={},
            input_schema_json={"type": "object", "required": "field_name"},
        )
        assert res.is_valid is False
        assert any(e.code == ERR_INVALID_SCHEMA_SYNTAX for e in res.errors)

    def test_unsupported_schema_draft_04_rejected(self):
        schema = {
            "$schema": "http://json-schema.org/draft-04/schema#",
            "type": "object",
        }
        res = validate_definition_version(
            task_type="noop",
            execution_type="builtin",
            configuration_json={},
            input_schema_json=schema,
        )
        assert res.is_valid is False
        assert any(e.code == ERR_UNSUPPORTED_SCHEMA_DRAFT for e in res.errors)

    def test_unsupported_schema_draft_07_rejected(self):
        schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
        }
        res = validate_definition_version(
            task_type="noop",
            execution_type="builtin",
            configuration_json={},
            input_schema_json=schema,
        )
        assert res.is_valid is False
        assert any(e.code == ERR_UNSUPPORTED_SCHEMA_DRAFT for e in res.errors)

    def test_unsupported_custom_draft_uri_rejected(self):
        schema = {
            "$schema": "https://custom.org/my-own-schema-meta",
            "type": "object",
        }
        res = validate_definition_version(
            task_type="noop",
            execution_type="builtin",
            configuration_json={},
            input_schema_json=schema,
        )
        assert res.is_valid is False
        assert any(e.code == ERR_UNSUPPORTED_SCHEMA_DRAFT for e in res.errors)

    def test_invalid_minimum_type_non_numeric(self):
        res = validate_definition_version(
            task_type="noop",
            execution_type="builtin",
            configuration_json={},
            input_schema_json={"type": "integer", "minimum": "ten"},
        )
        assert res.is_valid is False
        assert any(e.code == ERR_INVALID_SCHEMA_SYNTAX for e in res.errors)

    def test_invalid_pattern_regex_syntax(self):
        res = validate_definition_version(
            task_type="noop",
            execution_type="builtin",
            configuration_json={},
            input_schema_json={"type": "string", "pattern": "[unclosed-bracket"},
        )
        assert res.is_valid is False
        assert any(e.code == ERR_INVALID_SCHEMA_SYNTAX for e in res.errors)

    def test_invalid_additional_properties_type(self):
        res = validate_definition_version(
            task_type="noop",
            execution_type="builtin",
            configuration_json={},
            input_schema_json={"type": "object", "additionalProperties": "invalid"},
        )
        assert res.is_valid is False
        assert any(e.code == ERR_INVALID_SCHEMA_SYNTAX for e in res.errors)


# ===========================================================================
# Category 3: Local $ref Resolution (5 tests)
# ===========================================================================
class TestLocalRefResolution:
    def test_local_ref_to_defs(self):
        schema = {
            "$defs": {"person": {"type": "object", "properties": {"name": {"type": "string"}}}},
            "type": "object",
            "properties": {"author": {"$ref": "#/$defs/person"}},
        }
        res = validate_definition_version(
            task_type="noop",
            execution_type="builtin",
            configuration_json={},
            input_schema_json=schema,
        )
        assert res.is_valid is True

    def test_local_ref_to_definitions(self):
        schema = {
            "definitions": {"id_type": {"type": "integer", "minimum": 1}},
            "type": "object",
            "properties": {"account_id": {"$ref": "#/definitions/id_type"}},
        }
        res = validate_definition_version(
            task_type="noop",
            execution_type="builtin",
            configuration_json={},
            input_schema_json=schema,
        )
        assert res.is_valid is True

    def test_nested_local_pointer_traversal(self):
        schema = {
            "entities": {
                "geo": {"location": {"type": "object", "properties": {"lat": {"type": "number"}}}}
            },
            "type": "object",
            "properties": {"origin": {"$ref": "#/entities/geo/location"}},
        }
        res = validate_definition_version(
            task_type="noop",
            execution_type="builtin",
            configuration_json={},
            input_schema_json=schema,
        )
        assert res.is_valid is True

    def test_root_self_reference(self):
        schema = {
            "type": "object",
            "properties": {"self_child": {"$ref": "#"}},
        }
        res = validate_definition_version(
            task_type="noop",
            execution_type="builtin",
            configuration_json={},
            input_schema_json=schema,
        )
        # Self ref is allowed in Draft 2020-12 unless in a direct cycle without property boundary
        assert res.is_valid is True

    def test_multiple_sibling_local_refs(self):
        schema = {
            "$defs": {
                "tag": {"type": "string"},
                "count": {"type": "integer"},
            },
            "type": "object",
            "properties": {
                "t1": {"$ref": "#/$defs/tag"},
                "t2": {"$ref": "#/$defs/tag"},
                "c1": {"$ref": "#/$defs/count"},
            },
        }
        res = validate_definition_version(
            task_type="noop",
            execution_type="builtin",
            configuration_json={},
            input_schema_json=schema,
        )
        assert res.is_valid is True


# ===========================================================================
# Category 4: Remote Reference Rejection (5 tests)
# ===========================================================================
class TestRemoteRefRejection:
    def test_http_remote_reference_rejected(self):
        schema = {"$ref": "http://example.com/schemas/user.json"}
        res = validate_definition_version(
            task_type="noop",
            execution_type="builtin",
            configuration_json={},
            input_schema_json=schema,
        )
        assert res.is_valid is False
        assert any(e.code == ERR_REMOTE_REFERENCE_PROHIBITED for e in res.errors)

    def test_https_remote_reference_rejected(self):
        schema = {"properties": {"user": {"$ref": "https://secure.api.org/v1/user.json"}}}
        res = validate_definition_version(
            task_type="noop",
            execution_type="builtin",
            configuration_json={},
            input_schema_json=schema,
        )
        assert res.is_valid is False
        assert any(e.code == ERR_REMOTE_REFERENCE_PROHIBITED for e in res.errors)

    def test_file_protocol_reference_rejected(self):
        schema = {"$ref": "file:///etc/taskforge/secret.json"}
        res = validate_definition_version(
            task_type="noop",
            execution_type="builtin",
            configuration_json={},
            input_schema_json=schema,
        )
        assert res.is_valid is False
        assert any(e.code == ERR_REMOTE_REFERENCE_PROHIBITED for e in res.errors)

    def test_ftp_reference_rejected(self):
        schema = {"$ref": "ftp://ftp.example.com/data.json"}
        res = validate_definition_version(
            task_type="noop",
            execution_type="builtin",
            configuration_json={},
            input_schema_json=schema,
        )
        assert res.is_valid is False
        assert any(e.code == ERR_REMOTE_REFERENCE_PROHIBITED for e in res.errors)

    def test_relative_file_uri_rejected(self):
        schema = {"$ref": "external_model.json#/definitions/person"}
        res = validate_definition_version(
            task_type="noop",
            execution_type="builtin",
            configuration_json={},
            input_schema_json=schema,
        )
        assert res.is_valid is False
        assert any(e.code == ERR_REMOTE_REFERENCE_PROHIBITED for e in res.errors)


# ===========================================================================
# Category 5: Resource Limits & Cycle Safety (8 tests)
# ===========================================================================
class TestResourceLimits:
    def test_schema_size_within_limit_passes(self):
        # 1 KB schema
        schema = {
            "type": "object",
            "properties": {f"field_{i}": {"type": "string"} for i in range(20)},
        }
        res = validate_definition_version(
            task_type="noop",
            execution_type="builtin",
            configuration_json={},
            input_schema_json=schema,
        )
        assert res.is_valid is True

    def test_schema_size_exceeding_64kb_fails(self):
        # Generate schema exceeding MAX_SCHEMA_SIZE_BYTES (65,536 bytes)
        large_dict = {
            f"huge_field_name_padding_{i}": {"type": "string", "description": "x" * 200}
            for i in range(400)
        }
        schema = {"type": "object", "properties": large_dict}
        res = validate_definition_version(
            task_type="noop",
            execution_type="builtin",
            configuration_json={},
            input_schema_json=schema,
        )
        assert res.is_valid is False
        assert any(e.code == ERR_SCHEMA_SIZE_EXCEEDED for e in res.errors)

    def test_nesting_depth_within_limit_passes(self):
        # 5 nesting levels (each adds object + properties = 2 levels; 5 * 2 + 1 = 11, limit is 15)
        curr: dict = {"type": "string"}
        for _ in range(5):
            curr = {"type": "object", "properties": {"child": curr}}
        res = validate_definition_version(
            task_type="noop",
            execution_type="builtin",
            configuration_json={},
            input_schema_json=curr,
        )
        assert res.is_valid is True

    def test_nesting_depth_exceeding_15_fails(self):
        # 10 nesting levels (10 * 2 + 1 = 21, limit is 15)
        curr: dict = {"type": "string"}
        for _ in range(10):
            curr = {"type": "object", "properties": {"child": curr}}
        res = validate_definition_version(
            task_type="noop",
            execution_type="builtin",
            configuration_json={},
            input_schema_json=curr,
        )
        assert res.is_valid is False
        assert any(e.code == ERR_SCHEMA_DEPTH_EXCEEDED for e in res.errors)

    def test_reference_traversal_hops_within_limit_passes(self):
        # Chain of 3 hops: a -> b -> c -> int
        schema = {
            "$defs": {
                "c": {"type": "integer"},
                "b": {"$ref": "#/$defs/c"},
                "a": {"$ref": "#/$defs/b"},
            },
            "type": "object",
            "properties": {"val": {"$ref": "#/$defs/a"}},
        }
        res = validate_definition_version(
            task_type="noop",
            execution_type="builtin",
            configuration_json={},
            input_schema_json=schema,
        )
        assert res.is_valid is True

    def test_reference_traversal_exceeding_max_hops_fails(self):
        # Chain of 12 hops (limit is 10)
        defs = {f"node_{i}": {"$ref": f"#/$defs/node_{i + 1}"} for i in range(12)}
        defs["node_12"] = {"type": "string"}
        schema = {"$defs": defs, "$ref": "#/$defs/node_0"}
        res = validate_definition_version(
            task_type="noop",
            execution_type="builtin",
            configuration_json={},
            input_schema_json=schema,
        )
        assert res.is_valid is False
        assert any(e.code == ERR_REFERENCE_DEPTH_EXCEEDED for e in res.errors)

    def test_direct_circular_reference_loop_detected(self):
        # Direct self-loop: #/$defs/node -> #/$defs/node
        schema = {
            "$defs": {"node": {"$ref": "#/$defs/node"}},
            "$ref": "#/$defs/node",
        }
        res = validate_definition_version(
            task_type="noop",
            execution_type="builtin",
            configuration_json={},
            input_schema_json=schema,
        )
        assert res.is_valid is False
        assert any(e.code == ERR_CIRCULAR_REFERENCE_DETECTED for e in res.errors)

    def test_indirect_multi_hop_circular_reference_loop_detected(self):
        # Indirect cycle: a -> b -> c -> a
        schema = {
            "$defs": {
                "a": {"$ref": "#/$defs/b"},
                "b": {"$ref": "#/$defs/c"},
                "c": {"$ref": "#/$defs/a"},
            },
            "$ref": "#/$defs/a",
        }
        res = validate_definition_version(
            task_type="noop",
            execution_type="builtin",
            configuration_json={},
            input_schema_json=schema,
        )
        assert res.is_valid is False
        assert any(e.code == ERR_CIRCULAR_REFERENCE_DETECTED for e in res.errors)


# ===========================================================================
# Category 6: TaskRegistry Compatibility (6 tests)
# ===========================================================================
class TestRegistryCompatibility:
    def test_registered_task_type_noop_passes(self):
        res = validate_definition_version(
            task_type="noop",
            execution_type="builtin",
            configuration_json={},
            input_schema_json={"type": "object"},
        )
        assert res.summary["execution_binding"] == "PASSED"

    def test_registered_task_type_compute_prime_passes(self):
        res = validate_definition_version(
            task_type="compute_prime",
            execution_type="builtin",
            configuration_json={},
            input_schema_json={"type": "object"},
        )
        assert res.summary["execution_binding"] == "PASSED"

    def test_registered_task_type_json_transform_passes(self):
        res = validate_definition_version(
            task_type="json_transform",
            execution_type="builtin",
            configuration_json={},
            input_schema_json={"type": "object"},
        )
        assert res.summary["execution_binding"] == "PASSED"

    def test_unregistered_task_type_fails(self):
        res = validate_definition_version(
            task_type="nonexistent_alien_task",
            execution_type="builtin",
            configuration_json={},
            input_schema_json={"type": "object"},
        )
        assert res.is_valid is False
        assert any(e.code == ERR_UNKNOWN_TASK_TYPE for e in res.errors)
        assert res.summary["execution_binding"] == "FAILED"

    def test_empty_task_type_fails(self):
        res = validate_definition_version(
            task_type="",
            execution_type="builtin",
            configuration_json={},
            input_schema_json={"type": "object"},
        )
        assert res.is_valid is False
        assert any(e.code == ERR_UNKNOWN_TASK_TYPE for e in res.errors)

    def test_unsupported_execution_type_fails(self):
        res = validate_definition_version(
            task_type="noop",
            execution_type="http",
            configuration_json={},
            input_schema_json={"type": "object"},
        )
        assert res.is_valid is False
        assert any(e.code == ERR_UNSUPPORTED_EXECUTION_TYPE for e in res.errors)


# ===========================================================================
# Category 7: Deterministic Reporting (5 tests)
# ===========================================================================
class TestDeterministicReporting:
    def test_repeated_validation_of_identical_input_is_deterministic(self):
        payload = {
            "task_type": "compute_prime",
            "execution_type": "builtin",
            "configuration_json": {"limit": 100},
            "input_schema_json": {"type": "object", "properties": {"n": {"type": "integer"}}},
        }
        res1 = validate_definition_version(**payload)
        res2 = validate_definition_version(**payload)

        assert res1.is_valid == res2.is_valid
        assert res1.status == res2.status
        assert [e.to_dict() for e in res1.errors] == [e.to_dict() for e in res2.errors]

    def test_error_list_is_sorted_deterministically(self):
        # Schema with multiple errors across different paths
        schema = {
            "type": "object",
            "properties": {
                "z_field": {"type": "invalid_z"},
                "a_field": {"type": "invalid_a"},
            },
        }
        res = validate_definition_version(
            task_type="noop",
            execution_type="builtin",
            configuration_json={},
            input_schema_json=schema,
        )
        paths = [e.path for e in res.errors]
        assert paths == sorted(paths)

    def test_error_count_matches_reported_errors(self):
        schema = {"type": "object", "required": 123, "properties": {"x": {"type": "bad"}}}
        res = validate_definition_version(
            task_type="noop",
            execution_type="builtin",
            configuration_json={},
            input_schema_json=schema,
        )
        report = res.to_dict()
        assert report["error_count"] == len(res.errors)

    def test_max_validation_errors_capped_at_limit(self):
        # Generate 30 invalid properties
        bad_props = {f"prop_{i}": {"type": f"bad_type_{i}"} for i in range(30)}
        schema = {"type": "object", "properties": bad_props}
        res = validate_definition_version(
            task_type="noop",
            execution_type="builtin",
            configuration_json={},
            input_schema_json=schema,
        )
        assert len(res.errors) <= MAX_VALIDATION_ERRORS

    def test_summary_dictionary_contains_required_contract_keys(self):
        res = validate_definition_version(
            task_type="noop",
            execution_type="builtin",
            configuration_json={},
            input_schema_json={"type": "object"},
            output_schema_json={"type": "object"},
        )
        summary = res.summary
        assert "execution_binding" in summary
        assert "input_schema" in summary
        assert "output_schema" in summary
        assert summary["execution_binding"] == "PASSED"
        assert summary["input_schema"] == "PASSED"
        assert summary["output_schema"] == "PASSED"


# ===========================================================================
# Category 8: Historical Immutability (4 tests)
# ===========================================================================
class TestHistoricalImmutability:
    def test_validation_does_not_mutate_version_content_hash(self, db):
        svc = DefinitionService(db)
        defn, v1 = svc.create_definition(
            principal=PRINCIPAL_A,
            name="Immutable Hash Test",
            description=None,
            execution_type="builtin",
            task_type="noop",
            configuration_json={"cfg": 1},
            input_schema_json={"type": "object"},
        )
        initial_hash = v1.content_hash
        assert v1.validation_status == "UNVALIDATED"

        updated_v1, report = svc.validate_version(
            principal=PRINCIPAL_A,
            definition_id=defn.id,
            version_number=1,
        )

        assert updated_v1.content_hash == initial_hash
        assert updated_v1.validation_status == "VALID"
        assert report.is_valid is True

    def test_revalidating_historical_version_preserves_version_and_id(self, db):
        svc = DefinitionService(db)
        defn, v1 = svc.create_definition(
            principal=PRINCIPAL_A,
            name="Historical Preservation",
            description=None,
            execution_type="builtin",
            task_type="noop",
            configuration_json={},
            input_schema_json={"type": "object"},
        )
        v2 = svc.create_version(
            principal=PRINCIPAL_A,
            definition_id=defn.id,
            task_type="noop",
            configuration_json={},
            input_schema_json={"type": "object"},
        )

        updated_v1, _ = svc.validate_version(
            principal=PRINCIPAL_A,
            definition_id=defn.id,
            version_number=1,
        )
        assert updated_v1.id == v1.id
        assert updated_v1.version == 1

        v2_fetch = svc.get_version(PRINCIPAL_A, defn.id, 2)
        assert v2_fetch.id == v2.id
        assert v2_fetch.version == 2
        assert v2_fetch.validation_status == "UNVALIDATED"

    def test_executable_payload_remains_strictly_identical_after_validation(self, db):
        svc = DefinitionService(db)
        cfg = {"strict": True, "limit": 42}
        schema = {"type": "object", "properties": {"id": {"type": "integer"}}}
        defn, v1 = svc.create_definition(
            principal=PRINCIPAL_A,
            name="Payload Integrity",
            description=None,
            execution_type="builtin",
            task_type="noop",
            configuration_json=cfg,
            input_schema_json=schema,
        )

        updated_v1, _ = svc.validate_version(
            principal=PRINCIPAL_A,
            definition_id=defn.id,
            version_number=1,
        )

        assert updated_v1.configuration_json == cfg
        assert updated_v1.input_schema_json == schema

    def test_validating_v2_does_not_affect_v1_status_or_report(self, db):
        svc = DefinitionService(db)
        defn, v1 = svc.create_definition(
            principal=PRINCIPAL_A,
            name="Independent Version Validation",
            description=None,
            execution_type="builtin",
            task_type="noop",
            configuration_json={},
            input_schema_json={"type": "object"},
        )
        svc.validate_version(
            principal=PRINCIPAL_A,
            definition_id=defn.id,
            version_number=1,
        )

        # Create v2 with invalid schema
        svc.create_version(
            principal=PRINCIPAL_A,
            definition_id=defn.id,
            task_type="noop",
            configuration_json={},
            input_schema_json={"type": "unrecognized_type"},
        )

        svc.validate_version(
            principal=PRINCIPAL_A,
            definition_id=defn.id,
            version_number=2,
        )

        # Check v1 remains VALID
        v1_check = svc.get_version(PRINCIPAL_A, defn.id, 1)
        assert v1_check.validation_status == "VALID"

        # Check v2 is INVALID
        v2_check = svc.get_version(PRINCIPAL_A, defn.id, 2)
        assert v2_check.validation_status == "INVALID"


# ===========================================================================
# Category 9: Failure & Security Handling (7 tests)
# ===========================================================================
class TestFailureAndSecurityHandling:
    def test_schema_containing_tf_live_secret_string_is_rejected(self):
        schema = {
            "type": "object",
            "description": "Auth using tf_live_0123456789abcdef01234567",
        }
        res = validate_definition_version(
            task_type="noop",
            execution_type="builtin",
            configuration_json={},
            input_schema_json=schema,
        )
        assert res.is_valid is False
        assert any(e.code == ERR_PROHIBITED_SECRET_DETECTED for e in res.errors)

    def test_output_schema_containing_tf_live_secret_is_rejected(self):
        schema = {
            "type": "object",
            "properties": {"token": {"default": "tf_live_leaked_in_output_schema"}},
        }
        res = validate_definition_version(
            task_type="noop",
            execution_type="builtin",
            configuration_json={},
            input_schema_json={"type": "object"},
            output_schema_json=schema,
        )
        assert res.is_valid is False
        assert any(e.code == ERR_PROHIBITED_SECRET_DETECTED for e in res.errors)

    def test_unexpected_validator_exception_produces_invalid(self):
        # Simulate unexpected internal failure inside Draft202012Validator
        with patch(
            "taskforge.definitions.validation.Draft202012Validator.check_schema",
            side_effect=RuntimeError("injected_crash"),
        ):
            res = validate_definition_version(
                task_type="noop",
                execution_type="builtin",
                configuration_json={},
                input_schema_json={"type": "object"},
            )
            assert res.is_valid is False
            assert res.status == "INVALID"

    def test_internal_exception_error_code_is_internal_validator_error(self):
        with patch(
            "taskforge.definitions.validation.Draft202012Validator.check_schema",
            side_effect=ZeroDivisionError("unexpected"),
        ):
            res = validate_definition_version(
                task_type="noop",
                execution_type="builtin",
                configuration_json={},
                input_schema_json={"type": "object"},
            )
            assert any(e.code == ERR_INTERNAL_VALIDATOR_ERROR for e in res.errors)

    def test_activating_invalid_version_is_strictly_rejected(self, db):
        svc = DefinitionService(db)
        defn, _ = svc.create_definition(
            principal=PRINCIPAL_A,
            name="Invalid Activation Test",
            description=None,
            execution_type="builtin",
            task_type="noop",
            configuration_json={},
            input_schema_json={"type": "bad_type"},
        )
        svc.validate_version(
            principal=PRINCIPAL_A,
            definition_id=defn.id,
            version_number=1,
        )

        with pytest.raises(DefinitionValidationError, match="must be VALID before activation"):
            svc.activate_version(
                principal=PRINCIPAL_A,
                definition_id=defn.id,
                version_number=1,
            )

    def test_activating_unvalidated_version_is_strictly_rejected(self, db):
        svc = DefinitionService(db)
        defn, v1 = svc.create_definition(
            principal=PRINCIPAL_A,
            name="Unvalidated Activation Test",
            description=None,
            execution_type="builtin",
            task_type="noop",
            configuration_json={},
            input_schema_json={"type": "object"},
        )
        assert v1.validation_status == "UNVALIDATED"

        with pytest.raises(DefinitionValidationError, match="must be VALID before activation"):
            svc.activate_version(
                principal=PRINCIPAL_A,
                definition_id=defn.id,
                version_number=1,
            )

    def test_valid_version_activates_successfully(self, db):
        svc = DefinitionService(db)
        defn, _ = svc.create_definition(
            principal=PRINCIPAL_A,
            name="Valid Activation Success",
            description=None,
            execution_type="builtin",
            task_type="noop",
            configuration_json={},
            input_schema_json={"type": "object"},
        )
        svc.validate_version(
            principal=PRINCIPAL_A,
            definition_id=defn.id,
            version_number=1,
        )

        activated_defn = svc.activate_version(
            principal=PRINCIPAL_A,
            definition_id=defn.id,
            version_number=1,
        )
        assert activated_defn.status == "ACTIVE"
        assert activated_defn.active_version_id is not None


# ===========================================================================
# Category 10: Recursive Schema Semantics (G11.3-R1)
# ===========================================================================
class TestRecursiveSchemaSemantics:
    """Verifies Draft 2020-12 recursive schema semantics, distinguishing valid
    recursion from malformed cycles, unresolvable pointers, and resource exhaustion.
    """

    def test_valid_recursive_tree_schema_accepted(self):
        """Tree nodes referencing their own schema definition via $defs are valid Draft 2020-12."""
        schema = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$defs": {
                "treeNode": {
                    "type": "object",
                    "required": ["val"],
                    "properties": {
                        "val": {"type": "integer"},
                        "children": {
                            "type": "array",
                            "items": {"$ref": "#/$defs/treeNode"},
                        },
                    },
                }
            },
            "$ref": "#/$defs/treeNode",
        }
        res = validate_definition_version(
            task_type="noop",
            execution_type="builtin",
            configuration_json={},
            input_schema_json=schema,
        )
        assert res.is_valid is True
        assert res.status == "VALID"
        assert len(res.errors) == 0

    def test_valid_mutually_recursive_schemas_accepted(self):
        """Mutually recursive schemas (nodeA -> nodeB -> nodeA) are valid when property-bounded."""
        schema = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$defs": {
                "nodeA": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "next_b": {"$ref": "#/$defs/nodeB"},
                    },
                },
                "nodeB": {
                    "type": "object",
                    "properties": {
                        "weight": {"type": "number"},
                        "next_a": {"$ref": "#/$defs/nodeA"},
                    },
                },
            },
            "$ref": "#/$defs/nodeA",
        }
        res = validate_definition_version(
            task_type="noop",
            execution_type="builtin",
            configuration_json={},
            input_schema_json=schema,
        )
        assert res.is_valid is True
        assert res.status == "VALID"

    def test_valid_dynamic_ref_recursion_accepted(self):
        """Recursive schemas using $dynamicRef and $dynamicAnchor are valid in Draft 2020-12."""
        schema = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$defs": {
                "node": {
                    "$dynamicAnchor": "treeItem",
                    "type": "object",
                    "properties": {
                        "data": {"type": "string"},
                        "sub": {"$dynamicRef": "#treeItem"},
                    },
                }
            },
            "$dynamicRef": "#treeItem",
        }
        res = validate_definition_version(
            task_type="noop",
            execution_type="builtin",
            configuration_json={},
            input_schema_json=schema,
        )
        assert res.is_valid is True
        assert res.status == "VALID"

    def test_schema_compilation_distinguished_from_instance_validation(self):
        """G11.3 establishes schema contract validity without executing task handlers."""
        schema = {
            "type": "object",
            "properties": {"count": {"type": "integer"}},
        }
        # Compiling schema does not call any handlers
        validator = compile_schema(schema)
        assert validator is not None
        # Instance validation confirms the schema contract against valid and invalid payloads
        assert validate_schema_instance(schema, {"count": 42}) == []
        errs = validate_schema_instance(schema, {"count": "not_an_int"})
        assert len(errs) > 0
        assert any("count" in e for e in errs)

    def test_legitimate_recursive_instance_structures_accepted(self):
        """Legitimate deeply nested recursive instances are accepted by the compiled schema."""
        schema = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$defs": {
                "node": {
                    "type": "object",
                    "required": ["val"],
                    "properties": {
                        "val": {"type": "integer"},
                        "left": {"$ref": "#/$defs/node"},
                        "right": {"$ref": "#/$defs/node"},
                    },
                }
            },
            "$ref": "#/$defs/node",
        }
        errs = validate_schema_instance(schema, {"val": 1, "left": {"val": 2, "left": {"val": 3}}})
        assert errs == []

    def test_invalid_recursive_instance_data_rejected_with_precise_path(self):
        """An invalid data field at a deep recursive level is reported at its exact JSON path."""
        schema = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$defs": {
                "node": {
                    "type": "object",
                    "required": ["val"],
                    "properties": {
                        "val": {"type": "integer"},
                        "child": {"$ref": "#/$defs/node"},
                    },
                }
            },
            "$ref": "#/$defs/node",
        }
        bad_instance = {
            "val": 1,
            "child": {
                "val": 2,
                "child": {
                    "val": "invalid_string_not_int",
                },
            },
        }
        errs = validate_schema_instance(schema, bad_instance)
        assert len(errs) > 0
        assert any("val" in e for e in errs)

    def test_malformed_pure_alias_loop_rejected_as_circular(self):
        """A pure reference loop with no structural definitions is rejected as circular."""
        schema = {
            "$defs": {
                "a": {"$ref": "#/$defs/b"},
                "b": {"$ref": "#/$defs/a"},
            },
            "$ref": "#/$defs/a",
        }
        res = validate_definition_version(
            task_type="noop",
            execution_type="builtin",
            configuration_json={},
            input_schema_json=schema,
        )
        assert res.is_valid is False
        assert any(e.code == ERR_CIRCULAR_REFERENCE_DETECTED for e in res.errors)

    def test_unresolvable_pointer_rejected_not_cycle(self):
        """Unresolvable pointers produce ERR_UNRESOLVED_REFERENCE, not circular error."""
        schema = {"$ref": "#/$defs/nonexistent_schema"}
        res = validate_definition_version(
            task_type="noop",
            execution_type="builtin",
            configuration_json={},
            input_schema_json=schema,
        )
        assert res.is_valid is False
        assert any(e.code == ERR_UNRESOLVED_REFERENCE for e in res.errors)
        assert not any(e.code == ERR_CIRCULAR_REFERENCE_DETECTED for e in res.errors)

    def test_unresolvable_anchor_rejected(self):
        """Unresolvable anchor references produce ERR_UNRESOLVED_REFERENCE."""
        schema = {"$dynamicRef": "#missingAnchor"}
        res = validate_definition_version(
            task_type="noop",
            execution_type="builtin",
            configuration_json={},
            input_schema_json=schema,
        )
        assert res.is_valid is False
        assert any(e.code == ERR_UNRESOLVED_REFERENCE for e in res.errors)


# ===========================================================================
# Category 11: Reference Security Boundaries (G11.3-R2)
# ===========================================================================
class TestReferenceSecurityBoundaries:
    """Verifies local-only reference security against encoded URIs, scheme variations,
    remote $id injection, nested references, and restricted resolver boundaries.
    """

    def test_uri_encoded_external_scheme_rejected(self):
        """URI-encoded scheme %68%74%74%70%3a//evil.com is strictly rejected."""
        schema = {"$ref": "%68%74%74%70%3a//evil.com/schema.json"}
        res = validate_definition_version(
            task_type="noop",
            execution_type="builtin",
            configuration_json={},
            input_schema_json=schema,
        )
        assert res.is_valid is False
        assert any(e.code == ERR_REMOTE_REFERENCE_PROHIBITED for e in res.errors)

    def test_uri_encoded_file_scheme_rejected(self):
        """URI-encoded file scheme %66%69%6c%65%3a///etc/passwd is strictly rejected."""
        schema = {"$ref": "%66%69%6c%65%3a///etc/passwd"}
        res = validate_definition_version(
            task_type="noop",
            execution_type="builtin",
            configuration_json={},
            input_schema_json=schema,
        )
        assert res.is_valid is False
        assert any(e.code == ERR_REMOTE_REFERENCE_PROHIBITED for e in res.errors)

    def test_double_encoded_remote_scheme_rejected(self):
        """Double-encoded remote scheme is detected through recursive unquoting."""
        schema = {"$ref": "%2568%2574%2574%2570%253a//evil.com"}
        res = validate_definition_version(
            task_type="noop",
            execution_type="builtin",
            configuration_json={},
            input_schema_json=schema,
        )
        assert res.is_valid is False
        assert any(e.code == ERR_REMOTE_REFERENCE_PROHIBITED for e in res.errors)

    def test_uppercase_scheme_variation_rejected(self):
        """Case variations like HTTP:// and FILE:/// are strictly rejected."""
        schema = {"$ref": "HTTP://example.com/schema.json"}
        res = validate_definition_version(
            task_type="noop",
            execution_type="builtin",
            configuration_json={},
            input_schema_json=schema,
        )
        assert res.is_valid is False
        assert any(e.code == ERR_REMOTE_REFERENCE_PROHIBITED for e in res.errors)

    def test_protocol_relative_reference_rejected(self):
        """Protocol-relative URIs like //evil.com/schema.json are strictly rejected."""
        schema = {"$ref": "//evil.com/schema.json"}
        res = validate_definition_version(
            task_type="noop",
            execution_type="builtin",
            configuration_json={},
            input_schema_json=schema,
        )
        assert res.is_valid is False
        assert any(e.code == ERR_REMOTE_REFERENCE_PROHIBITED for e in res.errors)

    def test_whitespace_padded_remote_reference_rejected(self):
        """Whitespace-padded remote references are rejected."""
        schema = {"$ref": "  https://evil.com/schema.json  "}
        res = validate_definition_version(
            task_type="noop",
            execution_type="builtin",
            configuration_json={},
            input_schema_json=schema,
        )
        assert res.is_valid is False
        assert any(e.code == ERR_REMOTE_REFERENCE_PROHIBITED for e in res.errors)

    def test_fragment_smuggled_remote_scheme_rejected(self):
        """Fragment smuggling like #http://evil.com is strictly rejected."""
        schema = {"$ref": "#http://evil.com/schema"}
        res = validate_definition_version(
            task_type="noop",
            execution_type="builtin",
            configuration_json={},
            input_schema_json=schema,
        )
        assert res.is_valid is False
        assert any(e.code == ERR_REMOTE_REFERENCE_PROHIBITED for e in res.errors)

    def test_remote_id_at_root_rejected(self):
        """Root $id pointing to remote URL is strictly rejected to protect base URI."""
        schema = {
            "$id": "https://evil.com/schemas/base.json",
            "type": "object",
        }
        res = validate_definition_version(
            task_type="noop",
            execution_type="builtin",
            configuration_json={},
            input_schema_json=schema,
        )
        assert res.is_valid is False
        assert any(e.code == ERR_REMOTE_REFERENCE_PROHIBITED for e in res.errors)

    def test_remote_id_in_subschema_rejected(self):
        """Subschema $id pointing to external location is strictly rejected."""
        schema = {
            "$defs": {
                "sub": {
                    "$id": "http://evil.com/sub.json",
                    "type": "string",
                }
            },
            "type": "object",
        }
        res = validate_definition_version(
            task_type="noop",
            execution_type="builtin",
            configuration_json={},
            input_schema_json=schema,
        )
        assert res.is_valid is False
        assert any(e.code == ERR_REMOTE_REFERENCE_PROHIBITED for e in res.errors)

    def test_uri_encoded_remote_id_rejected(self):
        """URI-encoded remote $id is rejected."""
        schema = {
            "$id": "%68%74%74%70%3a//evil.com/base.json",
            "type": "object",
        }
        res = validate_definition_version(
            task_type="noop",
            execution_type="builtin",
            configuration_json={},
            input_schema_json=schema,
        )
        assert res.is_valid is False
        assert any(e.code == ERR_REMOTE_REFERENCE_PROHIBITED for e in res.errors)

    def test_nested_remote_ref_in_combinators_rejected(self):
        """Remote reference deeply nested inside anyOf and properties is caught."""
        schema = {
            "type": "object",
            "properties": {
                "config": {
                    "anyOf": [
                        {"type": "string"},
                        {"$ref": "https://attacker.org/remote.json"},
                    ]
                }
            },
        }
        res = validate_definition_version(
            task_type="noop",
            execution_type="builtin",
            configuration_json={},
            input_schema_json=schema,
        )
        assert res.is_valid is False
        assert any(e.code == ERR_REMOTE_REFERENCE_PROHIBITED for e in res.errors)

    def test_remote_dynamic_ref_rejected(self):
        """Remote URI in $dynamicRef is strictly rejected."""
        schema = {"$dynamicRef": "https://evil.com/dynamic"}
        res = validate_definition_version(
            task_type="noop",
            execution_type="builtin",
            configuration_json={},
            input_schema_json=schema,
        )
        assert res.is_valid is False
        assert any(e.code == ERR_REMOTE_REFERENCE_PROHIBITED for e in res.errors)

    def test_restricted_resolver_prevents_network_fallback(self):
        """The restricted resolver strictly prevents network access and raises Unretrievable/Unresolvable."""
        from referencing.exceptions import Unretrievable

        from taskforge.definitions.validation import get_restricted_registry

        # Directly verifying registry retrieval raises Unretrievable wrapping the prohibition
        with pytest.raises(Unretrievable) as exc_info:
            get_restricted_registry().get_or_retrieve("https://example.com/unreachable.json")
        assert "Remote schema resolution prohibited" in str(exc_info.value.__cause__)

        # Verifying validator using restricted registry blocks remote resolution
        with pytest.raises(Exception) as val_exc:
            validator = compile_schema({"$ref": "https://example.com/unreachable.json"})
            validator.validate({})
        assert "Unresolvable" in type(val_exc.value).__name__ or "Unresolvable" in str(
            val_exc.value
        )


# ===========================================================================
# Category 12: Exception Sanitization (G11.3-R3)
# ===========================================================================
class TestExceptionSanitization:
    """Verifies that internal validator exceptions, stack traces, and credentials
    are sanitized and never exposed in persisted reports.
    """

    def test_no_raw_exception_string_in_report(self):
        """Unexpected internal exceptions produce sanitized diagnostic message."""
        with patch(
            "taskforge.definitions.validation.Draft202012Validator.check_schema",
            side_effect=RuntimeError("injected_secret_token_deadbeef"),
        ):
            res = validate_definition_version(
                task_type="noop",
                execution_type="builtin",
                configuration_json={},
                input_schema_json={"type": "object"},
            )
            assert res.is_valid is False
            err = next(e for e in res.errors if e.code == ERR_INTERNAL_VALIDATOR_ERROR)
            assert "injected_secret_token_deadbeef" not in err.message
            assert "RuntimeError" not in err.message
            assert "Traceback" not in err.message
            assert err.message == (
                "An internal validator error occurred during schema validation. "
                "Diagnostic details have been securely logged."
            )

    def test_no_credentials_leaked_in_internal_error(self):
        """Exception containing database connection string with password is fully sanitized."""
        with patch(
            "taskforge.definitions.validation.Draft202012Validator.check_schema",
            side_effect=RuntimeError(
                "Connection failed to postgresql://user:super_secret_pw@db:5432/tf"
            ),
        ):
            res = validate_definition_version(
                task_type="noop",
                execution_type="builtin",
                configuration_json={},
                input_schema_json={"type": "object"},
            )
            report_str = str(res.to_dict())
            assert "super_secret_pw" not in report_str
            assert "postgresql://" not in report_str

    def test_internal_exception_logged_to_protected_log(self):
        """Detailed exception with exc_info=True is logged to secure application logs."""
        with patch("taskforge.definitions.validation.logger.error") as mock_log:
            with patch(
                "taskforge.definitions.validation.Draft202012Validator.check_schema",
                side_effect=ValueError("hidden_internal_state"),
            ):
                validate_definition_version(
                    task_type="noop",
                    execution_type="builtin",
                    configuration_json={},
                    input_schema_json={"type": "object"},
                )
                mock_log.assert_called_once()
                call_kwargs = mock_log.call_args[1]
                assert call_kwargs.get("exc_info") is True


# ===========================================================================
# Category 13: Live TaskRegistry Compatibility (G11.3-R4)
# ===========================================================================
class TestLiveRegistryCompatibility:
    """Verifies that task compatibility validation checks against the live TaskRegistry
    and enforces callable execution contracts.
    """

    def test_all_17_live_registered_handlers_pass_compatibility(self):
        """Every single one of the 17 live registered handlers validates successfully."""
        from taskforge.tasks.registry import list_registered_handlers

        handlers = list_registered_handlers()
        assert len(handlers) == 17

        for handler_name in handlers:
            res = validate_definition_version(
                task_type=handler_name,
                execution_type="builtin",
                configuration_json={},
                input_schema_json={"type": "object"},
            )
            assert res.is_valid is True, f"Handler '{handler_name}' failed validation: {res.errors}"
            assert res.summary["execution_binding"] == "PASSED"

    def test_dynamically_registered_handler_passes_immediately(self):
        """A new handler dynamically registered in TaskRegistry is immediately recognized."""
        from taskforge.tasks.registry import _HANDLERS, register_handler

        custom_name = "dynamic_reconciliation_handler"
        try:

            @register_handler(custom_name)
            def custom_fn(payload):
                return {"result": "ok"}

            res = validate_definition_version(
                task_type=custom_name,
                execution_type="builtin",
                configuration_json={},
                input_schema_json={"type": "object"},
            )
            assert res.is_valid is True
            assert res.summary["execution_binding"] == "PASSED"
        finally:
            _HANDLERS.pop(custom_name, None)

    def test_uncallable_handler_object_rejected(self):
        """A handler entry in the registry that is not callable violates the execution contract."""
        from taskforge.tasks.registry import _HANDLERS

        broken_name = "not_a_callable_task"
        try:
            _HANDLERS[broken_name] = "string_is_not_callable"  # type: ignore

            res = validate_definition_version(
                task_type=broken_name,
                execution_type="builtin",
                configuration_json={},
                input_schema_json={"type": "object"},
            )
            assert res.is_valid is False
            assert any(e.code == ERR_UNSUPPORTED_EXECUTION_TYPE for e in res.errors)
        finally:
            _HANDLERS.pop(broken_name, None)
