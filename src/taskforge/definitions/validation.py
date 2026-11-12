"""JSON Schema Draft 2020-12 validation engine for TaskForge definition versions.

Pure functional validation decoupled from database persistence:
- Validates input and output schemas against JSON Schema Draft 2020-12.
- Local-only $ref resolution; strictly rejects remote URIs and external protocols.
- Resource bounded: schema byte size, nesting depth, reference traversal depth.
- Detects circular reference loops and unbounded traversals.
- Validates TaskRegistry handler compatibility for builtin execution types.
- Deterministic error reporting with standard codes, JSON paths, and error limits.
"""

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlparse

from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError

import taskforge.tasks.handlers  # noqa: F401 - ensure built-in handlers are registered
from taskforge.tasks.registry import list_registered_handlers

# Resource and complexity limits
MAX_SCHEMA_SIZE_BYTES: int = 65_536  # 64 KB
MAX_SCHEMA_DEPTH: int = 15
MAX_REFERENCE_DEPTH: int = 10
MAX_VALIDATION_ERRORS: int = 20

# Supported Draft 2020-12 specification identifiers
SUPPORTED_SCHEMA_DRAFTS: frozenset[str] = frozenset(
    {
        "https://json-schema.org/draft/2020-12/schema",
        "https://json-schema.org/draft/2020-12/schema#",
        "http://json-schema.org/draft/2020-12/schema",
        "http://json-schema.org/draft/2020-12/schema#",
    }
)

# Standardized error codes for deterministic reporting
ERR_INVALID_SCHEMA_TYPE = "INVALID_SCHEMA_TYPE"
ERR_INVALID_SCHEMA_SYNTAX = "INVALID_SCHEMA_SYNTAX"
ERR_UNSUPPORTED_SCHEMA_DRAFT = "UNSUPPORTED_SCHEMA_DRAFT"
ERR_REMOTE_REFERENCE_PROHIBITED = "REMOTE_REFERENCE_PROHIBITED"
ERR_UNRESOLVED_REFERENCE = "UNRESOLVED_REFERENCE"
ERR_CIRCULAR_REFERENCE_DETECTED = "CIRCULAR_REFERENCE_DETECTED"
ERR_REFERENCE_DEPTH_EXCEEDED = "REFERENCE_DEPTH_EXCEEDED"
ERR_SCHEMA_SIZE_EXCEEDED = "SCHEMA_SIZE_EXCEEDED"
ERR_SCHEMA_DEPTH_EXCEEDED = "SCHEMA_DEPTH_EXCEEDED"
ERR_UNKNOWN_TASK_TYPE = "UNKNOWN_TASK_TYPE"
ERR_UNSUPPORTED_EXECUTION_TYPE = "UNSUPPORTED_EXECUTION_TYPE"
ERR_PROHIBITED_SECRET_DETECTED = "PROHIBITED_SECRET_DETECTED"
ERR_INTERNAL_VALIDATOR_ERROR = "INTERNAL_VALIDATOR_ERROR"


@dataclass(frozen=True)
class ValidationErrorDetail:
    """Structured detail for a single validation diagnostic."""

    code: str
    message: str
    path: str
    target: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class DefinitionValidationResult:
    """Immutable validation outcome and report for a definition version."""

    is_valid: bool
    status: str  # "VALID" or "INVALID"
    errors: list[ValidationErrorDetail]
    task_type: str
    execution_type: str
    validated_at: str
    summary: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "is_valid": self.is_valid,
            "task_type": self.task_type,
            "execution_type": self.execution_type,
            "validated_at": self.validated_at,
            "error_count": len(self.errors),
            "errors": [e.to_dict() for e in self.errors],
            "summary": self.summary,
        }


def _measure_nesting_depth(value: Any, current_depth: int = 0) -> int:
    """Compute maximum structural nesting depth of a JSON-compatible tree."""
    if isinstance(value, dict):
        if not value:
            return current_depth + 1
        return max(
            _measure_nesting_depth(sub_val, current_depth + 1)
            for sub_val in value.values()
        )
    if isinstance(value, list):
        if not value:
            return current_depth + 1
        return max(
            _measure_nesting_depth(sub_item, current_depth + 1)
            for sub_item in value
        )
    return current_depth


def _resolve_json_pointer(root: dict[str, Any], pointer: str) -> Any:
    """Resolve local JSON Pointer string (e.g., '#/$defs/person' or '#') in root object."""
    if pointer in ("#", "#/", ""):
        return root
    if not pointer.startswith("#/"):
        raise ValueError(f"Invalid local pointer '{pointer}'; must start with '#/'")

    parts = pointer[2:].split("/")
    curr: Any = root
    for raw_part in parts:
        # RFC 6901 unescaping: ~1 -> /, ~0 -> ~
        part = raw_part.replace("~1", "/").replace("~0", "~")
        if isinstance(curr, dict):
            if part not in curr:
                raise KeyError(f"Pointer token '{part}' not found")
            curr = curr[part]
        elif isinstance(curr, list):
            try:
                idx = int(part)
            except ValueError:
                raise KeyError(f"Invalid array index '{part}' in pointer") from None
            if idx < 0 or idx >= len(curr):
                raise IndexError(f"Array index {idx} out of range in pointer")
            curr = curr[idx]
        else:
            raise KeyError(f"Cannot traverse into primitive {type(curr).__name__} with '{part}'")
    return curr


class _SchemaInspector:
    """Validates structural constraints, remote references, circular loops, and Draft 2020-12 rules."""

    def __init__(self, target_name: str, errors: list[ValidationErrorDetail]):
        self.target_name = target_name
        self.errors = errors

    def _add_error(self, code: str, message: str, path: str) -> None:
        if len(self.errors) < MAX_VALIDATION_ERRORS:
            self.errors.append(
                ValidationErrorDetail(
                    code=code,
                    message=message,
                    path=path,
                    target=self.target_name,
                )
            )

    def validate_schema(self, schema: Any) -> None:
        if not isinstance(schema, (dict, bool)):
            self._add_error(
                ERR_INVALID_SCHEMA_TYPE,
                f"Schema must be a JSON object or boolean, got {type(schema).__name__}.",
                "$",
            )
            return

        # Boolean schemas (true / false) are valid Draft 2020-12 schemas
        if isinstance(schema, bool):
            return

        # 1. Byte size limit
        try:
            serialized = json.dumps(schema, separators=(",", ":"))
            encoded_len = len(serialized.encode("utf-8"))
            if encoded_len > MAX_SCHEMA_SIZE_BYTES:
                self._add_error(
                    ERR_SCHEMA_SIZE_EXCEEDED,
                    f"Schema size of {encoded_len} bytes exceeds limit of {MAX_SCHEMA_SIZE_BYTES} bytes.",
                    "$",
                )
                return
        except (TypeError, ValueError) as exc:
            self._add_error(ERR_INVALID_SCHEMA_SYNTAX, f"Failed to serialize schema: {exc}", "$")
            return

        # 2. Secret scanning in schema
        if "tf_live_" in serialized:
            self._add_error(
                ERR_PROHIBITED_SECRET_DETECTED,
                "Prohibited TaskForge API key secret detected in schema.",
                "$",
            )

        # 3. Nesting depth limit
        depth = _measure_nesting_depth(schema)
        if depth > MAX_SCHEMA_DEPTH:
            self._add_error(
                ERR_SCHEMA_DEPTH_EXCEEDED,
                f"Schema nesting depth {depth} exceeds limit of {MAX_SCHEMA_DEPTH}.",
                "$",
            )

        # 4. Draft check if $schema is specified
        if "$schema" in schema:
            draft_uri = schema["$schema"]
            if not isinstance(draft_uri, str) or draft_uri not in SUPPORTED_SCHEMA_DRAFTS:
                self._add_error(
                    ERR_UNSUPPORTED_SCHEMA_DRAFT,
                    f"Unsupported $schema '{draft_uri}'. TaskForge strictly requires Draft 2020-12 "
                    "(https://json-schema.org/draft/2020-12/schema).",
                    "$.$schema",
                )

        # 5. Syntax validation via jsonschema.Draft202012Validator
        try:
            Draft202012Validator.check_schema(schema)
        except SchemaError as exc:
            path_str = "$." + ".".join(str(p) for p in exc.path) if exc.path else "$"
            self._add_error(
                ERR_INVALID_SCHEMA_SYNTAX,
                f"Draft 2020-12 syntax error: {exc.message}",
                path_str,
            )

        # 6. Reference analysis (remote rejection, traversal resolution, circular loop detection)
        self._inspect_references(schema, schema, "$")

    def _inspect_references(
        self,
        current: Any,
        root_schema: dict[str, Any],
        path: str,
    ) -> None:
        """Walk schema AST to verify local-only $ref resolution and detect cycles."""
        if isinstance(current, dict):
            if "$ref" in current:
                ref_val = current["$ref"]
                ref_path = f"{path}.$ref"
                self._validate_single_ref(ref_val, root_schema, ref_path)

            for key, val in current.items():
                if key != "$ref":
                    self._inspect_references(val, root_schema, f"{path}.{key}")
        elif isinstance(current, list):
            for idx, item in enumerate(current):
                self._inspect_references(item, root_schema, f"{path}[{idx}]")

    def _validate_single_ref(
        self,
        ref_val: Any,
        root_schema: dict[str, Any],
        path: str,
    ) -> None:
        if not isinstance(ref_val, str):
            self._add_error(
                ERR_INVALID_SCHEMA_SYNTAX,
                f"$ref value must be a string, got {type(ref_val).__name__}.",
                path,
            )
            return

        # Remote references: contain protocol schemes or do not start with '#'
        parsed = urlparse(ref_val)
        if parsed.scheme in ("http", "https", "file", "ftp", "urn", "custom") or not ref_val.startswith("#"):
            self._add_error(
                ERR_REMOTE_REFERENCE_PROHIBITED,
                f"Remote reference '{ref_val}' is prohibited. Only local references starting with '#' are allowed.",
                path,
            )
            return

        # Traverse local reference chain to verify resolution and detect cycles
        visited_refs: list[str] = [ref_val]
        curr_ref = ref_val

        for hop in range(MAX_REFERENCE_DEPTH + 1):
            if hop >= MAX_REFERENCE_DEPTH:
                self._add_error(
                    ERR_REFERENCE_DEPTH_EXCEEDED,
                    f"Reference traversal from '{ref_val}' exceeded maximum depth of {MAX_REFERENCE_DEPTH} hops.",
                    path,
                )
                return

            try:
                target_node = _resolve_json_pointer(root_schema, curr_ref)
            except (KeyError, IndexError, ValueError):
                self._add_error(
                    ERR_UNRESOLVED_REFERENCE,
                    f"Local reference '{curr_ref}' could not be resolved within the schema document.",
                    path,
                )
                return

            # If target node is another $ref, continue traversing the alias chain
            if isinstance(target_node, dict) and "$ref" in target_node:
                next_ref = target_node["$ref"]
                if not isinstance(next_ref, str) or not next_ref.startswith("#"):
                    self._add_error(
                        ERR_REMOTE_REFERENCE_PROHIBITED,
                        f"Chained remote reference '{next_ref}' is prohibited.",
                        path,
                    )
                    return
                if next_ref in visited_refs:
                    cycle = " -> ".join(visited_refs + [next_ref])
                    self._add_error(
                        ERR_CIRCULAR_REFERENCE_DETECTED,
                        f"Circular reference detected: {cycle}.",
                        path,
                    )
                    return
                visited_refs.append(next_ref)
                curr_ref = next_ref
            else:
                # Target resolved to a concrete schema definition
                break


def validate_definition_version(
    *,
    task_type: str,
    execution_type: str,
    configuration_json: dict[str, Any],
    input_schema_json: dict[str, Any],
    output_schema_json: dict[str, Any] | None = None,
) -> DefinitionValidationResult:
    """Pure functional validator for TaskForge definition versions.

    Guarantees:
    - Never raises unhandled exceptions (catches internal faults and marks INVALID).
    - Validates execution binding against live TaskRegistry.
    - Validates input and output schemas independently.
    - Enforces local-only $ref resolution, resource bounds, and cycle safety.
    - Returns structured deterministic validation report.
    """
    now_iso = datetime.now(UTC).isoformat()
    errors: list[ValidationErrorDetail] = []
    checks: dict[str, str] = {
        "execution_binding": "PASSED",
        "input_schema": "PASSED",
        "output_schema": "SKIPPED",
    }

    try:
        # 1. Execution binding checks
        if execution_type != "builtin":
            errors.append(
                ValidationErrorDetail(
                    code=ERR_UNSUPPORTED_EXECUTION_TYPE,
                    message=(
                        f"Unsupported execution_type '{execution_type}'. "
                        "Only 'builtin' is executable in Phase 11."
                    ),
                    path="$.execution_type",
                    target="execution_binding",
                )
            )
            checks["execution_binding"] = "FAILED"

        registered_handlers = list_registered_handlers()
        if task_type not in registered_handlers:
            errors.append(
                ValidationErrorDetail(
                    code=ERR_UNKNOWN_TASK_TYPE,
                    message=(
                        f"Task type '{task_type}' is not registered in TaskRegistry. "
                        f"Available: {sorted(registered_handlers)}."
                    ),
                    path="$.task_type",
                    target="execution_binding",
                )
            )
            checks["execution_binding"] = "FAILED"

        # 2. Input schema validation
        input_inspector = _SchemaInspector(target_name="input_schema", errors=errors)
        input_inspector.validate_schema(input_schema_json)
        if any(e.target == "input_schema" for e in errors):
            checks["input_schema"] = "FAILED"

        # 3. Output schema validation (optional)
        if output_schema_json is not None:
            checks["output_schema"] = "PASSED"
            output_inspector = _SchemaInspector(target_name="output_schema", errors=errors)
            output_inspector.validate_schema(output_schema_json)
            if any(e.target == "output_schema" for e in errors):
                checks["output_schema"] = "FAILED"

    except Exception as exc:  # Safety boundary: unexpected exceptions must NEVER produce VALID
        errors.append(
            ValidationErrorDetail(
                code=ERR_INTERNAL_VALIDATOR_ERROR,
                message=f"Internal validator exception: {exc}",
                path="$",
                target="system",
            )
        )
        checks["system"] = "FAILED"

    # Deterministic error sorting: target, path, code, message
    errors.sort(key=lambda e: (e.target, e.path, e.code, e.message))
    is_valid = len(errors) == 0
    status = "VALID" if is_valid else "INVALID"

    return DefinitionValidationResult(
        is_valid=is_valid,
        status=status,
        errors=errors,
        task_type=task_type,
        execution_type=execution_type,
        validated_at=now_iso,
        summary=checks,
    )
