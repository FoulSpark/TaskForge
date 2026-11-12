"""JSON Schema Draft 2020-12 validation engine for TaskForge definition versions.

Pure functional validation decoupled from database persistence:
- Validates input and output schemas against JSON Schema Draft 2020-12.
- Local-only reference resolution with explicitly restricted registry (strictly rejects remote URIs,
  URI-encoded external schemes, protocol smuggling, and network fallback).
- Resource bounded: schema byte size, nesting depth, reference traversal depth.
- Distinguishes valid recursive schemas (e.g. self-referencing tree nodes) from malformed reference cycles.
- Validates TaskRegistry handler compatibility for builtin execution types against the live registry.
- Deterministic error reporting with sanitized diagnostics and capped error collections.
"""

import json
import re
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any
from urllib.parse import unquote, urlparse

from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError
from referencing import Registry, Resource
from referencing.exceptions import Unresolvable

import taskforge.tasks.handlers  # noqa: F401 - ensure built-in handlers are registered
from taskforge.core.logging import get_logger
from taskforge.tasks.registry import get_handler, list_registered_handlers

logger = get_logger(__name__)

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

# Forbidden scheme patterns for remote/external reference detection
FORBIDDEN_SCHEME_PATTERN = re.compile(
    r"^(https?|file|ftp|ftps|gopher|ws|wss|tftp|ldap|data|blob|javascript|about|urn|custom|jar|view-source):",
    re.IGNORECASE,
)

# Structural keywords that indicate a concrete schema definition rather than a pure reference alias
STRUCTURAL_SCHEMA_KEYWORDS: frozenset[str] = frozenset(
    {
        "type",
        "properties",
        "patternProperties",
        "additionalProperties",
        "items",
        "prefixItems",
        "contains",
        "allOf",
        "anyOf",
        "oneOf",
        "not",
        "if",
        "then",
        "else",
        "enum",
        "const",
        "minimum",
        "maximum",
        "exclusiveMinimum",
        "exclusiveMaximum",
        "multipleOf",
        "minLength",
        "maxLength",
        "pattern",
        "minItems",
        "maxItems",
        "uniqueItems",
        "minProperties",
        "maxProperties",
        "required",
        "dependentRequired",
        "dependentSchemas",
    }
)


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


def _sanitize_message(message: str) -> str:
    """Sanitize diagnostic messages to redact secrets and credentials."""
    if not message:
        return message
    # Redact TaskForge API key tokens
    message = re.sub(r"tf_live_[a-zA-Z0-9_]+", "[REDACTED_API_KEY]", message)
    # Redact credentials embedded in URIs (e.g., user:pass@host)
    message = re.sub(r"://[^@\s]+@", "://[REDACTED_CREDENTIALS]@", message)
    return message


def _restricted_retrieve(uri: str) -> Resource:
    """Strictly forbid any remote or external schema network resolution."""
    raise Unresolvable(
        f"Remote schema resolution prohibited for URI: '{uri}'. "
        "TaskForge definition schemas require self-contained local references."
    )


def get_restricted_registry() -> Registry:
    """Create a referencing.Registry that forbids remote network retrieval."""
    return Registry(retrieve=_restricted_retrieve)


def compile_schema(schema: dict[str, Any] | bool) -> Draft202012Validator:
    """Compile a Draft 2020-12 schema using a strictly local resolver with no network-capable fallback.

    This separates schema compilation from instance validation and task execution.
    """
    return Draft202012Validator(schema, registry=get_restricted_registry())


def validate_schema_instance(schema: dict[str, Any] | bool, instance: Any) -> list[str]:
    """Validate an instance payload against a compiled schema contract.

    Does NOT execute task handlers. Returns list of error message strings (empty if valid).
    """
    validator = compile_schema(schema)
    errors: list[str] = []
    for err in validator.iter_errors(instance):
        path = "$." + ".".join(str(p) for p in err.path) if err.path else "$"
        errors.append(_sanitize_message(f"At {path}: {err.message}"))
    return sorted(errors)


def _measure_nesting_depth(value: Any, current_depth: int = 0) -> int:
    """Compute maximum structural nesting depth of a JSON-compatible tree."""
    if isinstance(value, dict):
        if not value:
            return current_depth + 1
        return max(_measure_nesting_depth(sub_val, current_depth + 1) for sub_val in value.values())
    if isinstance(value, list):
        if not value:
            return current_depth + 1
        return max(_measure_nesting_depth(sub_item, current_depth + 1) for sub_item in value)
    return current_depth


def _recursively_unquote(val: str, max_rounds: int = 5) -> str:
    """Recursively decode URI-encoded characters to prevent multi-level encoding bypass."""
    curr = val
    for _ in range(max_rounds):
        nxt = unquote(curr)
        if nxt == curr:
            break
        curr = nxt
    return curr


def _check_remote_or_external_reference(ref_val: str) -> tuple[bool, str]:
    """Inspect a reference string for remote protocols, encoded schemes, protocol-relative URIs, or non-local targets.

    Returns (is_remote, reason).
    """
    if not isinstance(ref_val, str):
        return True, f"Reference must be a string, got {type(ref_val).__name__}."

    stripped = ref_val.strip()
    if stripped != ref_val:
        return True, "Reference contains leading or trailing whitespace."

    if not stripped:
        return True, "Reference cannot be empty."

    unquoted = _recursively_unquote(stripped)

    # Check protocol-relative: //evil.com
    if stripped.startswith("//") or unquoted.startswith("//"):
        return True, f"Protocol-relative remote reference '{ref_val}' is prohibited."

    # Check scheme patterns in stripped and unquoted forms
    if FORBIDDEN_SCHEME_PATTERN.search(stripped) or FORBIDDEN_SCHEME_PATTERN.search(unquoted):
        return True, f"External scheme reference '{ref_val}' is prohibited."

    # Parse scheme via urlparse
    parsed_raw = urlparse(stripped)
    parsed_unquoted = urlparse(unquoted)
    if parsed_raw.scheme or parsed_unquoted.scheme:
        return True, f"Scheme-qualified reference '{ref_val}' is prohibited."

    # Must start with '#'
    if not stripped.startswith("#"):
        return (
            True,
            f"Remote reference '{ref_val}' is prohibited. Only local references starting with '#' are allowed.",
        )

    # If it starts with '#', inspect the fragment itself for smuggled schemes (e.g. #http://evil.com or #//evil.com)
    fragment = stripped[1:]
    fragment_unquoted = unquoted[1:] if unquoted.startswith("#") else unquoted
    if fragment.startswith("//") or fragment_unquoted.startswith("//"):
        return True, f"Smuggled protocol-relative URI in fragment '{ref_val}' is prohibited."
    if FORBIDDEN_SCHEME_PATTERN.search(fragment) or FORBIDDEN_SCHEME_PATTERN.search(
        fragment_unquoted
    ):
        return True, f"Smuggled external scheme in fragment '{ref_val}' is prohibited."

    return False, ""


def _check_remote_id(id_val: Any) -> tuple[bool, str]:
    """Inspect an $id identifier to ensure it does not change the base URI to an external network location."""
    if not isinstance(id_val, str):
        return True, f"$id must be a string, got {type(id_val).__name__}."

    stripped = id_val.strip()
    if not stripped:
        return True, "$id cannot be empty."

    unquoted = _recursively_unquote(stripped)

    if stripped.startswith("//") or unquoted.startswith("//"):
        return True, f"Protocol-relative $id '{id_val}' is prohibited."

    if FORBIDDEN_SCHEME_PATTERN.search(stripped) or FORBIDDEN_SCHEME_PATTERN.search(unquoted):
        return (
            True,
            f"Remote $id '{id_val}' is prohibited. Schema identifiers must not change the base URI to an external resource.",
        )

    parsed_raw = urlparse(stripped)
    parsed_unquoted = urlparse(unquoted)
    if parsed_raw.scheme in ("http", "https", "file", "ftp") or parsed_unquoted.scheme in (
        "http",
        "https",
        "file",
        "ftp",
    ):
        return (
            True,
            f"Remote $id '{id_val}' is prohibited. Schema identifiers must not change the base URI to an external resource.",
        )

    return False, ""


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


def _collect_anchors(schema: Any, anchors: dict[str, Any] | None = None) -> dict[str, Any]:
    """Collect all $anchor and $dynamicAnchor definitions in the schema document."""
    if anchors is None:
        anchors = {}
    if isinstance(schema, dict):
        if "$anchor" in schema and isinstance(schema["$anchor"], str):
            anchors[schema["$anchor"]] = schema
        if "$dynamicAnchor" in schema and isinstance(schema["$dynamicAnchor"], str):
            anchors[schema["$dynamicAnchor"]] = schema
        for val in schema.values():
            if isinstance(val, (dict, list)):
                _collect_anchors(val, anchors)
    elif isinstance(schema, list):
        for item in schema:
            if isinstance(item, (dict, list)):
                _collect_anchors(item, anchors)
    return anchors


def _resolve_local_reference(
    root: dict[str, Any],
    ref: str,
    anchors: dict[str, Any],
) -> Any:
    """Resolve a local fragment reference (#, #/pointer, or #anchor) within the schema."""
    if ref in ("#", "#/", ""):
        return root
    if ref.startswith("#/"):
        return _resolve_json_pointer(root, ref)
    if ref.startswith("#"):
        anchor_name = ref[1:]
        if anchor_name in anchors:
            return anchors[anchor_name]
        raise KeyError(f"Local anchor '{anchor_name}' not found in schema")
    raise ValueError(f"Invalid local reference '{ref}'; must start with '#'")


class _SchemaInspector:
    """Validates structural constraints, remote references, circular alias loops, and Draft 2020-12 rules."""

    def __init__(self, target_name: str, errors: list[ValidationErrorDetail]):
        self.target_name = target_name
        self.errors = errors
        self.anchors: dict[str, Any] = {}

    def _add_error(self, code: str, message: str, path: str) -> None:
        if len(self.errors) < MAX_VALIDATION_ERRORS:
            self.errors.append(
                ValidationErrorDetail(
                    code=code,
                    message=_sanitize_message(message),
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
        except (TypeError, ValueError):
            self._add_error(
                ERR_INVALID_SCHEMA_SYNTAX,
                "Schema cannot be serialized to valid JSON.",
                "$",
            )
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

        # 6. Index local anchors and dynamic anchors
        self.anchors = _collect_anchors(schema)

        # 7. Walk AST: reference security, local resolution, cycle detection, and $id validation
        self._inspect_node(schema, schema, "$")

    def _inspect_node(
        self,
        current: Any,
        root_schema: dict[str, Any],
        path: str,
    ) -> None:
        """Walk schema AST to verify local-only resolution, reference security, and cycle safety."""
        if isinstance(current, dict):
            # Check $id to prevent changing base URI to remote origins
            if "$id" in current:
                is_remote_id, reason_id = _check_remote_id(current["$id"])
                if is_remote_id:
                    self._add_error(
                        ERR_REMOTE_REFERENCE_PROHIBITED,
                        reason_id,
                        f"{path}.$id",
                    )

            # Check $ref
            if "$ref" in current:
                ref_val = current["$ref"]
                self._validate_single_reference(ref_val, root_schema, f"{path}.$ref")

            # Check $dynamicRef
            if "$dynamicRef" in current:
                dyn_val = current["$dynamicRef"]
                self._validate_single_reference(dyn_val, root_schema, f"{path}.$dynamicRef")

            # Recurse into all keys
            for key, val in current.items():
                if key not in ("$ref", "$dynamicRef"):
                    self._inspect_node(val, root_schema, f"{path}.{key}")

        elif isinstance(current, list):
            for idx, item in enumerate(current):
                self._inspect_node(item, root_schema, f"{path}[{idx}]")

    def _validate_single_reference(
        self,
        ref_val: Any,
        root_schema: dict[str, Any],
        path: str,
    ) -> None:
        """Validate reference security, local resolution, and distinguish cycles from valid recursion."""
        if not isinstance(ref_val, str):
            self._add_error(
                ERR_INVALID_SCHEMA_SYNTAX,
                f"Reference value must be a string, got {type(ref_val).__name__}.",
                path,
            )
            return

        is_remote, reason = _check_remote_or_external_reference(ref_val)
        if is_remote:
            self._add_error(ERR_REMOTE_REFERENCE_PROHIBITED, reason, path)
            return

        # Traverse alias chain to verify local resolution and detect circular reference loops
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
                target_node = _resolve_local_reference(root_schema, curr_ref, self.anchors)
            except (KeyError, IndexError, ValueError):
                self._add_error(
                    ERR_UNRESOLVED_REFERENCE,
                    f"Local reference '{curr_ref}' could not be resolved within the schema document.",
                    path,
                )
                return

            # Check if target is a pure alias node (contains $ref or $dynamicRef without concrete structural keywords)
            if isinstance(target_node, dict):
                has_alias_ref = "$ref" in target_node or "$dynamicRef" in target_node
                has_structural_definition = any(
                    k in target_node for k in STRUCTURAL_SCHEMA_KEYWORDS
                )

                if has_alias_ref and not has_structural_definition:
                    next_ref = target_node.get("$ref") or target_node.get("$dynamicRef")
                    if not isinstance(next_ref, str):
                        self._add_error(
                            ERR_INVALID_SCHEMA_SYNTAX,
                            f"Chained reference must be a string, got {type(next_ref).__name__}.",
                            path,
                        )
                        return

                    is_next_remote, next_reason = _check_remote_or_external_reference(next_ref)
                    if is_next_remote:
                        self._add_error(ERR_REMOTE_REFERENCE_PROHIBITED, next_reason, path)
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
                    # Target is a concrete schema definition (contains structural constraints like properties, type, etc.)
                    # Valid recursive reference resolves successfully
                    break
            else:
                # Target resolved to a boolean schema (True / False)
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
    - Never exposes raw exception strings or stack traces in diagnostics.
    - Validates execution binding against live TaskRegistry and verifies callable contract.
    - Validates input and output schemas independently.
    - Enforces local-only reference resolution, resource bounds, and cycle safety.
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
        # 1. Execution binding checks against live registry
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
        else:
            try:
                handler = get_handler(task_type)
                if not callable(handler):
                    errors.append(
                        ValidationErrorDetail(
                            code=ERR_UNSUPPORTED_EXECUTION_TYPE,
                            message=f"Handler for task type '{task_type}' does not satisfy callable execution contract.",
                            path="$.task_type",
                            target="execution_binding",
                        )
                    )
                    checks["execution_binding"] = "FAILED"
            except Exception:
                logger.error("task_handler_lookup_failed", task_type=task_type, exc_info=True)
                errors.append(
                    ValidationErrorDetail(
                        code=ERR_UNKNOWN_TASK_TYPE,
                        message=f"Handler for task type '{task_type}' could not be loaded from TaskRegistry.",
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

    except Exception:  # Safety boundary: unexpected exceptions must NEVER produce VALID
        logger.error(
            "definition_validation_unexpected_exception",
            task_type=task_type,
            execution_type=execution_type,
            exc_info=True,
        )
        errors.append(
            ValidationErrorDetail(
                code=ERR_INTERNAL_VALIDATOR_ERROR,
                message=(
                    "An internal validator error occurred during schema validation. "
                    "Diagnostic details have been securely logged."
                ),
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
