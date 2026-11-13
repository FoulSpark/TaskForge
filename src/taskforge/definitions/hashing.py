"""Canonical JSON normalization and SHA-256 content hashing for definition versions."""

import hashlib
import json
import math
from typing import Any


def _canonicalize_value(value: Any) -> Any:
    """Recursively normalize JSON-compatible values for deterministic canonical hashing."""
    if value is None or isinstance(value, (str, bool)):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("Non-finite float values (NaN/Infinity) are prohibited.")
        if value == 0.0:
            return 0
        if value.is_integer():
            return int(value)
        return value
    if isinstance(value, list):
        return [_canonicalize_value(item) for item in value]
    if isinstance(value, dict):
        normalized: dict[str, Any] = {}
        for key in sorted(value.keys(), key=str):
            if not isinstance(key, str):
                raise ValueError("JSON object keys must be strings.")
            normalized[key] = _canonicalize_value(value[key])
        return normalized
    raise ValueError(f"Unsupported non-JSON type in definition content: {type(value).__name__}")


def compute_definition_content_hash(
    *,
    execution_type: str,
    task_type: str,
    configuration_json: dict[str, Any],
    input_schema_json: dict[str, Any],
    output_schema_json: dict[str, Any] | None = None,
    execution_policy_json: dict[str, Any] | None = None,
) -> str:
    """Compute canonical SHA-256 hex digest over immutable executable definition fields."""
    canonical_envelope = _canonicalize_value(
        {
            "execution_type": execution_type,
            "task_type": task_type,
            "configuration_json": configuration_json,
            "input_schema_json": input_schema_json,
            "output_schema_json": output_schema_json,
            "execution_policy_json": execution_policy_json or {},
        }
    )
    serialized = json.dumps(
        canonical_envelope,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()
