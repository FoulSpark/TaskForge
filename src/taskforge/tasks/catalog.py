"""Runtime task handler catalog metadata for Phase 10 Web Console."""

from typing import Any

import taskforge.tasks  # noqa: F401
from taskforge.tasks.registry import list_registered_handlers

SUBMISSION_CONSTRAINTS: dict[str, Any] = {
    "priority": {"min": 1, "max": 4, "default": 2},
    "timeout_seconds": {"min": 1, "max": 3600, "default": 60},
    "max_retries": {"min": 0, "max": 10, "default": 3},
    "task_name": {"min_length": 1, "max_length": 128},
    "idempotency_key": {"max_length": 128},
}

_CATALOG_DEFINITIONS: list[dict[str, Any]] = [
    {
        "task_type": "json_transform",
        "display_name": "JSON Transform",
        "category": "workload",
        "description": "Transforms, counts, and computes summary statistics over numeric records.",
        "default_payload": {"records": [10, 20, 30, 40]},
        "payload_schema": {
            "type": "object",
            "properties": {
                "records": {
                    "type": "array",
                    "description": "List of values (numeric entries are summed and averaged).",
                }
            },
        },
    },
    {
        "task_type": "compute_prime",
        "display_name": "Compute Primes (Sieve)",
        "category": "workload",
        "description": "CPU-bound Sieve of Eratosthenes computing primes up to n (capped at 100,000).",
        "default_payload": {"n": 1000},
        "payload_schema": {
            "type": "object",
            "properties": {
                "n": {"type": "integer", "minimum": 2, "maximum": 100000, "default": 1000}
            },
        },
    },
    {
        "task_type": "io_simulation",
        "display_name": "I/O Simulation",
        "category": "workload",
        "description": "Simulates external network or disk I/O with configurable delay and jitter.",
        "default_payload": {"delay_seconds": 0.1, "jitter": True, "size_bytes": 1024},
        "payload_schema": {
            "type": "object",
            "properties": {
                "delay_seconds": {"type": "number", "minimum": 0, "maximum": 10, "default": 0.1},
                "jitter": {"type": "boolean", "default": True},
                "size_bytes": {"type": "integer", "minimum": 0, "default": 1024},
            },
        },
    },
    {
        "task_type": "db_process",
        "display_name": "Batch DB Processing",
        "category": "workload",
        "description": "Simulates batch row processing and checksum calculation.",
        "default_payload": {"batch_size": 50},
        "payload_schema": {
            "type": "object",
            "properties": {"batch_size": {"type": "integer", "minimum": 1, "default": 50}},
        },
    },
    {
        "task_type": "long_running",
        "display_name": "Multi-Step Pipeline",
        "category": "workload",
        "description": "Executes sequential pipeline steps (capped at 10 steps).",
        "default_payload": {"steps": 3, "step_duration": 0.05},
        "payload_schema": {
            "type": "object",
            "properties": {
                "steps": {"type": "integer", "minimum": 1, "maximum": 10, "default": 3},
                "step_duration": {"type": "number", "minimum": 0, "default": 0.05},
            },
        },
    },
    {
        "task_type": "image_resize",
        "display_name": "Image Resize",
        "category": "workload",
        "description": "Simulated image resizing workload returning target dimensions.",
        "default_payload": {"width": 800, "height": 600},
        "payload_schema": {
            "type": "object",
            "properties": {
                "width": {"type": "integer", "minimum": 1, "default": 800},
                "height": {"type": "integer", "minimum": 1, "default": 600},
            },
        },
    },
    {
        "task_type": "payment_reconcile",
        "display_name": "Payment Reconciliation",
        "category": "workload",
        "description": "Simulated transaction reconciliation batch summary.",
        "default_payload": {"tx_count": 25},
        "payload_schema": {
            "type": "object",
            "properties": {"tx_count": {"type": "integer", "minimum": 1, "default": 1}},
        },
    },
    {
        "task_type": "report_generate",
        "display_name": "Report Generation",
        "category": "workload",
        "description": "Simulated asynchronous report builder.",
        "default_payload": {"report_type": "summary"},
        "payload_schema": {
            "type": "object",
            "properties": {"report_type": {"type": "string", "default": "summary"}},
        },
    },
    {
        "task_type": "webhook_send",
        "display_name": "Webhook Dispatch",
        "category": "workload",
        "description": "Simulated outbound webhook delivery confirmation.",
        "default_payload": {"url": "https://example.com/webhook"},
        "payload_schema": {
            "type": "object",
            "properties": {"url": {"type": "string", "default": "https://example.com/webhook"}},
        },
    },
    {
        "task_type": "csv_export",
        "display_name": "CSV Export",
        "category": "workload",
        "description": "Simulated tabular CSV export job.",
        "default_payload": {"rows": 100},
        "payload_schema": {
            "type": "object",
            "properties": {"rows": {"type": "integer", "minimum": 1, "default": 100}},
        },
    },
    {
        "task_type": "noop",
        "display_name": "No-op Baseline",
        "category": "benchmark",
        "description": "Zero-overhead deterministic handler for pipeline baseline measurement.",
        "default_payload": {"benchmark": True},
        "payload_schema": {"type": "object", "additionalProperties": True},
    },
    {
        "task_type": "cpu_bound",
        "display_name": "CPU-Bound Prime Sieve",
        "category": "benchmark",
        "description": "Deterministic CPU prime sieve up to n used in canonical benchmark W-02.",
        "default_payload": {"n": 500},
        "payload_schema": {
            "type": "object",
            "properties": {"n": {"type": "integer", "minimum": 2, "default": 10000}},
        },
    },
    {
        "task_type": "io_bound",
        "display_name": "I/O-Bound Delay",
        "category": "benchmark",
        "description": "Deterministic I/O sleep handler used in canonical benchmark W-03.",
        "default_payload": {"delay_seconds": 0.05, "bytes_read": 1024},
        "payload_schema": {
            "type": "object",
            "properties": {
                "delay_seconds": {"type": "number", "minimum": 0, "default": 0.05},
                "bytes_read": {"type": "integer", "minimum": 0, "default": 1024},
            },
        },
    },
    {
        "task_type": "failure_simulation",
        "display_name": "Transient Failure Simulation",
        "category": "fault_injection",
        "description": "Fails until _current_attempt >= fail_until_attempt to exercise retry & backoff.",
        "default_payload": {"fail_until_attempt": 2, "error_message": "Simulated transient outage"},
        "payload_schema": {
            "type": "object",
            "properties": {
                "fail_until_attempt": {"type": "integer", "minimum": 1, "default": 999},
                "error_message": {"type": "string"},
            },
        },
    },
    {
        "task_type": "timeout_simulation",
        "display_name": "Timeout Enforcement Simulation",
        "category": "fault_injection",
        "description": "Sleeps longer than timeout_seconds to verify deadline enforcement.",
        "default_payload": {"sleep_seconds": 3.0},
        "payload_schema": {
            "type": "object",
            "properties": {"sleep_seconds": {"type": "number", "minimum": 0.1, "default": 10.0}},
        },
    },
    {
        "task_type": "failing",
        "display_name": "Deterministic Failure",
        "category": "fault_injection",
        "description": "Always raises RuntimeError to verify retry exhaustion and DEAD_LETTER routing.",
        "default_payload": {"error_message": "Deliberate deterministic handler failure"},
        "payload_schema": {
            "type": "object",
            "properties": {"error_message": {"type": "string"}},
        },
    },
    {
        "task_type": "sleep",
        "display_name": "Deterministic Sleep",
        "category": "fault_injection",
        "description": "Sleeps for duration seconds for lease-renewal and timeout drills.",
        "default_payload": {"duration": 1.0},
        "payload_schema": {
            "type": "object",
            "properties": {"duration": {"type": "number", "minimum": 0, "default": 1.0}},
        },
    },
]


def build_task_types_catalog() -> dict[str, Any]:
    """Assemble live task-type catalog verified against TaskRegistry."""
    registered = set(list_registered_handlers())
    known_types = {item["task_type"] for item in _CATALOG_DEFINITIONS}

    items = [
        {**item, "available": item["task_type"] in registered}
        for item in _CATALOG_DEFINITIONS
    ]

    for extra_name in sorted(registered - known_types):
        items.append(
            {
                "task_type": extra_name,
                "display_name": extra_name.replace("_", " ").title(),
                "category": "workload",
                "description": "Dynamically registered task handler.",
                "available": True,
                "default_payload": {},
                "payload_schema": {"type": "object", "additionalProperties": True},
            }
        )

    return {
        "items": items,
        "submission_constraints": SUBMISSION_CONSTRAINTS,
    }
