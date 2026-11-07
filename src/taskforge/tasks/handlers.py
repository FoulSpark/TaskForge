import random
import time
from typing import Any, Dict

from taskforge.core.logging import get_logger
from taskforge.tasks.registry import register_handler

logger = get_logger(__name__)


@register_handler("json_transform")
def handle_json_transform(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Lightweight JSON transformation:
    Filters, aggregates, computes summaries on input records.
    """
    records = payload.get("records", [])
    if isinstance(records, list):
        count = len(records)
        nums = [r for r in records if isinstance(r, (int, float))]
        total = sum(nums)
        avg = (total / len(nums)) if nums else 0.0
        return {
            "record_count": count,
            "numeric_count": len(nums),
            "sum": total,
            "average": round(avg, 4),
            "status": "transformed",
        }
    return {"raw": payload, "status": "processed"}


@register_handler("compute_prime")
def handle_compute_prime(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    CPU-intensive mathematical operation:
    Computes prime numbers up to n using sieve of Eratosthenes or checks primality.
    """
    limit = min(payload.get("n", 1000), 100_000)
    sieve = [True] * (limit + 1)
    sieve[0] = sieve[1] = False

    for p in range(2, int(limit**0.5) + 1):
        if sieve[p]:
            for i in range(p * p, limit + 1, p):
                sieve[i] = False

    primes = [p for p in range(2, limit + 1) if sieve[p]]
    return {
        "limit": limit,
        "prime_count": len(primes),
        "largest_prime": primes[-1] if primes else None,
        "first_ten": primes[:10],
    }


@register_handler("io_simulation")
def handle_io_simulation(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    I/O simulation:
    Simulates external network/disk request with delay and jitter.
    """
    delay = float(payload.get("delay_seconds", 0.1))
    jitter = random.uniform(0.01, 0.05) if payload.get("jitter", True) else 0.0
    total_wait = min(delay + jitter, 10.0)

    time.sleep(total_wait)
    return {
        "requested_delay": delay,
        "actual_delay_seconds": round(total_wait, 4),
        "bytes_processed": payload.get("size_bytes", 1024),
        "status": "io_completed",
    }


@register_handler("db_process")
def handle_db_process(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Database processing:
    Simulates batch query processing, aggregation, and index lookups.
    """
    batch_size = payload.get("batch_size", 50)
    simulated_rows = [{"id": i, "val": i * 2, "hash": hash(str(i))} for i in range(batch_size)]
    return {
        "rows_processed": len(simulated_rows),
        "batch_status": "synced",
        "checksum": sum(r["val"] for r in simulated_rows),
    }


@register_handler("long_running")
def handle_long_running(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Long-running task:
    Simulates multi-stage pipeline with step intervals.
    """
    steps = min(payload.get("steps", 3), 10)
    step_duration = float(payload.get("step_duration", 0.05))

    completed_steps = []
    for step in range(1, steps + 1):
        time.sleep(step_duration)
        completed_steps.append(f"Step {step}/{steps} completed")

    return {
        "total_steps": steps,
        "steps_log": completed_steps,
        "status": "pipeline_complete",
    }


@register_handler("failure_simulation")
def handle_failure_simulation(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Fault injection handler:
    Deliberately throws an exception to test retry logic, exponential backoff, and DLQ.
    """
    fail_until_attempt = payload.get("fail_until_attempt", 999)
    current_attempt = payload.get("_current_attempt", 1)

    if current_attempt < fail_until_attempt:
        error_msg = payload.get("error_message", f"Simulated transient error on attempt {current_attempt}")
        raise RuntimeError(error_msg)

    return {
        "status": "recovered_after_retries",
        "succeeded_at_attempt": current_attempt,
    }


@register_handler("timeout_simulation")
def handle_timeout_simulation(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Timeout fault injection handler:
    Sleeps longer than timeout to test deadline enforcement.
    """
    sleep_seconds = float(payload.get("sleep_seconds", 10.0))
    time.sleep(sleep_seconds)
    return {"status": "finished_after_sleep", "slept": sleep_seconds}
