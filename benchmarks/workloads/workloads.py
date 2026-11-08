"""TaskForge Standard Workload Definitions for Benchmarks.

Defines the 5 canonical workloads:
1. json_transform: CPU memory data restructuring.
2. compute_prime: Pure CPU compute bound math.
3. io_simulation: Network/disk asynchronous IO latency.
4. db_process: Database record read/write simulation.
5. long_running: Multi-step pipeline execution.
"""
from typing import Any, Dict


def get_workload_payload(task_type: str, scale: int = 1) -> Dict[str, Any]:
    if task_type == "json_transform":
        return {
            "records": [i for i in range(10 * scale)],
            "operation": "normalize_and_aggregate",
        }
    elif task_type == "compute_prime":
        return {
            "n": 100 * scale,
        }
    elif task_type == "io_simulation":
        return {
            "delay_seconds": 0.05 * scale,
            "jitter": True,
        }
    elif task_type == "db_process":
        return {
            "batch_size": 25 * scale,
        }
    elif task_type == "long_running":
        return {
            "steps": 3,
            "step_duration": 0.02 * scale,
        }
    elif task_type == "failure_simulation":
        return {
            "fail_until_attempt": 2,
            "error_message": "Benchmark injected transient failure",
        }
    return {"scale": scale}

