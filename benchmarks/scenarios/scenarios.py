"""Benchmark scenario specifications for TaskForge."""
from typing import Any, Dict

SCENARIOS: Dict[str, Dict[str, Any]] = {
    "smoke": {
        "description": "Rapid sanity check for pipeline connectivity and basic throughput.",
        "total_tasks": 100,
        "concurrency": 5,
        "trials": 5,
    },
    "baseline": {
        "description": "Standard baseline measurement under steady low-to-medium concurrency.",
        "total_tasks": 1000,
        "concurrency": 20,
        "trials": 5,
    },
    "burst": {
        "description": "High-volume spike traffic to evaluate queue buffering and rate limiting.",
        "total_tasks": 3000,
        "concurrency": 50,
        "trials": 5,
    },
    "moderate": {
        "description": "Sustained load under representative production volume.",
        "total_tasks": 5000,
        "concurrency": 50,
        "trials": 5,
    },
    "heavy": {
        "description": "Stress-test approaching worker pool and database connection saturation.",
        "total_tasks": 10000,
        "concurrency": 100,
        "trials": 5,
    },
}

