"""Seed realistic demonstration tasks into TaskForge."""
import time
import httpx

BASE_URL = "http://127.0.0.1:8000"


def seed_tasks():
    client = httpx.Client(base_url=BASE_URL, timeout=10.0)

    demo_tasks = [
        {
            "task_name": "Order ETL Transformation #1082",
            "task_type": "json_transform",
            "payload": {"records": [10, 25, 42, 99, 150]},
            "priority": 1,
        },
        {
            "task_name": "Prime Number Generation (n=500)",
            "task_type": "compute_prime",
            "payload": {"n": 500},
            "priority": 2,
        },
        {
            "task_name": "S3 Cloud Storage I/O Sync",
            "task_type": "io_simulation",
            "payload": {"delay_seconds": 0.5, "jitter": True},
            "priority": 3,
        },
        {
            "task_name": "Warehouse Batch Aggregator",
            "task_type": "db_process",
            "payload": {"batch_size": 250},
            "priority": 2,
        },
        {
            "task_name": "Video Transcode Pipeline Simulation",
            "task_type": "long_running",
            "payload": {"steps": 4, "step_duration": 0.25},
            "priority": 4,
        },
        {
            "task_name": "Resilient Retry Demo Task",
            "task_type": "failure_simulation",
            "payload": {"fail_until_attempt": 2, "error_message": "Transient network timeout"},
            "max_retries": 3,
            "priority": 2,
        },
        {
            "task_name": "Quarantine Demo (Dead Letter)",
            "task_type": "failure_simulation",
            "payload": {"fail_until_attempt": 999, "error_message": "Permanent downstream auth failure"},
            "max_retries": 1,
            "priority": 3,
        },
    ]

    print(f"Connecting to {BASE_URL}...")
    try:
        health = client.get("/health/live")
        if health.status_code != 200:
            print("TaskForge is not healthy.")
            return
    except Exception as exc:
        print(f"Failed to connect to TaskForge at {BASE_URL}: {exc}")
        print("Please start TaskForge first (e.g. `python scripts/run_dev.py`).")
        return

    print("Submitting demo tasks...")
    for t in demo_tasks:
        resp = client.post("/api/v1/tasks", json=t)
        if resp.status_code == 201:
            data = resp.json()
            print(f"  [+] Created: {data['task_name']} (ID: {data['id'][:8]}..., Priority: {data['priority']})")
        else:
            print(f"  [-] Failed: {t['task_name']} - {resp.status_code}: {resp.text}")

    print("\nDemo seed complete! Visit http://127.0.0.1:8000/ui to view live processing.")


if __name__ == "__main__":
    seed_tasks()
