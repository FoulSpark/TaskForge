"""Locust Load Testing Suite for TaskForge.

Simulates concurrent user load, mixed workload submission, polling,
idempotency storming, and observability querying.
"""

import random

from locust import HttpUser, between, task


class TaskForgeUser(HttpUser):
    wait_time = between(0.1, 1.0)

    WORKLOADS = [
        ("json_transform", {"records": [random.randint(1, 100) for _ in range(10)]}, 4),
        ("compute_prime", {"n": random.randint(50, 200)}, 2),
        ("io_simulation", {"delay_seconds": 0.05, "jitter": True}, 2),
        ("db_process", {"batch_size": random.randint(20, 100)}, 1),
        ("long_running", {"steps": 2, "step_duration": 0.05}, 1),
    ]

    @task(6)
    def submit_and_poll_task(self):
        """Submit a task and poll until complete or timeout."""
        # Weighted choice of workload
        workload_types, payloads, weights = zip(*self.WORKLOADS, strict=False)
        chosen_type = random.choices(workload_types, weights=weights, k=1)[0]
        chosen_payload = next(p for t, p, _ in self.WORKLOADS if t == chosen_type)
        priority = random.choice([1, 2, 3, 4])

        resp = self.client.post(
            "/api/v1/tasks",
            json={
                "task_name": f"Locust-{chosen_type}-{random.randint(1000, 9999)}",
                "task_type": chosen_type,
                "payload": chosen_payload,
                "priority": priority,
                "timeout_seconds": 30,
            },
            name="/api/v1/tasks [submit]",
        )
        if resp.status_code == 201:
            task_id = resp.json().get("id")
            if task_id:
                # Poll task once
                self.client.get(f"/api/v1/tasks/{task_id}", name="/api/v1/tasks/[id] [poll]")

    @task(3)
    def query_recent_tasks(self):
        """Query the paginated tasks list."""
        page = random.randint(1, 3)
        self.client.get(
            f"/api/v1/tasks?page={page}&page_size=20",
            name="/api/v1/tasks [list]",
        )

    @task(2)
    def query_system_stats(self):
        """Query telemetry stats endpoint."""
        self.client.get("/api/v1/stats", name="/api/v1/stats")

    @task(1)
    def query_health_ready(self):
        """Query health readiness."""
        self.client.get("/health/ready", name="/health/ready")


class TaskForgeChaosUser(HttpUser):
    wait_time = between(0.2, 0.5)

    @task(3)
    def submit_duplicate_idempotent_task(self):
        """Simulate concurrent client duplicate delivery with identical idempotency key."""
        fixed_key = f"locust-shared-key-{random.randint(1, 5)}"
        self.client.post(
            "/api/v1/tasks",
            json={
                "task_name": "Idempotent Storm Task",
                "task_type": "json_transform",
                "payload": {"data": "idempotency-test"},
                "idempotency_key": fixed_key,
            },
            name="/api/v1/tasks [idempotency storm]",
        )

    @task(1)
    def trigger_failure_simulation(self):
        """Submit a task intended to test failure and retry backoff."""
        self.client.post(
            "/api/v1/tasks",
            json={
                "task_name": "Simulated Chaos Task",
                "task_type": "failure_simulation",
                "payload": {"fail_until_attempt": 2, "error_message": "Locust injected error"},
                "max_retries": 2,
            },
            name="/api/v1/tasks [chaos failure]",
        )
