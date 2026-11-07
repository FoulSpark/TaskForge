# TASKFORGE

**A Reliable, Observable, and Scalable Asynchronous Task Processing Platform**

[![Python 3.12](https://img.shields.io/badge/Python-3.12-blue.svg)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/API-FastAPI-009688.svg)](https://fastapi.tiangolo.com/)
[![PostgreSQL](https://img.shields.io/badge/Database-PostgreSQL-336791.svg)](https://www.postgresql.org/)
[![Redis](https://img.shields.io/badge/Broker-Redis-DC382D.svg)](https://redis.io/)
[![Celery](https://img.shields.io/badge/Workers-Celery-37814A.svg)](https://docs.celeryq.dev/)
[![Prometheus](https://img.shields.io/badge/Metrics-Prometheus-E6522C.svg)](https://prometheus.io/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

> **Project Motto:** *Build systems that can prove they work.*

---

## 🚀 Live Interactive Frontend & Active Backend

TaskForge separates task submission from task execution to guarantee high concurrency and ultra-low HTTP latency. It features an **active backend** and a **mission control web dashboard** where users can submit tasks, watch real-time state machine transitions, inspect attempt histories, and inject chaos faults live.

```
Client / Web UI → FastAPI → PostgreSQL (State) 
                         ↘ Transactional Outbox → Redis (Priority Broker) 
                                                      ↓
                                              Celery / Worker Fleet 
                                                      ↓
                                           PostgreSQL (Results & Audit)
```

- **Interactive Web Dashboard:** `http://127.0.0.1:8000/` or `/ui`
- **Interactive Swagger Docs:** `http://127.0.0.1:8000/docs`
- **Prometheus Metrics:** `http://127.0.0.1:8000/metrics`
- **Real-Time System Telemetry:** `http://127.0.0.1:8000/api/v1/stats`

---

## 🌟 Key Engineering Capabilities

1. **Transactional Outbox Pattern:**
   Guarantees zero lost tasks. Task definition and outbox event are committed atomically in PostgreSQL. Even if the message broker is temporarily offline, tasks are staged safely and published automatically upon reconnection.

2. **Strict Task State Machine:**
   Enforces legal lifecycle transitions:
   `PENDING` → `QUEUED` → `RUNNING` → `SUCCESS` / `RETRY_WAIT` / `FAILED` → `DEAD_LETTER` / `CANCELLED`.
   Illegal transitions are rejected with HTTP 409 and logged to `task_events`.

3. **Reliability & Fault Tolerance:**
   - **Idempotency Keys:** Client-supplied deduplication prevents duplicate side effects (verified under 100-request storm tests).
   - **Exponential Backoff with Full Jitter:** Automatically spaces retries $T = \min(60, 2^{\text{attempt}} + \text{jitter})$ to prevent thundering herd spikes.
   - **Distributed Execution Leases:** Workers acquire and auto-renew time-bounded leases. If a worker crashes, the background reaper recovers the task safely without data loss.
   - **Dead-Letter Queue (DLQ):** Tasks that exhaust max retries enter `DEAD_LETTER` with full attempt audit logs and stack traces.
   - **Deadline Enforcement:** Strict per-task timeouts abort long-running runaway workloads.

4. **Multi-Workload Processing:**
   Built-in registered handlers:
   - `json_transform`: Lightweight JSON transformation, sorting, and statistical aggregation.
   - `compute_prime`: CPU-intensive prime generation using the sieve algorithm.
   - `io_simulation`: Network fetch / I/O latency simulation with configurable jitter.
   - `db_process`: Relational record batch query and checksum calculations.
   - `long_running`: Multi-stage pipeline execution with step logs.
   - `failure_simulation` & `timeout_simulation`: Chaos injection workloads.

5. **Observability & Monitoring:**
   - Native `/metrics` exposing 12 Prometheus metrics (submission counters, throughput, latency percentiles, queue depth by priority, active workers, connection pool gauges).
   - Pre-configured Grafana dashboard (`infrastructure/grafana/dashboards/taskforge_overview.json`).
   - OpenTelemetry distributed tracing and `X-Correlation-ID` propagation.

---

## 🛠️ Quickstart

### Prerequisites
- Python 3.12+
- Docker & Docker Compose (optional for local multi-container deployment)

### Option 1: Zero-Dependency Local Python (Immediate Run)

TaskForge features automatic fallback to SQLite and in-memory priority queue, allowing you to run the complete active platform out-of-the-box on any developer machine without starting Docker first:

```powershell
# 1. Clone repository
git clone https://github.com/your-username/taskforge.git
cd taskforge

# 2. Activate virtual environment
.\.venv\Scripts\activate

# 3. Start TaskForge Active Backend & Dashboard
python -m uvicorn taskforge.main:app --host 127.0.0.1 --port 8000 --reload
```

Open your browser to **`http://127.0.0.1:8000/`** to interact with the live Mission Control Dashboard!

### Option 2: Full Docker Compose Deployment (PostgreSQL + Redis + Celery + Prometheus + Grafana)

```bash
docker compose up -d
```

Services started:
- **FastAPI API & UI:** `http://localhost:8000`
- **PostgreSQL 16:** `localhost:5432`
- **Redis 7:** `localhost:6379`
- **Prometheus:** `http://localhost:9090`
- **Grafana:** `http://localhost:3000` (admin / admin)

---

## 🧪 Automated Testing

TaskForge includes an automated test suite across unit, integration, and reliability engineering:

```powershell
# Run all tests
python -m pytest

# Run by phase
python -m pytest tests/unit/test_state_machine.py
python -m pytest tests/integration/test_tasks_api.py
python -m pytest tests/integration/test_execution.py
python -m pytest tests/reliability/test_reliability.py
```

---

## 📊 Benchmark Experiments & 5-Run Methodology

TaskForge includes an automated benchmark runner that executes 5 repeated trials per experiment, recording median, mean, P50, P95, P99, and standard deviation:

```powershell
# Run Baseline Benchmark (1,000 tasks, 5 trials)
python benchmarks/runner/run_benchmarks.py --url http://127.0.0.1:8000 --profile baseline

# Run Smoke Benchmark (100 tasks, 5 trials)
python benchmarks/runner/run_benchmarks.py --url http://127.0.0.1:8000 --profile smoke
```

Raw JSON benchmarks are preserved in `benchmarks/raw/` and CSV summary reports are saved in `benchmarks/reports/`.

---

## 📑 Architecture Decision Records (ADRs)

Key architectural choices are formally documented with context, alternatives, and validation evidence:

- [ADR-001: Modular Monolith vs Microservices](docs/adr/ADR-001-modular-monolith-vs-microservices.md)
- [ADR-002: PostgreSQL as Source of Truth](docs/adr/ADR-002-postgresql-as-source-of-truth.md)
- [ADR-003: Redis as Message Broker](docs/adr/ADR-003-redis-as-message-broker.md)
- [ADR-004: Celery and Custom Async Dispatcher](docs/adr/ADR-004-celery-and-async-dispatcher.md)
- [ADR-005: Transactional Outbox Pattern](docs/adr/ADR-005-transactional-outbox-pattern.md)
- [ADR-006: At-Least-Once Delivery Semantics](docs/adr/ADR-006-at-least-once-delivery.md)
- [ADR-007: Idempotency Mechanism](docs/adr/ADR-007-idempotency-mechanism.md)
- [ADR-008: Task State Machine](docs/adr/ADR-008-task-state-machine.md)
- [ADR-009: Worker Recovery and Leases](docs/adr/ADR-009-worker-recovery-and-leases.md)
- [ADR-010: Metrics and Tracing Architecture](docs/adr/ADR-010-metrics-and-tracing-architecture.md)

---

## 🛡️ License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
