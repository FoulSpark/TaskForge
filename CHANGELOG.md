# Changelog

All notable changes to TaskForge will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [1.0.0] - 2026-11-07

### Added
- **Phase 0 (Project Foundation):**
  - Python 3.12 project structure with `pyproject.toml`.
  - Structured JSON logging via `structlog`.
  - Application configuration with environment variable overrides.
  - `/health/live` and `/health/ready` endpoints.
  - Initial Docker Compose infrastructure.
- **Phase 1 (Core Task Management):**
  - Relational database schema: `tasks`, `task_attempts`, `task_results`, `task_events`.
  - Strict 8-state Task State Machine with transition validation.
  - REST API endpoints: `POST /api/v1/tasks`, `GET /api/v1/tasks/{id}`, `GET /api/v1/tasks` with pagination and filtering.
  - `X-Correlation-ID` middleware and standardized API exception handlers.
- **Phase 2 (Asynchronous Task Execution):**
  - Transactional outbox table (`outbox_events`) and background publisher daemon.
  - Celery application (`celery_app.py`) and Redis broker integration.
  - Five standard workload handlers: `json_transform`, `compute_prime`, `io_simulation`, `db_process`, `long_running`.
  - Worker heartbeat tracking and health monitoring.
- **Phase 3 (Reliability Engineering):**
  - Idempotency key deduplication (tested with 100-request storms).
  - Exponential backoff with full randomized jitter.
  - Distributed worker execution leases with background auto-renewal.
  - Dead-Letter Queue (DLQ) for exhausted retries.
  - Stale lease recovery reaper.
  - Deadline timeout enforcement.
- **Phase 4 (Concurrency and Scalability):**
  - Configurable worker pool concurrency (1, 2, 4, 8 threads/processes).
  - Priority queue scheduling (`CRITICAL`, `HIGH`, `MEDIUM`, `LOW`).
  - Sliding window API rate limiting (`429 Too Many Requests`).
  - Read-through caching with automatic invalidation.
- **Phase 5 (Observability):**
  - Native `/metrics` exposing 12 Prometheus metrics.
  - Pre-configured Grafana 12-panel dashboard JSON.
  - Prometheus alerting rules.
  - Distributed tracing correlation ID propagation.
- **Phase 6 (Performance Benchmarking):**
  - Automated benchmark runner with 5-run statistical methodology.
  - Workload profiles (Smoke, Baseline, Moderate, Heavy).
  - Experiment register documenting E01 through E10.
  - Raw JSON and CSV report export.
- **Phase 7 (Release & Interactive UI):**
  - Full-featured TaskForge Mission Control Web Dashboard (`/` and `/ui`).
  - Chaos Engineering Lab for interactive fault injection.
  - Architecture Decision Records ADR-001 through ADR-010.
  - Complete documentation suite: README, ARCHITECTURE, REQUIREMENTS, BENCHMARKS, SECURITY.
