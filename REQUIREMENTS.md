# TaskForge Requirements & Acceptance Register

## 1. Functional Requirements

- **FR-01 (Task Ingestion):** The platform must accept tasks with custom JSON payloads, priorities (1 to 4), timeout deadlines, and retry policies via a REST API.
- **FR-02 (State Persistence):** Task states, attempt histories, output results, and event logs must persist in PostgreSQL and survive process restarts.
- **FR-03 (Asynchronous Decoupling):** HTTP task submission must respond immediately with HTTP 201 Created and an assigned UUID without blocking for task completion.
- **FR-04 (Transactional Outbox):** Task persistence and broker message staging must execute within the same database transaction.
- **FR-05 (Multi-Workload Handlers):** The platform must support registered handlers for JSON transformation, CPU-intensive math, I/O simulation, DB processing, and long-running pipelines.
- **FR-06 (Idempotency):** The platform must accept client-supplied idempotency keys to guarantee duplicate submissions return the existing task without duplicate execution.
- **FR-07 (Retry & Backoff):** Failed tasks must automatically retry up to `max_retries` with exponential backoff and randomized jitter.
- **FR-08 (Dead-Letter Handling):** Tasks exceeding maximum retries must transition to `DEAD_LETTER` with complete stack traces and attempt histories.
- **FR-09 (Lease Management & Reaper):** Worker execution must use distributed leases; crashed workers must have their tasks automatically recovered.
- **FR-10 (Cancellation):** Non-terminal tasks (`PENDING`, `QUEUED`, `RUNNING`, `RETRY_WAIT`) must be cancellable via API or UI.
- **FR-11 (Telemetry & Metrics):** Real-time Prometheus metrics (`/metrics`) and JSON system statistics (`/api/v1/stats`) must be exposed.
- **FR-12 (Interactive Dashboard):** A responsive web UI must allow task submission, live status inspection, detailed drawer analysis, and chaos injection.

---

## 2. Master Acceptance Criteria Register

| ID | Requirement | Pass Condition | Status |
|---|---|---|---|
| **AC-01** | Clean Infrastructure Startup | Application starts and dependencies connect | PASSED |
| **AC-02** | Health Checks Pass | `/health/live` and `/health/ready` report expected health checks | PASSED |
| **AC-03** | Database Persistence | Records survive process restarts and schema queries | PASSED |
| **AC-04** | Task Submission & Retrieval | 100/100 submissions persisted and all IDs retrievable | PASSED |
| **AC-05** | State Machine Correctness | 100% legal transitions accepted; all illegal transitions rejected | PASSED |
| **AC-06** | Asynchronous Execution | API does not block; workers process tasks in background | PASSED |
| **AC-07** | Multiple Workers | Concurrent worker threads process independent tasks simultaneously | PASSED |
| **AC-08** | Transactional Outbox | Pending outbox records are staged atomically and dispatched | PASSED |
| **AC-09** | Idempotent Submission | 100 repeated submissions with same key produce 1 logical task | PASSED |
| **AC-10** | Retry and Timeout Handling | Retries follow exponential backoff; timeouts enforce deadline | PASSED |
| **AC-11** | Worker Crash Recovery | Expired leases detected and recovered by reaper without lost tasks | PASSED |
| **AC-12** | Duplicate Delivery Safety | Re-delivered messages for finished tasks cause zero duplicate side effects | PASSED |
| **AC-13** | Configurable Scaling | Worker concurrency can be scaled without application code changes | PASSED |
| **AC-14** | Rate Limiting & Caching | Sliding window rate limiter rejects bursts; cache accelerates reads | PASSED |
| **AC-15** | Query Optimization Evidence | Composite indexes eliminate sequential scans | PASSED |
| **AC-16** | Metrics Collection | Prometheus metrics exposed with proper labels | PASSED |
| **AC-17** | Monitoring Dashboards | 12-panel Grafana dashboard definition provided | PASSED |
| **AC-18** | Distributed Tracing | Correlation IDs injected and propagated across lifecycle | PASSED |
| **AC-19** | Benchmark Automation | Automated runner measures throughput and latency percentiles | PASSED |
| **AC-20** | Five-Run Methodology | 5 consecutive trials per profile with statistical variability analysis | PASSED |
| **AC-21** | Performance Reports | Baseline vs optimized benchmarks documented in register | PASSED |
| **AC-22** | Reproducible Public Release | Clean repository structure, docker setup, and comprehensive docs | PASSED |
