# G9.0 — Baseline and Scope Lock Specification

## 1. Executive Summary & Purpose

Milestone G9.0 establishes an immutable technical baseline against which all subsequent Phase 9 hardening milestones (Security 9.1, Observability 9.2, Deployment 9.3, Performance 9.4, Architecture 9.5) are measured. 

**Core Invariant:** G9.0 is strictly a documentation, specification, and measurement milestone. No security middleware, rate limiting, authentication enforcement, database migrations, queue protocol changes, or worker modifications are introduced in this milestone.

---

## 2. Release Provenance & Baseline Anchor

| Attribute | Measured / Verified Value |
| :--- | :--- |
| **Formal Release Tag** | `v0.8.0-phase8-freeze` |
| **Commit SHA** | `da27047f56fe2eec75acdb8502f505542cf61202` |
| **Platform Implementation Baseline** | `9ab6f91` (`git diff --exit-code 9ab6f91..da27047 -- src/` exits with 0) |
| **Repository State** | Clean working tree; no uncommitted changes |
| **Profiling Isolation** | Detached worktree `..\TaskForge-G90` checking out `v0.8.0-phase8-freeze` |
| **Regression Status** | 165 / 165 tests passing cleanly under `pytest` with zero warnings, zero skips |

---

## 3. Environment & Runtime Specifications

All measurements for Gate G9.0 are conducted on the following verified physical and containerized infrastructure:

### 3.1 Host Hardware & Operating System
* **Host OS Platform:** Windows 11 Professional (AMD64)
* **Kernel / Build:** Windows NT 10.0.26200-SP0
* **Logical CPU Cores:** 12
* **Total Physical RAM:** 15.65 GB (16,025 MB)
* **Python Runtime:** Python 3.12.10 AMD64 (MSC v.1943 64 bit) in virtual environment (`.venv`)

### 3.2 Database & Broker Infrastructure (Docker Containerized)
* **Docker Engine:** Docker version 29.9.2, build a82e528
* **PostgreSQL Service:**
  - **Image:** `postgres:16-alpine`
  - **Engine Version:** PostgreSQL 16.15 on `x86_64-pc-linux-musl`, compiled by gcc (Alpine 15.2.0)
  - **Host Port Binding:** `localhost:5432`
  - **Connection Ceiling (`max_connections`):** 100
  - **Database Mode:** Standalone relational with transactional outbox
* **Redis Service:**
  - **Image:** `redis:7-alpine`
  - **Engine Version:** Redis server 7.4.12
  - **Host Port Binding:** `localhost:6379`
  - **Persistence:** Append-Only File (`appendonly yes`) enabled
  - **Mode:** Standalone in-memory priority broker & delay sorted set

---

## 4. Contractual Corrections & Invariant Locks

1. **Redis Version Verification:** Corrected documentation from speculative `7.2` to the exact running version `7.4.12` reported by `redis.info()`.
2. **Worker Identity Model:** Worker IDs are treated as untrusted process identifiers. Mutual exclusion and ownership validation are exclusively governed by unguessable 64-bit cryptographically random lease tokens committed in PostgreSQL under row-level locks.
3. **Task Immutability Rules:** Following task submission (`POST /api/v1/tasks`), the following fields are strictly immutable:
   - `payload`: Input JSON arguments
   - `task_type`: Registered execution handler name
   - `idempotency_key`: Deduplication identity
   - `max_retries`: Execution budget
   State and status transitions may only proceed via the state machine.
4. **Tenant Isolation Contract:** Task polling and management endpoints must validate both tenant existence and tenant ownership of the requested task ID.
5. **Rate Limiting Scope:** 100 req/sec is defined as a preliminary design target for Phase 9.1; actual production quotas will be calibrated based on G9.0 baseline throughput measurements.
6. **Docker Compose Audit:** Rather than recreating container topology, Phase 9 leverages the existing verified multi-service `docker-compose.yml` (`api`, `worker`, `postgres`, `redis`, `prometheus`, `grafana`).

---

## 5. Baseline Performance Summary & Interpretation Guardrails

The G9.0 baseline profiling campaign was executed across 900 tasks over three canonical workloads on `v0.8.0-phase8-freeze`:
* **W-01 (Baseline Noop Profiling, 500 tasks, c=10):** Measures raw framework, DB transaction, and broker dispatch overhead.
* **W-02 (CPU-Bound Prime Sieve, 200 tasks, c=4):** Measures CPU saturation and task scheduling under compute load.
* **W-03 (I/O Wait Simulation, 200 tasks, c=10):** Measures worker concurrency under external I/O delays.

### Interpretation Guardrails:
1. **Dispatch Delay Causality:** In W-01, the median broker dispatch delay of ~22.5s reflects composite queuing dynamics (burst ingestion filling the outbox and Redis queue, combined with worker polling intervals) under burst concurrency. It is recorded as an empirical measurement of baseline queue drain behavior, not as a single proven defect or isolated root cause.
2. **Worker Attempt Duration:** The `duration_ms` recorded in `task_attempts` measures the entire pipeline attempt lifecycle (`worker_attempt_duration` including lease verification, argument unpacking, execution, result persistence, and state transitions), rather than handler execution alone.
3. **Throughput Denominator:** All throughput figures ($T = N_{\text{completed}} / \Delta t_{\text{total}}$) use the full end-to-end wall-clock time from the first task's initial HTTP submission to the final task's durable completion in PostgreSQL.


