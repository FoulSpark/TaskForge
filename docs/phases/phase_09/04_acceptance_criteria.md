# G9.0.5 — Acceptance Criteria Across Phase 9 Milestones

## 1. Gate G9.0: Baseline & Scope Lock (Current Gate)

- [ ] **G9.0.1 Provenance:** Tag `v0.8.0-phase8-freeze` (`da27047`) recorded; host OS, CPU, RAM, Docker, PostgreSQL 16.15, and Redis 7.4.12 verified.
- [ ] **G9.0.2 Threat Model:** STRIDE analysis and residual risks documented in `01_threat_model.md`.
- [ ] **G9.0.3 Trust Boundaries:** Access control rules, tenant fencing, and secret masking policies documented in `02_trust_boundaries.md`.
- [ ] **G9.0.4 Roadmap:** Sequential dependencies (9.1 to 9.5) and non-goals documented in `03_architecture_roadmap.md`.
- [ ] **G9.0.5 Baseline Profiling:**
  - Automated external benchmark harness executed against disposable database `taskforge_g90_benchmark`.
  - API submission latency, broker dispatch delay, execution overhead, and end-to-end completion measured across >= 500 samples.
  - Latency percentiles (p50, p95, p99, max) and throughput accurately computed without discarding failed or timed-out tasks.
  - Raw execution logs preserved in `benchmarks/raw/` and aggregated schema validated in `benchmarks/scenarios/g90/baseline_performance.json`.
  - Zero modification to `src/` (implementation baseline `9ab6f91` strictly frozen).

---

## 2. Gate G9.1: Security Hardening

- [x] **AuthN & Key Lifecycle:** API endpoints reject unauthenticated requests with HTTP `401 Unauthorized`. API keys adhere to canonical format `tf_live_<24-hex-key-id>_<32-char-secret>` with SHA-256 digests stored in `hashed_secret`.
- [x] **RBAC:** Endpoints enforce scope checks (`tasks:write`, `tasks:read`, `admin:operator`); clients attempting administrative actions receive HTTP `403 Forbidden`.
- [x] **Tenant Fencing:** All queries enforce `tenant_id` isolation. Client-supplied tenant IDs in request bodies are ignored. Cross-tenant task queries return HTTP `404 Not Found`.
- [x] **Rate Limiting:** Atomic Sliding-Window Log Rate Limiter (Redis Lua-backed sorted set) enforces rolling 60-second quotas (default 300 req/min), returning HTTP `429 Too Many Requests` with `Retry-After` and `X-RateLimit-*` headers.
- [x] **Secret Sanitization:** Automated test suite asserts zero plain credentials, passwords, or raw 64-char lease tokens appear in logs, traces, or HTTP error bodies.

---

## 3. Gate G9.2: Observability & Health Diagnosability

- [ ] **Prometheus Exporter:** `/metrics` endpoint emits standard Prometheus text format with valid gauges, counters, and histograms.
- [ ] **Health Probes:**
  - `GET /healthz/live` returns 200 if the process event loop is responsive.
  - `GET /healthz/ready` returns 200 only if active PostgreSQL pool connection and Redis ping succeed.
- [ ] **Tracing Context:** Distributed `trace_id` / `request_id` propagated across HTTP request, Outbox event, Redis message envelope, and worker execution logs.

---

## 4. Gate G9.3: Deployment Reproducibility

- [ ] **Clean Bootstrap:** `docker-compose up -d` boots a clean environment from zero, applies Alembic migrations automatically, and reaches `healthy` state within 30 seconds.
- [ ] **Migration Drill:** Automated test verifies database migration upgrade, downgrade, and upgrade cycle on production schema.
- [ ] **Recovery Procedure:** Documented runbook verified: simulated container kill (`docker kill taskforge-worker`) recovers tasks automatically upon container restart.

---

## 5. Gate G9.4: Performance Characterization

- [ ] **Throughput Profiling:** Sustained throughput measured across three canonical workloads (Noop, CPU-bound Prime Sieve, I/O Bound).
- [ ] **Tail Latency:** p50, p95, and p99 latencies reported under 10 ops/sec, 100 ops/sec, and saturation arrival rates.
- [ ] **Honest Saturation Boundaries:** System behavior under pool exhaustion and queue saturation documented, showing controlled backpressure rather than silent data loss.

---

## 6. Gate G9.5: Public Architecture Package

- [ ] **Engineering Whitepaper:** Published architecture design record detailing at-least-once delivery, transactional outbox, and lease fencing.
- [ ] **Reproducible Demo:** One-command benchmark and demo script allowing any independent engineer to clone, boot, and verify TaskForge on standard hardware.

