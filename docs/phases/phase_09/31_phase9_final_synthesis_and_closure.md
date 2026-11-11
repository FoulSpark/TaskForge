# Phase 9 — Final Engineering Synthesis & Milestone Closure Report

**Program Phase:** Phase 9 — Operational Hardening, Performance Characterization & Public Verification  
**Final Milestone:** Gate G9.5 Stage 4 — Phase 9 Final Synthesis & Closure  
**Status:** SYNTHESIS COMPLETE — SUBMITTED FOR FORMAL PHASE 9 CLOSURE  
**Date:** 2026-11-11  
**Active Branch:** `phase-9-hardening`  
**Phase 8 Frozen Tag:** `v0.8.0-phase8-freeze` (Tag `86475e20bb4eaf6c0c72d35d7694e9a23d78e1d3` $\to$ Commit `da27047f56fe2eec75acdb8502f505542cf61202`, Preserved)  
**G9.3 Production Optimization Commit:** `a89858b73f8488e05cbcc2393a61f3080ffc7a38` (Preserved)  
**Latest Production Source (`src/`) Commit:** `5ab5abd` (Gate G9.4.4; 0 `src/` changes across Gate G9.4 Synthesis & all Gate G9.5 Stages 0–4)  
**Canonical Public Benchmark Harness / Artifact Commits:** `1d4a76cd047a61548e92e57cfb1f032ba7914068` (`Run fc557f14`) / `2a14600`  
**Public Whitepaper & Diagrams Commits:** `db333f6` / `b472d6d`  

---

## 1. Executive Summary & Milestone Reconciliation (G9.0 – G9.5)

Phase 9 transformed TaskForge from a functionally complete Phase 8 prototype (`v0.8.0-phase8-freeze` at `da27047`) into an empirically characterized, security-hardened, observable, resilience-tested, and independently reproducible asynchronous task processing platform.

Every Phase 9 gate and subgate has progressed through formal scoping, implementation, reconciliation, and closure review:

| Gate / Stage | Scope & Deliverable Focus | Key Implementation / Evidence Commits | Current Status |
| :--- | :--- | :--- | :---: |
| **G9.0** | Baseline & Scope Lock, STRIDE Threat Model, Trust Boundaries, Baseline Profiling (`W-01`–`W-03`) | `1e32386` | **CLOSED** |
| **G9.1** | Security Hardening: `tf_live_...` API Keys, RBAC Scopes, Tenant Fencing, Redis Lua Rate Limiter, Secret Sanitization, Migration 003 | `d9887c9`, `0f8f353`, `68d1e3c` | **CLOSED** |
| **G9.2** | Observability & Health Probes: 22 Bounded Prometheus Metrics, `ReadinessProbeCoordinator`, `GaugeSnapshotCache`, `PrometheusHttpMiddleware` | `e38c894` – `ada0b9b` | **CLOSED** |
| **G9.3** | Lifecycle Attribution (16-event profiler), Capacity Wake-up (`threading.Event`) Production Adoption, Batch Dequeue Rejection (`c <= 10`) | `0cc079d` – `9422505` (Prod: `a89858b`, Synth: `2830ac9`) | **CLOSED** |
| **G9.4.1** | Outbox Publisher Lag Attribution & 1,000-Event Burst Drain Profiling (`276.10 tasks/s`) | `3fee428`, `f835287`, `afd3313` | **CLOSED** |
| **G9.4.2** | DB Connection Pool Telemetry (`InstrumentedQueuePool`), $2\times$ Saturation Drills, Option A Cardinality Adoption | `83bbec9`, `0d9877b`, `26cc315` | **CLOSED** |
| **G9.4.3** | Worker Lease Renewal Telemetry & Multi-Worker Fencing Verification across all 5 Guarded Repository Write Paths | `d59d584`, `2aaf5c8` | **CLOSED** |
| **G9.4.4** | Broker Reconnection Bounded Backoff & Real OS TCP Socket Outage Recovery (`5.71s`, 0 durable loss) | `5ab5abd`, `15e0268` | **CLOSED** |
| **G9.4** | Operational Resilience & Reliability Synthesis (AC-9.4.1 – AC-9.4.7) | `1e1d736` | **CLOSED** |
| **G9.5 Stage 0** | Public Benchmark Protocol, Provenance Lock, `ZPOPMAX` Reconciliation, Option A (`25/28` series) & Demo Contract (D1–D12) | `3eb1d51`, `7a6dd87` | **CLOSED** |
| **G9.5 Stage 1** | Reproducible Local Demo Harness (`scripts/demo.py`, D1–D12, `RealSocketTCPProxy`, Ownership Manifest) | `a6f4af2`, `f740e13` | **CLOSED** |
| **G9.5 Stage 2** | Canonical Public Benchmark Suite (`benchmarks/public/run_public_benchmarks.py`, `schema/v1.json`, Run `fc557f14`, Exact Latency Decomposition) | `40d40a6`, `1d4a76c`, `2a14600` | **CLOSED** |
| **G9.5 Stage 3** | Public Architecture Whitepaper (`docs/whitepaper/ARCHITECTURE.md`) & Rendered Mermaid Diagrams (`D1`–`D4`) + Link Validation | `db333f6`, `b472d6d` | **CLOSED** |
| **G9.5 Stage 4** | Phase 9 Final Engineering Synthesis & Release-Readiness Closure Report (This Document) | *Current Head* | **SUBMITTED FOR CLOSURE** |

---

## 2. Architecture & Implementation Provenance

Phase 9 maintains a strict, auditable Git lineage separating frozen baselines, production code changes, diagnostic campaigns, and public documentation:

```text
v0.8.0-phase8-freeze (tag 86475e20 -> commit da27047, Phase 8 src baseline 9ab6f91)
  ├── G9.0 Baseline Lock & Threat Model (1e32386)
  ├── G9.1 Security Hardening (d9887c9 -> 0f8f353 -> 68d1e3c)
  ├── G9.2 Observability & Probes (e38c894 .. ada0b9b)
  ├── G9.3 Profiling & Production Capacity Wake-up Adoption (a89858b; synth 2830ac9 -> 9422505)
  ├── G9.4 Resilience & Final src/ Hardening:
  │     ├── G9.4.1 Outbox Diagnostics (3fee428 -> afd3313)
  │     ├── G9.4.2 DB Pool Telemetry & Option A Lock (83bbec9 -> 26cc315)
  │     ├── G9.4.3 Lease Renewal & 5-Path Fencing (d59d584 -> 2aaf5c8)
  │     ├── G9.4.4 Broker Reconnect Backoff [Last src/ touch] (5ab5abd -> 15e0268)
  │     └── G9.4 Synthesis (1e1d736)
  └── G9.5 Public Package [0 src/ changes, 0 benchmarks/scenarios/ changes]:
        ├── Stage 0 Protocol Lock (3eb1d51 -> 7a6dd87)
        ├── Stage 1 Local Demo Harness (a6f4af2 -> f740e13)
        ├── Stage 2 Public Benchmarks (40d40a6 -> harness 1d4a76c [Run fc557f14] -> evidence 2a14600)
        ├── Stage 3 Whitepaper & Mermaid D1-D4 (db333f6 -> b472d6d)
        └── Stage 4 Phase 9 Final Synthesis (Current Commit)
```

### Provenance Clarifications Preserved Across Milestones
1. **Production `src/` Boundary:** Commit `a89858b` introduced the production dispatcher capacity wake-up optimization; subsequent G9.4 resilience/telemetry commits (`83bbec9`, `d59d584`, `5ab5abd`) completed all production runtime modifications under `src/`. Since G9.4 synthesis (`1e1d736`) through all of Gate G9.5 Stages 0–4, `git diff --stat 1e1d736..HEAD -- src/ benchmarks/scenarios/` is strictly empty (`0` modifications).
2. **Stage 2 Clean Benchmark Lineage:** Benchmark Run `fc557f14` (`git_dirty = false`) executed against clean harness commit `1d4a76cd047a61548e92e57cfb1f032ba7914068` (`harness_sha = 6576de5ba6...`); the resulting artifact `benchmarks/public/benchmark_results.json` and reconciliation addendum `28_g95_stage2_reconciliation_addendum.md` were subsequently committed in `2a14600`.
3. **Historical Isolation:** Pre-existing G9.0, G9.3, and G9.4 diagnostic scripts and JSON reports in `benchmarks/scenarios/` remain completely preserved without alteration.

---

## 3. Master Acceptance-Criteria Traceability Matrix

Early Phase 9 scope placeholders in [`docs/phases/phase_09/04_acceptance_criteria.md`](04_acceptance_criteria.md) were refined and decomposed into formal gate contracts across G9.1–G9.5. Every executed Phase 9 criterion maps to a concrete verification artifact:

| Gate / Criterion | Requirement Summary | Primary Evidence Artifact(s) | Verification Command / Result | Status |
| :--- | :--- | :--- | :--- | :---: |
| **G9.0 (Baseline & Threat Model)** | Immutable tag `v0.8.0-phase8-freeze` (`da27047`), STRIDE model, trust boundaries, 900-task baseline profiling | `00_baseline_and_scope_lock.md`, `01_threat_model.md`, `02_trust_boundaries.md`, `benchmarks/scenarios/g90/baseline_performance.json` | 165/165 pre-hardening baseline tests; `W-01`–`W-03` JSON baseline recorded | **SATISFIED** |
| **G9.1 (Security Hardening)** | `tf_live_...` SHA-256 keys, RBAC scopes, tenant isolation, Redis Lua sliding-window rate limiter, secret masking, Alembic migration 003 safety | `src/taskforge/core/auth.py`, `rate_limiter.py`, `sanitizer.py`, `scripts/verify_migration_safety.py`, `tests/security/test_security_hardening.py` | 16/16 security tests & 175/175 regression at `0f8f353`; included in 267/267 final suite | **SATISFIED** |
| **G9.2 (Observability & Health)** | Bounded `/metrics` exporter, `/health/live`, `/health/ready` (`ReadinessProbeCoordinator`), `GaugeSnapshotCache`, HTTP telemetry middleware | `06_metrics_contract.md`, `tests/integration/test_g92_health_probes.py`, `test_g92_gauge_cache.py`, `test_g92_lifecycle_metrics.py`, `test_g92_http_telemetry.py` | All G9.2 integration suites passing inside 267/267 full regression | **SATISFIED** |
| **G9.3 (Profiling & Dispatcher Optimization)** | 16-event attribution, capacity wake-up optimization (`a89858b`), target-lock rejecting batch dequeue (`c <= 10`) | `15_g93_final_performance_profiling_and_optimization_report.md`, `benchmarks/scenarios/g93/*.json`, `tests/unit/test_dispatcher_capacity_wakeup.py` | 9/9 capacity wake-up tests + 18-trial Phase 2C campaign (`235/235` at `2d3b7f6`) | **SATISFIED** |
| **G9.4 AC-9.4.1 & AC-9.4.2** | Outbox lag baseline + 1,000-event burst drain capacity across 3 trials | `17_g941_outbox_publisher_diagnostic_report.md`, `benchmarks/scenarios/g94/g941_outbox_report.json` | Mean `276.10 tasks/s`, `3.622s` mean drain, exact Redis ZSET membership audit | **SATISFIED** |
| **G9.4 AC-9.4.3** | Connection pool $2\times$ contention (30 callers / 15 pool capacity), bounded timeout, 0 leaks | `18_g942_connection_pool_resilience_report.md`, `tests/integration/test_g942_pool_telemetry_saturation.py` | Bounded wait $\le 5.011\text{s}$, recovery $< 0.1\text{s}$, 0 leaks | **SATISFIED** |
| **G9.4 AC-9.4.4** | Stale-worker fencing across all 5 guarded repository write paths | `19_g943_lease_fencing_resilience_report.md`, `tests/integration/test_g943_lease_fencing.py` | 5/5 stale attempts rejected, 0 stale writes accepted, 5 fencing rejections recorded | **SATISFIED** |
| **G9.4 AC-9.4.5** | Un-mocked OS TCP proxy Redis outage (`5.71s`), 0 durable event loss, bounded backoff | `20_g944_broker_outage_recovery_report.md`, `21_g944_reconciliation_addendum.md`, `tests/integration/test_g944_broker_outage_recovery.py` | 50/50 outbox events preserved & published, 50/50 tasks `SUCCESS` | **SATISFIED** |
| **G9.4 AC-9.4.6** | Finite label hygiene & Option A telemetry ceiling ($\le 25$ net-new G9.4, $\le 28$ combined) | `24_g95_stage0_reconciliation_addendum.md`, `src/taskforge/monitoring/metrics.py` | Exactly `25` net-new G9.4 series (`19` pool + `6` lease/fencing) + `3` G9.2 baseline = `28` | **SATISFIED** |
| **G9.4 AC-9.4.7** | Full G9.4 regression pass | `22_g94_operational_resilience_and_reliability_synthesis.md` | 253/253 passed at G9.4 closure | **SATISFIED** |
| **G9.5 Stage 0** | Protocol lock, `ZPOPMAX` score proof, Option A reconciliation, D1–D12 contract | `23_g95_stage0_protocol_and_provenance_lock.md`, `24_g95_stage0_reconciliation_addendum.md` | Formally reviewed & accepted (`7a6dd87`) | **SATISFIED** |
| **G9.5 Stage 1 (D1–D12)** | Local reproducible demo with disposable resources, priority verify, outage drill, D7/D8 separation | `scripts/demo.py`, `benchmarks/public/demo_execution_summary.json`, `tests/integration/test_g95_demo_reproducibility.py` | 3/3 integration tests; 41/41 demo identity reconciliation (`f740e13`) | **SATISFIED** |
| **G9.5 Stage 2 (`W-01`–`W-03`)** | Canonical public benchmark suite, `schema/v1.json`, exact additive latency invariant, clean provenance | `benchmarks/public/run_public_benchmarks.py`, `benchmark_results.json`, `tests/integration/test_g95_public_benchmarks.py` | 4/4 benchmark tests; Run `fc557f14` clean (`1d4a76c`), max per-task $\Delta = 0.000000\,\text{ms}$ | **SATISFIED** |
| **G9.5 Stage 3 (Whitepaper & D1–D4)** | 13-section whitepaper, rendered D1–D4 Mermaid diagrams (`mmdc 12.2.0`/`mermaid 12.3.0`), Markdown link/path check | `docs/whitepaper/ARCHITECTURE.md`, `docs/whitepaper/diagrams/*.mmd`, `tests/unit/test_g95_whitepaper_deliverables.py` | 4/4 valid SVGs rendered; 6/6 whitepaper/link tests passed (`b472d6d`) | **SATISFIED** |

---

## 4. Consolidated Performance Synthesis & Measurement Boundaries

To prevent apples-to-oranges comparisons across Phase 9 campaigns, metrics from G9.0, G9.3, G9.4, and G9.5 must always be read alongside their distinct measurement boundaries:

| Campaign & Baseline | Topology & Execution Mode | Warmup & Denominator Policy | Key Empirical Measurements | Measurement Boundary / Scope Note |
| :--- | :--- | :--- | :--- | :--- |
| **G9.0 Baseline** (`da27047`, `W-01`/`W-02`/`W-03`) | HTTP API ingestion via Locust/external harness (`c=10/4/10`), `n=500/200/200` | Full end-to-end wall-clock from initial HTTP `POST` to terminal completion | `W-01`: `10.66 TPS` ($p50=22,823.5\,\text{ms}$)<br>`W-02`: `7.53 TPS` ($p50=14,540.2\,\text{ms}$)<br>`W-03`: `10.96 TPS` ($p50=5,271.1\,\text{ms}$) | Includes HTTP ingress overhead + legacy $200\,\text{ms}$ fixed dispatcher sleep; unoptimized baseline. |
| **G9.3 Production Optimization** (`a89858b`, 18-trial Phase 2B/2C) | 16-event profiler + capacity wake-up (`threading.Event`), `c=4/10/20` | 3-paired multi-trial means across Phase 2A/2B/2C campaigns | Sleep lag **$-94.94\%$ to $-98.66\%$**; replenishment lag **$-79.2\%$ to $-91.0\%$**; worker saturation **$91.3\%$** at $c=4$; E2E $p50$ **$-6.62\%$ to $-23.63\%$** | Showed dispatcher serialization saturation at $c=20$ ($74.0\%$ duty cycle, $24.8\%$ saturation); onset threshold ($10 < c \le 20$) unmeasured. |
| **G9.4.1 Outbox Burst Drain** (`3fee428` / `2ea407c`) | 1,000 pre-seeded outbox rows, $100\,\text{ms}$ poll interval, `batch=50`, 3 trials | Quiescent pre-seeded backlog drain across 20 batches | Mean **`276.10 tasks/s`** (`3.622s` mean); $42.25\%$ active SQL/Redis vs. $57.75\%$ poll sleep | Measures isolated outbox publisher drain capacity only (no worker execution). |
| **G9.5 Public Benchmarks** (`1d4a76c`, Run `fc557f14`, `W-01`–`W-03`) | Pre-seeded tasks + `0.05s` outbox poll + `WorkerDispatcher` (`c=4`), Windows 11 single host | First `10%` warmup excluded from percentiles & steady-state window ($N_{\text{measured}} / \Delta t_{\text{window}}$); whole-run outcomes also tracked (`100%` success) | **`W-01` (`noop`):** `44.78 tasks/s`, E2E mean `4,092.136 ms` (`955.318` outbox + `3,084.507` queue + `52.310` worker tenure)<br>**`W-02` (`cpu` $n=500$):** `44.59 tasks/s`, E2E mean `2,827.287 ms` (`599.911` + `2,173.850` + `53.526`)<br>**`W-03` (`io` $50\,\text{ms}$):** `28.82 tasks/s`, E2E mean `2,296.029 ms` (`316.172` + `1,876.566` + `103.291`) | Single-host Windows 11 / local PG 16.15 / Redis 7.4.12 measurements. **Worker execution / lease tenure** ($t_{\text{completed}} - t_{\text{started}}$) includes DB lease ownership and terminal commit, not isolated Python callable runtime. Do not generalize to other hardware/topologies without rerunning. |

---

## 5. Reliability & Security Limitations

In accordance with TaskForge's public engineering contract ([`docs/whitepaper/ARCHITECTURE.md`](../../whitepaper/ARCHITECTURE.md)):

### 5.1 Core Delivery Guarantee & Explicit Non-Claims
> **TaskForge provides durable task submission through PostgreSQL transactions and a transactional outbox. Queue delivery is at-least-once. Worker ownership is protected by leases and fencing tokens. Task completion is persisted transactionally, and duplicate delivery is handled through idempotent execution and authoritative task state.**

TaskForge **does NOT claim**:
- Universal exactly-once execution.
- Exactly-once external side effects.
- Zero duplicate handler execution.
- Unlimited horizontal scalability.
- Fault tolerance beyond the failure scenarios actually tested in Phase 8 and Phase 9.

### 5.2 Consolidated Operational Boundaries
1. **Duplicate Message Suppression vs. External Side-Effect Idempotency:** PostgreSQL state guards (`is_terminal_state` returning `TERMINAL_DUPLICATE`) and ingress `idempotency_key` (`SQLSTATE 23505`) protect internal task state from duplicate corruption. However, if a worker crashes after calling an external webhook/payment service but *before* committing `complete_task_success()`, lease recovery will trigger handler re-execution under at-least-once delivery. External handlers must propagate downstream idempotency keys.
2. **Tested Failure Boundaries vs. Untested Topologies:** Empirical drills proved resilience against a `5.71s` local OS TCP socket Redis outage, $2\times$ DB pool saturation (`30` callers / `15` pool capacity), and `5/5` stale-worker lease-token collisions. Multi-hour broker outages causing massive outbox backlog growth, PostgreSQL primary failover / replication lag, cross-AZ network partitions, and disk exhaustion remain untested.
3. **Rate-Limiting Failure Policy (`Fail-Closed`):** `RateLimitMiddleware` queries Redis (`LUA_RATE_LIMIT`) and intentionally fails closed (`HTTP 503`, `Retry-After: 5`) if Redis becomes unreachable. While this protects PostgreSQL from unbounded ingestion during broker impairment, it causes API submission unavailability (`POST`/`GET /api/v1/tasks`) until Redis recovers (health probes and `/metrics` remain exempt).
4. **Observability & Cardinality Limits:** Runtime metrics strictly normalize unknown labels (`task_type`, `route`, `priority`, `failure_reason`) into finite whitelist buckets (`"other"`), trading fine-grained per-tenant or per-custom-task observability for bounded TSDB series (`25` net-new G9.4 / `28` combined pool+lease series ceiling). Additionally, `GaugeSnapshotCache` (`5.0s` TTL) serves slightly delayed infrastructure gauges and preserves stale snapshots (or omits cold-start gauges) while incrementing `taskforge_telemetry_scrape_errors_total` if queries fail.
5. **Single-Dispatcher Concurrency Limit:** `WorkerDispatcher` runs a single dequeue loop feeding its thread pool. At `c=20` concurrency, G9.3 observed dispatcher serialization saturation (`74.0%` duty cycle, `24.8%` worker saturation). Horizontal scaling must add worker processes/containers rather than raising per-process thread concurrency beyond `c=10`.

---

## 6. Final Regression & Repository Integrity Audit

All verification commands were executed on the final repository state after all Stage 3 edits:

| Verification Check | Exact Command Executed | Observed Result |
| :--- | :--- | :--- |
| **Full Platform Regression Suite (`b472d6d`)** | `.venv\Scripts\pytest -q` | **`267 passed in 392.02s (0:06:32)`** (`0` failures, `0` skips; run immediately prior to adding this synthesis document to `doc_files`) |
| **G9.5 Documentation & Deliverables Verification (Post-Synthesis Edit)** | `.venv\Scripts\pytest -v tests/unit/test_g95_whitepaper_deliverables.py` | **`6 passed in 0.06s`** (validates whitepaper, D1–D4 embeds, and link/path resolution across `ARCHITECTURE.md`, Stage 3 reports, Stage 4 synthesis, and `README.md`) |
| **G9.5 Demo & Public Benchmark Suites (`b472d6d`)** | `.venv\Scripts\pytest -v tests/integration/test_g95_demo_reproducibility.py tests/integration/test_g95_public_benchmarks.py` | **`7 passed`** (`3` demo tests + `4` public benchmark tests) |
| **Linter (Stage 3 Test Additions)** | `.venv\Scripts\ruff check tests/unit/test_g95_whitepaper_deliverables.py` | **`All checks passed!`** |
| **Stage 3 Mermaid Rendering (`R1`)** | `mmdc` v`12.2.0` (`mermaid` v`12.3.0`) across `D1`–`D4` | **4/4 rendered cleanly** (`exit=0`, valid `<svg>`, `has_syntax_error=False`) |
| **Zero `src/` / Historical Changes in G9.5** | `git diff --stat 1e1d736..HEAD -- src/ benchmarks/scenarios/` | **Empty output** (`0` modifications to `src/` or `benchmarks/scenarios/`) |
| **Immutable Tag & Baseline Verification** | `git rev-parse "v0.8.0-phase8-freeze^{commit}"` / `git rev-parse a89858b...` | Verified intact: `da27047f56fe2eec75acdb8502f505542cf61202` and `a89858b73f8488e05cbcc2393a61f3080ffc7a38` |

---

## 7. Final Release-Readiness Decision

### Verdict: **CONDITIONALLY READY FOR CONTROLLED DEPLOYMENT — RECOMMENDED FOR PHASE 9 CLOSURE**

TaskForge satisfies all technical, operational, security, observability, and documentation gates established for Phase 9 on the target single-primary PostgreSQL 16 + Redis 7 topology, **subject to explicit acceptance of the following operational boundaries and requirements**:

1. **Mandatory Operational Requirements for Deployment:**
   - Provision PostgreSQL with `max_connections` sized above the sum of `API (db_pool_size + max_overflow)` + `Dedicated Telemetry Engine (3)` + `Worker Pool` connections.
   - Run Redis with persistent AOF enabled (`--appendonly yes`) and treat Redis availability as mandatory for API ingress (due to fail-closed rate limiting).
   - Enforce external side-effect idempotency tokens inside any custom task handler calling non-transactional third-party services.
   - Scale worker throughput horizontally via multiple container/process replicas (`c <= 10` per dispatcher) rather than increasing single-process thread concurrency toward `c=20`.
   - Deploy Prometheus scraping `/metrics` with existing provisioned rules in `infrastructure/prometheus/alerting_rules.yml` (`TaskForgeHighFailureRate`, `TaskForgeQueueBuildup`, `TaskForgeNoActiveWorkers`, `TaskForgeDeadLetterDetected`, `TaskForgeHighApiLatency`) and add operational alerts for the G9.4 resilience signals (`taskforge_outbox_lag_seconds`, `taskforge_db_pool_checkout_timeouts_total`, `taskforge_fencing_rejections_total`).
2. **Remaining Accepted Risks (Post-Phase 9 Roadmap Candidates):**
   - Multi-host PostgreSQL HA failover and cross-AZ partition behavior remain unverified by live fault drills.
   - `OutboxPublisherWorker` polling cadence (`0.05s`–`0.5s`) remains polling-driven (`LISTEN/NOTIFY` optimization deferred outside Phase 9 scope).
   - Historical integration test backward-compatibility bypass in `src/taskforge/api/dependencies.py` (`TASKFORGE_TESTING=1` without `TASKFORGE_STRICT_AUTH=1`) must **never** be enabled outside isolated test runs.

With **Gates G9.0, G9.1, G9.2, G9.3, G9.4, and G9.5 Stages 0–3 formally closed** and this final synthesis artifact complete, we submit **Gate G9.5 Stage 4** for final engineering review and request **formal closure of Phase 9**.
