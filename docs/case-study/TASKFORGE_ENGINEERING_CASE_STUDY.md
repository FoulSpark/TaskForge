# TaskForge Engineering Case Study: From Prototype Task Queue to Empirically Verified Reliability

**Scope:** Architecture Evolution, Bottleneck Attribution, Fault-Injection Drills & Reproducible Benchmarking  
**Frozen Phase 9 Baseline:** `v0.9.0-phase9-freeze` (`fb31546`, `267/267` passing tests)  
**Production Code Baseline:** `a89858b` (G9.3 optimization) · `5ab5abd` (G9.4 resilience hardening)  
**Canonical Public Benchmark Run:** `fc557f14` (Harness Commit `1d4a76c`, `git_dirty=false`)  
**Companion Architecture Specification:** [`docs/whitepaper/ARCHITECTURE.md`](../whitepaper/ARCHITECTURE.md)  

---

## 1. The Problem

Building a task queue that "works on the happy path" is straightforward: accept an HTTP `POST`, write a row to a database, push a JSON payload onto a Redis list, and pop it from a worker thread.

Production failures happen in the gaps between happy-path steps:
1. **The Dual-Write Gap:** An API handler commits a task to PostgreSQL and crashes—or Redis rejects a connection—before enqueuing the message. The task now sits permanently orphaned in the database with no worker ever notified.
2. **Split-Brain Stale Workers:** Worker A claims a task, pauses (GC stall, CPU starvation, or network partition), and exceeds its lease timeout. A supervisor re-queues the task and Worker B runs it cleanly. Ten seconds later, Worker A wakes up and overwrites Worker B's authoritative result with stale data.
3. **Unmeasured Optimizations & Telemetry Hazards:** Developers frequently guess at bottlenecks—rewriting brokers or adding complex batching—without proving where time actually goes, or attach `task_id` / `tenant_id` to Prometheus metrics and silently melt their monitoring stack via cardinality explosion.

**TaskForge** began as a working asynchronous task platform (`v0.8.0-phase8-freeze` at `da27047`). Phase 9 (`G9.0`–`G9.5`) existed for a single engineering goal: **prove what actually breaks, measure where time actually goes, harden the platform with bounded operational complexity, reject optimizations unsupported by data, and ship a verification package any engineer can reproduce locally.**

---

## 2. System Architecture

TaskForge is structured as a **modular monolith control plane and worker execution engine** (`src/taskforge/`) backed by PostgreSQL 16 and Redis 7, plus Prometheus and Grafana for observability ([`d1_system_architecture.mmd`](../whitepaper/diagrams/d1_system_architecture.mmd)).

- **API Control Plane (`src/taskforge/main.py`):** FastAPI application layering `CorrelationIdMiddleware`, Redis-backed `RateLimitMiddleware`, `PrometheusHttpMiddleware`, Bearer API key authentication (`tf_live_...`), and scope-enforced REST routes (`/api/v1/tasks`, `/api/v1/operator/*`, `/api/v1/workers`, `/health/live`, `/health/ready`, `/metrics`).
- **Authoritative State Store (PostgreSQL 16):** Single source of truth across `tasks`, `outbox_events`, `task_attempts`, `task_results`, `task_events`, and `api_keys`.
- **Transactional Outbox (`OutboxPublisherWorker`):** Polls eligible `PENDING` rows (`available_at <= now`, `FOR UPDATE SKIP LOCKED`, batch `50`), publishes to Redis, and records `PUBLISHED` timestamps without dual-write windows.
- **Priority Broker (Redis 7 ZSET + Lua):** Sorted sets `ready_queue` and `in_flight` driven by server-side atomic scripts (`LUA_ENQUEUE`, `LUA_DEQUEUE`, `LUA_ACK`, `LUA_RECLAIM`).
- **Execution & Fencing Plane (`WorkerDispatcher`, `LeaseManager`, `Reaper`):** Concurrent `WorkerDispatcher` using `ZPOPMAX` and event-driven `threading.Event` slot-release signaling, paired with PostgreSQL row-locked leases (`lease_token`, `lease_expires_at`), background heartbeat renewal, and two-stage expired-lease recovery.
- **Bounded Observability:** 22 registered Prometheus metrics behind finite whitelist normalizers, `GaugeSnapshotCache` (`5.0s` single-flight refresh over a dedicated telemetry engine), and `ReadinessProbeCoordinator` (`max_workers=2`, `1.0s` timeout).

---

## 3. The Engineering Challenges

Hardening TaskForge uncovered six concrete engineering challenges that shaped the Phase 9 design:

1. **Durable Submission Across Broker Outages:** If Redis goes down during burst ingestion, API calls must still accept durable tasks (within PostgreSQL capacity) and recover automatically once Redis returns without dropping events or crashing worker threads.
2. **Stale-Worker Fencing vs. Duplicate Message Suppression:** Redelivering an already completed task from Redis is harmless if suppressed cleanly (`TERMINAL_DUPLICATE`). Redelivering an *in-flight* task after a lease expiry requires cryptographic `lease_token` validation on every database write path so a zombie worker cannot corrupt a replacement worker's state.
3. **Database Connection Pool Saturation & Telemetry Contention:** Under load, naive `/metrics` scrapes checking out connections from the primary `QueuePool` compete directly with task execution transactions and worsen outages.
4. **Rate-Limiting Without Client Clock Skew:** Implementing a sliding-window rate limiter using application wall-clock timestamps across processes risks quota drift and sub-millisecond Sorted Set member collisions.
5. **The "Idle Worker Paradox":** Early Phase 9 profiling showed tasks sitting in Redis for tens of seconds while worker threads sat idle—requiring causal attribution rather than blind tuning.
6. **Measurement & Decomposition Rigor:** Several subtle bugs surfaced only when forcing exact mathematical reconciliation across pipeline timestamps and running real external renderers/linters against documentation artifacts.

---

## 4. Experiments and Findings (G9.0 – G9.4)

### 4.1 G9.0 Baseline Profiling (`da27047`)
Baseline HTTP-ingress load tests (`W-01` `noop` `n=500, c=10`, `W-02` `compute_prime` `n=200, c=4`, `W-03` `io_simulation` `n=200, c=10`) established starting throughput of **`10.66 TPS`**, **`7.53 TPS`**, and **`10.96 TPS`**, with end-to-end $p50$ latencies of `22,823.5 ms`, `14,540.2 ms`, and `5,271.1 ms`.

*What G9.0 established:* Quantified unoptimized baseline turnaround. *What it did NOT establish:* Why tasks spent up to `22.5s` waiting in queue under burst ingestion.

### 4.2 G9.3 Stage 1B & 2A — Diagnosing the "Idle Worker Paradox"
To isolate where time went without slowing execution, we built a non-blocking 16-event diagnostic profiler (background flusher thread, 65,536-event bounded buffer). Two findings immediately reshaped our roadmap:
- **Stage 1B Attribution:** `93.65%–98.49%` of task turnaround was queue dwell (`9.25–22.46s`), while isolated Python handler execution took only `0.013–6.239 ms`. Crucially, timeline reconstruction proved worker occupancy was `0` for **`58.4%–78.7%` of queue-backed time**.
- **Stage 2A Dispatcher Scheduling Diagnostics (18 interleaved trials):** `74.5%–89.9%` of worker slot completions happened while `WorkerDispatcher` was blocked inside its fixed `200 ms` polling sleep. Sleep lag (`22.9–24.0 ms` $p50$) accounted for **`75%–82%` of total slot replenishment delay** (`28.6–32.6 ms`), while Redis dequeue RTT took only `1.47–4.26 ms`.
- *Honest Nuance on Hypothesis H1:* Stage 2A had pre-registered heuristic thresholds ($T_{\text{sleep-lag}} > 50\,\text{ms}$ and $R_{\text{starve}} > 30\%$) that were **not** numerically exceeded (`24 ms` sleep lag, `25%–29%` starvation). Rather than claiming H1 passed blindly, we documented the partial threshold miss while recognizing that eliminating `75%–82%` of avoidable sleep lag directly justified testing event-driven wake-ups.

### 4.3 G9.3 Stage 2B & 2C — Capacity Wake-Up & Target-Lock (`a89858b`)
Adding a `threading.Event` capacity wake-up signal (`_capacity_event.set()` inside `_task_wrapper`'s `finally:` block, paired with clear-before-check race safety) yielded across multi-trial campaigns:
- **Sleep lag ($p50$):** Collapsed by **`94.94%–98.66%`** (`23.85–25.82 ms -> 0.35–1.21 ms`).
- **Per-slot replenishment lag ($p50$):** Dropped by **`79.2%–91.0%`** (`29.36–33.99 ms -> 2.64–7.14 ms`).
- **Worker saturation:** Jumped **`+33.5 to +42.6` percentage points** (reaching **`90.9%–91.3%`** saturation at `c=4`).
- **Phase 2C Target-Lock & Concurrency Ceiling:** At `c=4`, Redis server `EVALSHA` execution took only `49.7–61.9 us` (`INFO commandstats`) and dispatcher duty cycle was only `7.5%`. However, pushing single-dispatcher concurrency to **`c=20`** exposed a hard serialization boundary: dispatcher duty cycle spiked to **`74.0%`** and worker saturation dropped to **`24.8%`** (the exact onset inflection between `c=10` and `c=20` remains unmeasured).

### 4.4 G9.4 Operational Resilience Drills
- **G9.4.1 Outbox Burst Drain (AC-9.4.2):** Drained `1,000` pre-seeded outbox events across 3 trials at a mean rate of **`276.10 tasks/s`** (`3.622s` mean across 20 batches of 50). Duty-cycle decomposition showed `42.25%` active SQL/Redis work and `57.75%` configured `100ms` poll sleep.
- **G9.4.2 Pool Saturation (AC-9.4.3):** Forced $2\times$ contention (`30` concurrent callers vs. `15` total pool capacity `5 + 10 overflow`, `5.0s` timeout). Bounded rejections occurred at `<= 5.011s`, pool returned to `0 active / 0 overflow` within `< 0.1s` after load ceased, with `0` leaked connections.
- **G9.4.3 Multi-Worker Fencing (AC-9.4.4):** Injected `5` stale-token write attempts across all 5 guarded repository operations (`complete_task_success`, `complete_task_failure`, `record_result`, `record_attempt`, `update_status`): **`5/5` rejected**, **`0` stale commits accepted**, `taskforge_fencing_rejections_total` incremented by `5`.
- **G9.4.4 Broker Outage Recovery (AC-9.4.5):** Used an un-mocked OS-level TCP socket proxy (`RealSocketTCPProxy`) to inject real `WinError 10054` / `10061` transport failures for `5.71s`. All `50/50` events stayed `PENDING` in PostgreSQL, in-process threads backed off (`<= 5.0s`) without crashing or restarting, and `50/50` tasks finished `SUCCESS` upon restoration (`0` durable event loss).

---

## 5. Architectural Decisions

| Decision | Choice Adopted | Alternative Rejected | Evidence / Rationale |
| :--- | :--- | :--- | :--- |
| **Authoritative State** (`ADR-002`) | PostgreSQL 16 (`tasks`, `outbox_events`) | Redis as primary state store | Redis memory/AOF does not provide relational row locks (`SELECT ... FOR UPDATE`), `SQLSTATE 23505` idempotency constraints, or multi-table atomic transactions across tasks, attempts, results, and outbox rows. |
| **Queue & Execution Dual Path** (`ADR-003`, `ADR-004`) | Custom Redis Lua ZSET (`RedisBroker`) + `WorkerDispatcher` alongside Celery (`celery_app`) | Relying solely on Celery's opaque broker internals | Custom Lua gave exact disjoint priority score bands `score = (priority * 10^12) - seq` via `ZPOPMAX`, per-delivery `delivery_token` visibility fencing (`LUA_ACK`), zero-daemon Windows developer ergonomics, and fine-grained wake-up signaling (`threading.Event`), while sharing `execute_task()`. |
| **Dispatcher Optimization** (G9.3 Phase 2B/2C) | Event-driven `threading.Event` capacity wake-up | Batch dequeue (`ZPOPMAX N > 1`) | At `c <= 10`, single-item dequeue already keeps workers `91.3%` saturated with `7.5%` dispatcher duty cycle. Batch prefetching increases crash blast-radius and partial-lease fencing complexity for zero measurable gain in the operating envelope. |
| **Observability Budget** (G9.2 / G9.4 Option A) | Finite label whitelists + `GaugeSnapshotCache` (`5s` TTL) on isolated engine (`28`-series G9.4 pool+lease runtime ceiling) | Dynamic labels (`task_id`, `worker_id`) or synchronous per-scrape queries on primary pool | Prevents Prometheus cardinality explosion and eliminates `/metrics` scrape interference with worker DB checkout latency under saturation. |

---

## 6. Measured Results (G9.5 Canonical Public Benchmarks)

To give external reviewers reproducible numbers on a frozen codebase, Gate G9.5 Stage 2 executed three locked workloads (`W-01`, `W-02`, `W-03`) in disposable isolated PostgreSQL/Redis namespaces (`benchmarks/public/run_public_benchmarks.py`, Run `fc557f14`, clean harness commit `1d4a76c`, evidenced in `2a14600`):

- **Environment:** Single-host Windows 11 (`AMD64`, `12` logical cores, `15.65 GB` RAM), Python `3.12.10`, PostgreSQL `16.15`, Redis `7.4.12`, `concurrency=4`, `0.05s` outbox poll interval.
- **Measurement Contract:** Excludes the first `10%` warmup tasks from percentiles and steady-state window throughput ($N_{\text{measured}} / \Delta t_{\text{window}}$), while separately reporting whole-run completion (`600/600` total seeded tasks succeeded, `0` failed, `0` retried).

| Workload ID | Task Type | Seeded / Warmup / Measured | Steady-State Throughput | Outbox Dwell Mean | Queue Dwell Mean | Worker Execution / Lease Tenure Mean | End-to-End ($p50$ / $p95$ / $p99$ / Mean) |
| :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **`W-01`** | `noop` (framework overhead) | 300 / 30 / 270 | **`44.78 tasks/s`** | `955.318 ms` | `3,084.507 ms` | `52.310 ms` | `4,063.9` / `6,756.8` / `7,003.2` / **`4,092.136 ms`** |
| **`W-02`** | `cpu_bound` (prime sieve $n=500$) | 200 / 20 / 180 | **`44.59 tasks/s`** | `599.911 ms` | `2,173.850 ms` | `53.526 ms` | `2,842.9` / `4,607.8` / `4,743.1` / **`2,827.287 ms`** |
| **`W-03`** | `io_bound` ($50\,\text{ms}$ blocking sleep) | 100 / 10 / 90 | **`28.82 tasks/s`** | `316.172 ms` | `1,876.566 ms` | `103.291 ms` | `2,309.3` / `3,555.8` / `3,675.5` / **`2,296.029 ms`** |

Visual comparisons generated directly from `benchmarks/public/benchmark_results.json`:

- Throughput Comparison: [`benchmarks/public/charts/throughput_comparison.svg`](../../benchmarks/public/charts/throughput_comparison.svg)
- Tail Latency Percentiles: [`benchmarks/public/charts/latency_percentiles.svg`](../../benchmarks/public/charts/latency_percentiles.svg)
- Causal Stage Decomposition: [`benchmarks/public/charts/latency_decomposition.svg`](../../benchmarks/public/charts/latency_decomposition.svg)

Every individual observation across all `540` measured tasks reconciles identically ($L_{\text{outbox}} + L_{\text{queue}} + L_{\text{worker}} \equiv L_{\text{e2e}}$, `max_per_task_discrepancy_ms = 0.000000 ms`). Crucially, **`W-01` and `W-02` achieve nearly identical throughput** because ~52–53 ms of PostgreSQL lease acquisition and terminal result commit overhead dominates `< 0.15 ms` of Python prime-sieve compute.

---

## 7. Failure Analysis and Trade-Offs

Treating verification as adversarial engineering uncovered real defects before final freeze:

1. **Latency Decomposition Mismatch (Caught in G9.5 Stage 2 Review):** Our initial benchmark script timed `handler_execution_ms` from `TaskAttempt.duration_ms` (which timed only the Python callable in memory), omitting the ~100–140 ms spent in post-handler PostgreSQL lease validation and `complete_task_success()` commit overhead. Redefining the stage causally as **Worker Execution / Lease Tenure** ($t_{\text{completed}} - t_{\text{started}}$) eliminated the missing time and enforced exact `0.000000 ms` per-task reconciliation.
2. **Rate Limiter Clock Skew & Sub-Millisecond Collisions (Caught in G9.1 Review):** Early sorted-set sliding windows using Python timestamps could drop concurrent requests inside the same millisecond or drift across clients. Updating `LUA_RATE_LIMIT` to read authoritative `redis.call('TIME')` and store unique `{sec}:{usec}:{req_id}` members fixed both issues (`68d1e3c`).
3. **Negative Pool Overflow Gauges & Scrape Contention (Caught in G9.4.2):** SQLAlchemy's `pool.overflow()` returns negative numbers when the base pool is under-utilized; clamping `max(0, raw_overflow)` and caching backend queries inside `GaugeSnapshotCache` prevented corrupted Prometheus gauges.
4. **D4 Sequence Diagram Parse Defect (Caught in G9.5 Stage 3 via `mmdc` v12.2.0):** A bare semicolon (`;`) inside a Mermaid `Note over` statement caused `mermaid` v12.3.0 to reject `d4_outbox_broker_recovery.mmd` with a parse error—demonstrating why automated file-existence checks are never a substitute for executing the actual renderer.
5. **Descriptive vs. Statistically Conclusive Experiments:** G9.3 CPU-time and profiler-overhead measurements used $N=3$ paired trials; we explicitly framed those as descriptive characterizations rather than statistical equivalence proofs.

---

## 8. Reliability Guarantees

> **TaskForge provides durable task submission through PostgreSQL transactions and a transactional outbox. Queue delivery is at-least-once. Worker ownership is protected by leases and fencing tokens. Task completion is persisted transactionally, and duplicate delivery is handled through idempotent execution and authoritative task state.**

To maintain strict engineering credibility, our claims fall into three explicit buckets:

| Category | Exact Claims Supported by Code & Evidence |
| :--- | :--- |
| **Measured Performance** | `W-01` `44.78 tasks/s`, `W-02` `44.59 tasks/s`, `W-03` `28.82 tasks/s` under `c=4` on local Windows 11 (`PostgreSQL 16.15`, `Redis 7.4.12`, Run `fc557f14`); outbox drain mean `276.10 tasks/s`; G9.3 `74.5%–89.4%` slot-replenishment latency drop. |
| **Verified in Reported Drills** | Stale-worker lease fencing across all 5 guarded DB write paths (`5/5` rejected); `5.71s` real OS TCP Redis interruption & automatic thread survival (`0` lost events); `TERMINAL_DUPLICATE` suppression on redelivery; `SQLSTATE 23505` ingress deduplication; `2x` DB pool contention timeout bounds (`<= 5.011s`, `0` leaks); exact `41/41` demo and `540/540` benchmark task/event/result identity reconciliation. |
| **NOT Established / Explicitly Unclaimed** | **Universal exactly-once execution**, **exactly-once external side effects**, **zero duplicate handler execution** (an in-flight crash before DB commit will re-invoke the handler after lease expiry), **unlimited horizontal scalability**, and **multi-region / multi-node cluster fault tolerance**. |

---

## 9. Operational Limitations

1. **Single-Host Local Benchmark Scope:** All public benchmark numbers reflect a single Windows 11 machine (`12` cores, `15.65 GB` RAM) running local containers. Network RTT, disk fsync latency, and Linux container scheduling in cloud deployments will materially change absolute throughput and latency.
2. **Dispatcher Serialization Boundary (`c=20`):** Single-dispatcher `WorkerDispatcher` execution stays efficient through `c=10`, but saturates at `c=20` (`74.0%` dispatcher duty cycle). Production deployments must scale horizontally via multiple worker containers (`c <= 10` per process).
3. **Fail-Closed Rate Limiter Coupling:** `RateLimitMiddleware` fails closed (`HTTP 503`) when Redis is down to protect PostgreSQL from unbounded traffic spikes—meaning Redis outages temporarily block API task ingestion even though PostgreSQL and `OutboxPublisherWorker` preserve previously queued work.
4. **Polling Outbox Floor & `LUA_ACK` Scan:** Outbox polling (`50ms–500ms`) sets a dwell latency floor under light load, and `LUA_ACK` uses `ZRANGE in_flight 0 -1` (linear in active `in_flight` queue depth).
5. **Tested Outage Duration:** Outage recovery was verified for a `5.71s` single-node Redis socket reset; multi-hour broker outages generating hundreds of thousands of `PENDING` outbox rows, PostgreSQL primary failover, and disk exhaustion remain untested.

---

## 10. What I Would Improve Next

Rather than adding speculative microservices or distributed consensus layers, the measured Phase 9 data points to five focused, high-leverage improvements:

1. **Event-Driven Outbox Wake-Up (`PostgreSQL LISTEN/NOTIFY` + Poll Fallback):** G9.4.1 showed `57.75%` of outbox drain time was idle polling sleep (`100ms` interval). Adding `LISTEN/NOTIFY` or an in-process wake latch while retaining periodic polling as a missed-signal safety net would cut light-load outbox dwell without changing durability semantics.
2. **Profile & Characterize the `10 < c <= 20` Dispatcher Knee + Multi-Process Worker Fleet Testing:** Pinpoint the exact concurrency inflection point and benchmark horizontal multi-dispatcher scaling across separate OS processes/containers under PostgreSQL contention.
3. **`O(1)` In-Flight Delivery Index for `LUA_ACK`:** Replace the `ZRANGE in_flight 0 -1` scan with a Redis Hash mapping `{task_id}:{delivery_token}` $\to$ serialized envelope member so `LUA_ACK` remains constant-time regardless of cluster concurrency.
4. **Linux Containerized CI Benchmark & Multi-Hour Outbox Soak Drills:** Run `run_public_benchmarks.py` inside Linux Docker Compose on CI hardware and test PostgreSQL primary restart + multi-minute backlog drain pacing to establish true Linux production capacity profiles.
5. **Provision G9.4 PromQL Alerts Directly in Alertmanager Config:** Add the G9.4 runbook alert rules (`taskforge_outbox_lag_seconds`, `taskforge_db_pool_checkout_timeouts_total`, `taskforge_fencing_rejections_total`) alongside the existing five rules in `infrastructure/prometheus/alerting_rules.yml`.
