# Gate G9.4 — Operational Resilience & Reliability Hardening: Scoping & Measurement Contract

**Milestone:** Gate G9.4 — Operational Resilience & Reliability Hardening (Scoping & Protocol Lock)  
**Author:** TaskForge Platform Hardening Team  
**Date:** 2026-11-11  
**Status:** Formally Amended Scoping & Measurement Contract  
**Authorized Production Base Commit:** `a89858b73f8488e05cbcc2393a61f3080ffc7a38`  
**G9.3 Closure Commit:** `9422505`  
**Frozen Baseline Tag:** `v0.8.0-phase8-freeze` (`da27047f56fe2eec75acdb8502f505542cf61202`)  
**Workload Definitions Hash (SHA-256):** `04f2306538868a41514c19f0958e04bf0b0586b3a83989e115231a32e50f0110`  
**Scope Boundary:** Strictly Scoping & Measurement Contract Design. Zero production source-code modifications. Zero connection-pool modifications. Zero outbox publisher semantic changes. Zero modification to retry, lease, fencing, or delivery semantics.

---

## 1. Executive Summary & Objective

Following the formal closure of Gate G9.3, TaskForge's dispatcher scheduling efficiency is hardened in production:
- Event-driven capacity wake-up is active by default (`a89858b`), reducing sleep lag by $95\% - 99\%$ and queue starvation by $86\% - 94\%$.
- Batch dequeue was formally rejected for the production operating envelope ($c \le 10$).
- Sequential single-element priority dequeue is locked.

Gate G9.4 shifts focus from scheduling optimization to **Operational Resilience & Reliability Hardening** under dependency stress, burst backlog, and resource constraints. The objectives of Gate G9.4 are:
1. **Outbox Publisher Throughput & Cadence Profiling:** G9.3 established that eligible outbox lag ($T_{\text{outbox-lag}} = 71.78 - 145.13\,\text{ms}$) is consistent with a substantial contribution from publisher polling cadence ($100\,\text{ms}$) and batch-claim operations. G9.4 will baseline publisher backlog drain capacity under a controlled 1,000-task burst.
2. **Database Connection-Pool Resilience:** Characterize PostgreSQL connection pool checkout latency, active occupancy, and saturation behavior under $2\times$ contention (30 concurrent callers against pool capacity 15).
3. **Lease Telemetry & Fault-Injection Verification:** Implement G9.2-compatible Prometheus telemetry for lease ownership transitions and verify that stale-worker fencing, Redis transient outages, and dependency degradations fail safely and recover cleanly.

---

## 2. Scope Boundaries & Explicit Non-Goals

### 2.1 Permitted Workstreams (Staged Execution Order):
1. **G9.4.0 — Scope & Protocol Lock (Active Milestone):**
   - Establish executable acceptance criteria, finite-label telemetry specifications, failure topologies, and measurement contracts.
2. **G9.4.1 — Outbox Publisher Profiling:**
   - Execute a diagnostic baseline measuring eligible outbox lag across W-01, W-02, W-03, and evaluate publisher drain rate under a 1,000-task burst backlog. (Strictly diagnostic; zero production changes).
3. **G9.4.2 — Connection-Pool Telemetry & Saturation Drills:**
   - Instrument connection pool metrics adhering to the G9.2 cardinality contract, and execute a $2\times$ saturation drill (30 callers against capacity 15).
4. **G9.4.3 — Lease Telemetry & Fault Injection Drills:**
   - Instrument lease renewal and fencing metrics; execute stale worker fencing drills, 5-second Redis outages, and database degradation scenarios.
5. **G9.4.4 — Milestone Synthesis & Closure:**
   - Compile the comprehensive G9.4 Operational Resilience Report.

### 2.2 Explicit Exclusions & Locked Boundaries:
- **Zero Production Source Code Modifications During Scoping:** Production code remains locked at `a89858b`.
- **Zero Outbox Optimization During Scoping:** Publisher polling interval ($0.1\,\text{s}$) and batch size ($50$) remain unchanged in production.
- **Zero Connection-Pool Modifications During Scoping:** Pool configuration remains locked until empirically characterized.
- **Authoritative PostgreSQL State:** PostgreSQL remains the sole authoritative source of truth for task state and lease ownership.
- **At-Least-Once Delivery Preservation:** At-least-once delivery semantics remain locked; fencing protects state transitions, not universal duplicate execution prevention.
- **Phase 8 Frozen Tag:** `v0.8.0-phase8-freeze` (`da27047`) remains untouched.

---

## 3. Failure-Mode Taxonomy & Injected Fault Topologies

| Failure Mode | Component | Trigger & Injected Topology | Architectural Vulnerability | Target Resilience & Recovery Boundary |
| :--- | :--- | :--- | :--- | :--- |
| **F-01: Outbox Ingestion Backlog** | Outbox Publisher | Ingestion burst (1,000 tasks pre-inserted while publisher paused) | Backlog accumulation; tasks experience outbox dwell before broker insertion | Measure batch claim throughput and drain time; identify publisher duty cycle and bottleneck |
| **F-02: Connection Pool Exhaustion** | Database Engine | 30 concurrent threads (API submissions and worker lease commits) against pool capacity 15 (`pool_size=5, max_overflow=10`, `pool_timeout=5.0s`) | `QueuePool` exhaustion; threads block or time out; potential thread hanging | Bounded checkout wait; clear fast-fail or graceful timeout; recovery within 5.0s after load ceases; zero hung worker threads |
| **F-03A: Stale Worker Network Stall** | Lease Manager | Worker process pauses (SIGSTOP / thread sleep) exceeding heartbeat lease ($30\,\text{s}$); replacement worker claims task and completes it; original worker resumes and attempts commit | Stale worker commits state after ownership reassigned | Authoritative PostgreSQL conditional write (`WHERE lease_token = :token`) rejects stale commit; 0 state corruptions |
| **F-03B: Worker Partition from PostgreSQL** | Lease Manager | Worker loses TCP connectivity to PostgreSQL while Redis remains reachable; lease expires; worker attempts recovery | Worker attempts uncoordinated actions without authoritative state check | Worker enters degraded state, drops leases, and refuses ACK without DB confirmation |
| **F-04: Redis Transient Disconnect** | Broker Client | Redis is paused/restarted for 5 seconds affecting both Outbox Publisher and Worker Dispatcher client paths | Dispatcher/publisher enter tight exception loop or crash; unacked deliveries leaked | Bounded reconnect retry backoff; zero durable outbox events lost; unacked tasks recovered by PostgreSQL; clean recovery without restart |
| **F-05: Database Transient Degradation** | Persistence Layer | Slow query lock contention artificially injected on task rows (1-2s query stall) | Worker thread pool blocked on DB transactions; in-flight tasks approach lease timeout | Lease extension keeps active tasks protected; readiness probe marks service unready; recovery once lock clears |

---

## 4. G9.2-Compatible Telemetry & Finite Cardinality Contract

All new operational metrics implemented in G9.4 must strictly comply with the finite-label and cardinality contract established in G9.2:

### 4.1 Telemetry Metric Definitions
| Metric Name | Type | Description | Allowed Labels & Finite Values | Histogram Buckets (s) |
| :--- | :---: | :--- | :--- | :--- |
| `taskforge_db_pool_active_connections` | Gauge | Currently checked-out active connections | `pool_name` (`primary`) | N/A |
| `taskforge_db_pool_overflow_connections` | Gauge | Currently allocated overflow connections | `pool_name` (`primary`) | N/A |
| `taskforge_db_pool_checkout_duration_seconds` | Histogram | Latency waiting to acquire a connection from pool | `pool_name` (`primary`) | `[0.001, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1.0, 2.5, 5.0]` |
| `taskforge_db_pool_checkout_timeouts_total` | Counter | Total connection pool checkout timeout exceptions | `pool_name` (`primary`) | N/A |
| `taskforge_outbox_claim_duration_seconds` | Histogram | Duration of batch outbox claim query | `status` (`success`, `error`) | `[0.001, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1.0]` |
| `taskforge_outbox_lag_seconds` | Histogram | Duration from task commit to outbox claim | None (single global series) | `[0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]` |
| `taskforge_outbox_publications_total` | Counter | Total tasks published to Redis from outbox | `status` (`published`, `failed`) | N/A |
| `taskforge_lease_renewals_total` | Counter | Total worker lease extension attempts | `status` (`success`, `failed`) | N/A |
| `taskforge_fencing_rejections_total` | Counter | Stale worker commits rejected by fencing token | `stage` (`heartbeat`, `terminal_commit`) | N/A |

### 4.2 Cardinality & Failure-Isolation Rules:
1. **Unbounded Labels Strictly Prohibited:** No `task_id`, `worker_id`, `delivery_token`, `correlation_id`, or raw exception message may ever appear in Prometheus metric labels.
2. **Finite Series Budget:** Total metric series added across G9.4 must not exceed **25 series**.
3. **Failure Isolation:** Telemetry recording failures must be caught and logged; a metric collection or export error must **never** fail or roll back a business transaction or worker task.

---

## 5. Executable Acceptance Criteria (Gate G9.4)

| Criterion ID | Dimension | Executable Test Contract & Acceptance Conditions | Verification Method |
| :--- | :--- | :--- | :--- |
| **AC-9.4.1** | **Outbox Lag Baseline** | Quantify eligible outbox lag distribution ($p50/p95/p99$) across W-01, W-02, and W-03 under baseline production dispatcher (`a89858b`). Must report both outbox lag and pure Redis enqueue round-trip. | Dedicated 3-trial outbox profiling campaign across all 3 workloads |
| **AC-9.4.2** | **Backlog Drain Contract** | 1. **Initial Backlog:** Exactly 1,000 eligible outbox events pre-inserted into PostgreSQL while publisher is paused.<br>2. **Quiescent Arrival:** Zero new tasks arrive during drain.<br>3. **Boundaries:** Measurement starts upon publisher unpause, ends when 0 pending records remain.<br>4. **Calculations:** Total drain time, drain rate ($R_{\text{drain}} = 1000 / \Delta t$), and publication rate per batch cycle ($50\,\text{tasks/batch}$) across 3 independent trials. | 1,000-task burst backlog drain harness with per-batch cadence tracking |
| **AC-9.4.3** | **Pool Saturation Contract** | 1. **Configuration:** SQLAlchemy `pool_size=5`, `max_overflow=10` (total capacity = 15), `pool_timeout=5.0s`.<br>2. **Contention:** 30 concurrent threads ($2\times$ saturation) executing mixed API submissions and worker lease transactions.<br>3. **Acceptance Conditions:** No indefinite thread or request blocking; all checkout wait times or rejections remain within configured `pool_timeout` bounds; system recovers within 5.0s after load ceases; underlying worker thread lifecycle verified (0 hung threads). | Synthetic pool saturation test with thread lifecycle verification |
| **AC-9.4.4** | **Fencing Correctness Contract** | **Exact Contract:** For every injected stale-worker scenario, a worker holding an expired or superseded lease token must be unable to commit an authoritative task-state transition or completion result. All stale commit attempts must be rejected by the fencing mechanism. Duplicate handler execution may occur under at-least-once delivery and must not be represented as universally prevented. External side effects require handler-level idempotency or deduplication.<br>**Required Measurements:**<br>- Number of stale-worker commit attempts.<br>- Number of stale-worker commits accepted (must be 0).<br>- Number of fencing rejections (must equal attempts).<br>- Final authoritative task state (valid terminal state from authorized worker).<br>- Number of handler invocations, tracked separately from accepted commits. | Fault-injection drill simulating worker pause, lease expiration, replacement, and resume |
| **AC-9.4.5** | **Redis Outage & Recovery Contract** | 1. **Injected Outage:** 5-second simulated Redis disconnect affecting both publisher and dispatcher client paths.<br>2. **Acceptance Conditions:**<br>- Zero acknowledged durable outbox events silently lost.<br>- Unpublished outbox events remain recoverable (`status='PENDING'`).<br>- Republished events do not corrupt task state when delivery is duplicated.<br>- Workers do not accept stale lease ownership after reconnect.<br>- Clean recovery without restarting application processes.<br>- Retry behavior bounded by backoff sleep without tight spinning.<br>- PostgreSQL remains authoritative. | Redis transient network partition drill with state verification |
| **AC-9.4.6** | **Telemetry Export** | All G9.4 metrics exported via `/metrics`, conform to finite-label cardinality contract, and scrape cleanly without business transaction interference. | Automated Prometheus scrape integration tests |
| **AC-9.4.7** | **Explicit Regression Gate** | **Exact Contract:** The complete regression suite, including the frozen Phase 8 baseline and all applicable G9.1–G9.4 tests, must pass with zero failures and zero skips unless an explicitly reviewed exception is documented. The report must include the exact test count, command, duration, and environment. | `pytest tests/` across full test suite |

---

## 6. Baseline Provenance & Scope Lock Declaration

- **Production Commit:** `a89858b73f8488e05cbcc2393a61f3080ffc7a38` remains completely unchanged.
- **Phase 8 Frozen Tag:** `v0.8.0-phase8-freeze` (`da27047f56fe2eec75acdb8502f505542cf61202`) remains untouched.
- **G9.4.0 Status:** Documentation and measurement contract design only. No production implementation authorized.
- **Next Step:** Proceed to **G9.4.1 (Outbox Publisher Diagnostic Profiling)** following user review of this amended protocol.
