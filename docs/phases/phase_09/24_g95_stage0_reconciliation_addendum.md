# Gate G9.5 — Stage 0 Reconciliation Addendum

**Milestone:** Gate G9.5 — Public Architecture Package & Reproducible Verification Suite (Stage 0 Reconciliation)  
**Author:** TaskForge Platform Hardening Team  
**Date:** 2026-11-11  
**Status:** Stage 0 Reconciled — Stage 1 Authorization Ready  
**Authorized Production Base Commit:** `a89858b73f8488e05cbcc2393a61f3080ffc7a38` (Immutable & Preserved)  
**Frozen Baseline Tag:** `v0.8.0-phase8-freeze` (`da27047f56fe2eec75acdb8502f505542cf61202`, Preserved)  
**Reference Document:** [`docs/phases/phase_09/23_g95_stage0_protocol_and_provenance_lock.md`](file:///e:/TaskForge/docs/phases/phase_09/23_g95_stage0_protocol_and_provenance_lock.md)  

---

## 1. Executive Summary

This addendum directly resolves the three blocking corrections identified in the Gate G9.5 Stage 0 Formal Review:
1. **R2:** Reconciles the production dequeue direction (`ZPOPMAX`) and score encoding against the frozen codebase with exact line references.
2. **R3:** Reconciles the cardinality terminology and master allocation table, distinguishing net-new G9.4 additions from pre-existing G9.2 baselines.
3. **R4:** Formalizes the revised Stage 1 demo acceptance contract (D1–D12), separating stale-worker lease fencing from terminal duplicate suppression, and requiring unique disposable databases per run for concurrency safety.

---

## 2. Finding R2 — Source References for Dequeue Operation & Priority Ordering

A line-by-line inspection of the frozen production implementation at `a89858b` confirms the exact Lua script commands and score encoding.

### 2.1. Exact Code References in `src/taskforge/queue/broker.py`
1. **Priority Score Calculation (`LUA_ENQUEUE`, lines 55–56):**
   ```lua
   local score = (priority * 1000000000000) - seq
   redis.call('ZADD', queue, score, payload)
   ```
2. **Dequeue Operation (`LUA_DEQUEUE`, lines 68–71):**
   ```lua
   local res = redis.call('ZPOPMAX', queue, 1)
   if #res == 0 then
       return nil
   end
   ```

### 2.2. Mathematical Priority & FIFO Invariant
- **Priority Tiering:** Task priorities are integer values $p \in \{1, 2, 3, 4\}$ (Low = 1, Normal = 2, High = 3, Critical = 4).
  - Priority 4 base score: $4 \times 10^{12} = 4{,}000{,}000{,}000{,}000$
  - Priority 3 base score: $3 \times 10^{12} = 3{,}000{,}000{,}000{,}000$
  - Disjoint bands: For any adjacent tiers, $\min(\text{score}_p) = (p-1) \times 10^{12} + 1 > \max(\text{score}_{p-1}) = (p-1) \times 10^{12} - 1$.
- **Intra-Tier FIFO Ordering:** Within a given priority tier, tasks receive sequentially incrementing sequence numbers `seq` ($1, 2, 3, \dots$). Earlier tasks have smaller `seq`, and therefore have a **strictly higher score**:
  $$\text{score}(p, \text{seq}_1) > \text{score}(p, \text{seq}_2) \iff \text{seq}_1 < \text{seq}_2$$
- **Dequeue Direction:** Because `redis.call('ZPOPMAX', queue, 1)` pops the **maximum score**, it guarantees:
  1. Higher priority tiers are dequeued before lower priority tiers.
  2. Tasks within the same priority tier are dequeued in strict FIFO arrival order.

*Reconciliation Decision:* Any reference in Stage 0 drafts to `ZPOPMIN` was an erroneous transcription. The production codebase at `a89858b` executes `ZPOPMAX`. All whitepaper and demo documentation is updated to reference canonical `ZPOPMAX`.

---

## 3. Finding R3 — Master Cardinality Allocation Reconciliation

The telemetry terminology is reconciled to clearly distinguish net-new G9.4 series from pre-existing G9.2 series:

### 3.1. Reconciled Accounting Structure
```
Combined Runtime Exposition: 28 Series (Ceiling: 28)
├── Pre-Existing G9.2 Baseline Gauges: 3 Series
│   └── taskforge_db_pool_connections{state="active|overflow|idle"}
└── Net-New G9.4 Telemetry Additions: 25 Series (Ceiling: 25)
    ├── G9.4.2 Database Connection Pool: 19 Series
    │   ├── taskforge_db_pool_checkout_duration_seconds [11 buckets + sum + count] (13)
    │   ├── taskforge_db_pool_checkout_timeouts_total{pool_name="primary"} (2)
    │   ├── taskforge_db_pool_active_connections{pool_name="primary"} (2)
    │   └── taskforge_db_pool_overflow_connections{pool_name="primary"} (2)
    └── G9.4.3 Lease & Fencing Additions: 6 Series
        ├── taskforge_lease_renewals_total{status="success|failed"} (4)
        └── taskforge_fencing_rejections_total (2)
```

- **Net-New G9.4 Allocation:** Exactly **$19 + 6 = 25\text{ series}$** ($\le 25$).
- **Pre-Existing G9.2 Baseline:** Exactly **$3\text{ series}$** (pre-dates G9.4; not a G9.4 addition).
- **Total Combined Exposition:** Exactly **$25 + 3 = 28\text{ series}$** ($\le 28$).
- Zero new metrics or label modifications will be introduced in G9.5.

---

## 4. Finding R4 — Revised Stage 1 Demo Acceptance Contract (D1–D12)

### 4.1. Concurrency-Safe Database & Namespace Isolation
To prevent resource collisions during concurrent demo invocations:
- **Unique Disposable Database:** The demo generates a unique 8-character run identifier (`run_id = uuid.uuid4().hex[:8]`), provisions `taskforge_demo_<run_id>`, runs Alembic migrations, and drops the database upon exit.
- **Isolated Redis Namespace:** All Redis keys are scoped to `taskforge:demo:<run_id>:*`.
- **Ownership Verification:** Teardown strictly targets resources matching the current process `run_id`, refusing to touch any database or Redis key outside its ownership boundary.

### 4.2. Functional Separation: Lease Fencing vs. Terminal Duplicate Suppression
The demo separates and independently verifies two distinct reliability behaviors:
- **D7 (Actual Stale-Worker Lease Fencing):**
  1. Worker A claims Task 1 with lease token $T_A$.
  2. Worker A's lease expires; Worker B claims Task 1 with replacement token $T_B$.
  3. Worker A attempts a state transition using stale token $T_A$.
  4. PostgreSQL conditional atomic write (`WHERE lease_token = :token`) rejects Worker A's commit ($0$ rows updated).
  5. Worker B commits valid state using token $T_B$.
  6. Final PostgreSQL state reflects Worker B's authoritative result.
- **D8 (Terminal Duplicate Suppression):**
  1. A duplicate message for an already completed terminal task (`SUCCESS`) is enqueued to Redis.
  2. Dispatcher pulls the message; worker identifies task as already terminal in PostgreSQL.
  3. Execution is suppressed, the message is acknowledged as `TERMINAL_DUPLICATE`, and authoritative state is left uncorrupted.

### 4.3. Locked Stage 1 Demo Acceptance Table (D1–D12)
| ID | Requirement | Verification Method |
| :--- | :--- | :--- |
| **D1** | Environment validation & `--check` mode | Pre-flight check verifies PostgreSQL, Redis, Python, Docker without running demo |
| **D2** | Isolated disposable PostgreSQL & Redis resources | `taskforge_demo_<run_id>` and `taskforge:demo:<run_id>:*` |
| **D3** | Automatic database migrations | Alembic migration upgrade applied against disposable database |
| **D4** | Deterministic multi-priority workload | 30 tasks: 10 High, 10 Normal, 10 Low |
| **D5** | Verified priority ordering | High processed before Normal, Normal before Low under `ZPOPMAX` |
| **D6** | Real broker transport interruption & recovery | 3-second TCP socket severance; publisher & dispatcher back off and recover |
| **D7** | Actual stale lease-token rejection | Real PostgreSQL conditional write rejection; 0 stale state accepted |
| **D8** | Separate terminal duplicate suppression | Redelivery of terminal task suppressed as `TERMINAL_DUPLICATE` |
| **D9** | Initial-to-final identity reconciliation | Exact set equality: $E_{\text{initial}} = E_{\text{final}}$ and $T_{\text{initial}} = T_{\text{final}}$ |
| **D10** | 100% terminal completion | All seeded tasks reach terminal state; failures explicitly logged |
| **D11** | Safe, ownership-checked teardown | Drops only resources matching own `run_id` |
| **D12** | Progress reporting & summary result | Real-time terminal progress and machine-readable JSON summary |

---

## 5. Disposition

All three review findings (R2, R3, R4) are resolved with exact code citations and executable contracts. Stage 0 is complete. Authorization is requested to begin **Stage 1 (Local Demo Implementation)**.
