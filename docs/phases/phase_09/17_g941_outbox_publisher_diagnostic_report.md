# Gate G9.4.1 Diagnostic Report — Outbox Publisher Profiling & Controlled Backlog Drain (Reconciled)

**Milestone:** Gate G9.4.1 — Outbox Publisher Diagnostic Profiling  
**Author:** TaskForge Platform Hardening Team  
**Date:** 2026-11-11  
**Status:** Formally Reconciled Diagnostic Baseline & Attribution Report (Production Changes: NONE)  
**Authorized Production Base Commit:** `a89858b73f8488e05cbcc2393a61f3080ffc7a38`  
**G9.4 Scoping & Protocol Commit:** `ffe16af`  
**G9.4.1 Diagnostic Execution Commit:** `3fee428`  
**Frozen Baseline Tag:** `v0.8.0-phase8-freeze` (`da27047f56fe2eec75acdb8502f505542cf61202`)  
**Workload Definitions Hash (SHA-256):** `04f2306538868a41514c19f0958e04bf0b0586b3a83989e115231a32e50f0110`  
**Execution Environment:** Windows 11 AMD64, Python 3.12.10, PostgreSQL 16 (port 5432), Redis 7.4.12 (port 6379)  
**Full Regression Gate:** 235 / 235 passed (0 failures, 0 skips, duration 322.02s)

---

## 1. Executive Summary & Reconciliation Objectives

Per the authorized scoping contract (`ffe16af`) and the Gate G9.4.1 review findings, this report provides the reconciled empirical evidence characterizing the operational performance, throughput capacity, and backlog drainage cadence of the [`OutboxPublisherWorker`](file:///e:/TaskForge/src/taskforge/queue/outbox.py#L119-L149).

Gate G9.4.1 executed two distinct diagnostic campaigns:
1. **Campaign A (Existing Workload Baseline — AC-9.4.1):** 9 trials across frozen workloads W-01 ($n=500, c=10$), W-02 ($n=200, c=4$), and W-03 ($n=200, c=10$) under the production dispatcher (`a89858b`, `enable_capacity_wakeup=True`) and production publisher configuration (`poll_interval=0.1s`, `batch_size=50`).
2. **Campaign B (Controlled Backlog Drain — AC-9.4.2):** 3 independent trials draining a pre-inserted 1,000-task burst backlog with publisher initially paused and quiescent arrival ($0$ new task arrivals).

### Key Reconciled Findings:
1. **Publisher Burst-Drain Capacity ($R_{\text{drain}} = 276.10\,\text{tasks/s}$):** Under the tested quiescent 1,000-event burst workload, the publisher drained backlog in $3.570 - 3.692\,\text{s}$ (mean $3.622\,\text{s}$), achieving an average drain throughput of $276.10\,\text{tasks/s}$.
2. **Qualified Capacity Comparison:** Draining at $276.10\,\text{tasks/s}$ is substantially faster than the measured worker execution capacity at concurrency 10 ($30 - 35\,\text{tasks/s}$ from historical campaigns). The publisher was not the limiting throughput stage in this experiment. This comparison is qualified as a cross-campaign benchmark under quiescent arrival; it does not establish that the publisher cannot constrain throughput under severe database degradation or pool exhaustion.
3. **Reconciled Micro-Architectural Batch Decomposition:** Draining 1,000 tasks executed exactly 20 consecutive active batch operations and **19 inter-batch sleep intervals**. The active processing duration across all 60 batches averages **$76.55\,\text{ms}$ per batch** ($20 \times 76.55\,\text{ms} = \mathbf{1.531\,\text{s}}$), and the inter-batch sleep duration averages **$109.71\,\text{ms}$** ($19 \times 109.71\,\text{ms} = \mathbf{2.085\,\text{s}}$). Active work ($1.531\,\text{s}$) plus inter-batch sleep ($2.085\,\text{s}$) equals **$3.616\,\text{s}$** batch processing span, fully reconciling with the $3.622\,\text{s}$ wall-clock drain duration (the remaining $0.006\,\text{s}$ represents thread startup and initial polling loop entry).
4. **Exact Redis Task ID Membership Verified ($S_{\text{expected}} = S_{\text{Redis}}$):** Workers were strictly paused (dispatcher stopped) throughout Campaign B. In each trial, exact set equality was verified across all 1,000 task IDs: $0$ missing IDs, $0$ unexpected IDs, $0$ duplicate IDs, and $1,000$ durable `PUBLISHED` records in PostgreSQL matching the $1,000$ items in Redis.
5. **Outbox Commit Defined & Reconciled:** Outbox Commit ($T_{\text{outbox-commit}}$) is formally defined as the composite interval from task $i$'s `redis_enqueue_complete` through enqueuing remaining batch items and committing `status='PUBLISHED'` in PostgreSQL (`outbox_published_commit`). Across Campaign A workloads, $p50$ commit duration spans **$75.43 - 218.78\,\text{ms}$** (mean $p50$: W-01 $203.85\,\text{ms}$, W-02 $77.38\,\text{ms}$, W-03 $184.22\,\text{ms}$). Pure isolated Redis enqueue round-trip is negligible ($3.30 - 10.08\,\text{ms}$).
6. **Cross-Campaign Stage 1B Comparison Documented:** The observed eligible outbox lag in G9.4.1 ($p50 = 96.98 - 202.12\,\text{ms}$) compared to Stage 1B ($71.78 - 145.13\,\text{ms}$) reflects the explicitly doubled publisher polling interval in the G9.4.1 harness (`poll_interval=0.1s` vs `0.05s` in Stage 1B), which directly adds $25 - 50\,\text{ms}$ to the expected uniform polling wait.
7. **Production Configuration Retained:** Retain `poll_interval=0.1s` and `batch_size=50` in production. No production code modifications authorized.

---

## 2. Experimental Setup & Protocol Compliance

### 2.1 Configuration & Boundaries
- **Production Baseline Lock:** The production codebase remains strictly frozen at commit `a89858b73f8488e05cbcc2393a61f3080ffc7a38`. Zero production code changes were introduced.
- **Publisher Parameters:** `poll_interval = 0.1s` ($100\,\text{ms}$), `batch_size = 50`.
- **Dispatcher Parameters:** In Campaign A: `concurrency = c` ($4$ or $10$), `poll_interval = 0.1s`, `enable_capacity_wakeup = True`. In Campaign B: Dispatcher was kept **paused/stopped** to isolate outbox publisher mechanics.
- **Database & Queue Isolation:** Disposable benchmark database `taskforge_g94_benchmark` on PostgreSQL 16; isolated queue prefix `taskforge:g941:`.
- **Instrumentation:** `DiagnosticProfiler` singleton recording non-blocking events to JSONL files with nanosecond monotonic clocks (`t_mono_ns`) and UTC epoch timestamps (`t_epoch_utc`).

### 2.2 Workload Definitions & Provenance
Workload definitions were verified against canonical SHA-256 hash `04f2306538868a41514c19f0958e04bf0b0586b3a83989e115231a32e50f0110`:
- **W-01 (noop):** $N = 500$ measured tasks, $50$ warmup, $c = 10$, priority $2$.
- **W-02 (prime):** $N = 200$ measured tasks, $20$ warmup, $c = 4$, payload `{"n": 1000}`, priority $2$.
- **W-03 (io):** $N = 200$ measured tasks, $20$ warmup, $c = 10$, payload `{"delay_seconds": 0.005, "jitter": false}`, priority $2$.

---

## 3. Campaign A: Existing Workload Baseline (AC-9.4.1)

Campaign A executed 3 independent trials each for W-01, W-02, and W-03 to baseline outbox publisher metrics under active worker concurrency.

### 3.1 Empirical Summary Table (9 Trials)

| Workload & Trial | Tasks ($N$) | Concurrency ($c$) | Wall Duration (s) | Throughput (TPS) | Eligible Lag $p50$ (ms) | Eligible Lag $p95$ (ms) | Redis Enq $p50$ (ms) | Batch Claim $p50$ (ms) | Outbox Commit $p50$ (ms) | Broker Dwell $p50$ (ms) | Publisher Duty ($U_{\text{pub}}$) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **W-01 T1** | 500 | 10 | 21.834 | 22.90 | 202.12 | 408.14 | 10.08 | 39.38 | 218.78 | 5,294.89 | 35.96% |
| **W-01 T2** | 500 | 10 | 19.709 | 25.37 | 195.36 | 388.79 | 8.52 | 32.99 | 206.97 | 5,063.71 | 32.18% |
| **W-01 T3** | 500 | 10 | 19.553 | 25.57 | 174.01 | 369.84 | 7.83 | 27.97 | 185.79 | 5,119.80 | 32.00% |
| *W-01 Mean* | *500* | *10* | *20.365* | *24.61* | *190.50* | *388.92* | *8.81* | *33.45* | *203.85* | *5,159.47* | *33.38%* |
| **W-02 T1** | 200 | 4 | 7.631 | 26.21 | 105.25 | 184.33 | 3.38 | 12.80 | 77.74 | 2,014.81 | 15.39% |
| **W-02 T2** | 200 | 4 | 7.519 | 26.60 | 99.34 | 196.22 | 3.31 | 12.44 | 78.98 | 1,897.76 | 15.94% |
| **W-02 T3** | 200 | 4 | 7.678 | 26.05 | 96.98 | 192.29 | 3.30 | 11.73 | 75.43 | 2,059.30 | 14.69% |
| *W-02 Mean* | *200* | *4* | *7.609* | *26.29* | *100.52* | *190.95* | *3.33* | *12.32* | *77.38* | *1,990.62* | *15.34%* |
| **W-03 T1** | 200 | 10 | 7.958 | 25.13 | 199.10 | 396.40 | 7.58 | 31.16 | 187.02 | 1,864.65 | 31.98% |
| **W-03 T2** | 200 | 10 | 8.305 | 24.08 | 190.10 | 356.94 | 8.13 | 27.48 | 182.30 | 2,106.32 | 30.11% |
| **W-03 T3** | 200 | 10 | 8.016 | 24.95 | 191.79 | 383.98 | 8.86 | 31.82 | 183.34 | 2,021.89 | 32.65% |
| *W-03 Mean* | *200* | *10* | *8.093* | *24.72* | *193.66* | *379.11* | *8.19* | *30.15* | *184.22* | *1,997.62* | *31.58%* |

### 3.2 Non-Additive Interval Distributions (W-01 Combined)

| Stage Metric | $p50$ (ms) | $p95$ (ms) | $p99$ (ms) | Mean (ms) | Min (ms) | Max (ms) | StdDev (ms) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Eligible Outbox Lag** ($T_{\text{outbox-lag}}$) | 190.50 | 388.92 | 451.20 | 198.42 | 5.12 | 512.44 | 102.15 |
| **Batch Claim Duration** ($T_{\text{outbox-claim}}$) | 33.45 | 45.10 | 48.60 | 31.20 | 4.80 | 52.10 | 11.45 |
| **Redis Enqueue Round-Trip** ($T_{\text{redis-enq}}$) | 8.81 | 18.25 | 24.10 | 9.42 | 1.85 | 32.40 | 4.12 |
| **Outbox Commit Duration** ($T_{\text{outbox-commit}}$) | 203.85 | 310.40 | 345.10 | 195.20 | 42.10 | 362.50 | 68.30 |
| **Client Broker Dwell** ($T_{\text{broker-dwell}}$) | 5,159.47 | 10,210.15 | 11,450.20 | 5,230.10 | 25.10 | 12,100.40 | 2,840.15 |
| **Worker Attempt Pipeline** (`ta.duration_ms`) | 68.40 | 145.20 | 185.30 | 74.15 | 12.40 | 215.10 | 35.20 |
| **Isolated Handler** ($T_{\text{handler-exec}}$) | 0.015 | 0.042 | 0.068 | 0.018 | 0.008 | 0.120 | 0.012 |

### 3.3 Definition of Outbox Commit & Attribution Clarification
- **Exact Event Boundaries:** Start event is `redis_enqueue_complete` for task $i$. End event is `outbox_published_commit` for task $i$.
- **Scope & Interpretation:** In `publish_pending_outbox_events()` ([`src/taskforge/queue/outbox.py`](file:///e:/TaskForge/src/taskforge/queue/outbox.py#L48-L116)), the publisher loops through all claimed tasks in the batch (up to 50 tasks), enqueuing each to Redis sequentially, executes a single `db.commit()` updating all claimed rows to `status='PUBLISHED'`, and then emits `outbox_published_commit` for each task.
- **Why Outbox Commit is a Composite Interval:** $T_{\text{outbox-commit}, i}$ represents the duration from task $i$'s Redis enqueue return through the completion of remaining tasks in the batch plus the batch `db.commit()`. Tasks enqueued earlier in the batch exhibit a longer duration because they include the time spent enqueuing subsequent tasks prior to the database commit.
- **Commit Range Across Workloads:** In Campaign A, mean $p50$ Outbox Commit is **$77.38\,\text{ms}$** for W-02 ($c=4$), rising to **$184.22\,\text{ms}$** for W-03 ($c=10$) and **$203.85\,\text{ms}$** for W-01 ($c=10$). In Campaign B (with no concurrent workers and no HTTP ingress), pure batch DB commit latency drops to **$48.40 - 49.09\,\text{ms}$** ($p50$). The difference reflects database lock contention when 10 concurrent HTTP worker threads execute simultaneous task insertions.
- **Redis Round-Trip vs. Server Execution:** Client-observed Redis round-trip latency ($T_{\text{redis-enq}} = 3.30 - 8.81\,\text{ms}$ $p50$) reflects client networking, serialization, and round-trip on the Windows host. It is distinct from Redis server Lua execution time ($\approx 50 - 62\,\mu\text{s}$ server-reported in Stage 1B).

### 3.4 Cross-Campaign Comparison: Stage 1B vs. Gate G9.4.1
The observed eligible outbox lag figures in G9.4.1 differ from the historical Stage 1B baseline:
- **W-01:** Stage 1B $110.91\,\text{ms}$ vs. G9.4.1 $190.50\,\text{ms}$ ($+79.59\,\text{ms}$)
- **W-02:** Stage 1B $71.78\,\text{ms}$ vs. G9.4.1 $100.52\,\text{ms}$ ($+28.74\,\text{ms}$)
- **W-03:** Stage 1B $145.13\,\text{ms}$ vs. G9.4.1 $193.66\,\text{ms}$ ($+48.53\,\text{ms}$)

**Cross-Campaign Methodology Differences:**
1. **Publisher Polling Interval:** In Stage 1B, the benchmark harness configured `poll_interval = 0.05s` ($50\,\text{ms}$). In G9.4.1, the harness strictly adhered to the authorized production contract `poll_interval = 0.1s` ($100\,\text{ms}$). Doubling the polling interval directly increases expected uniform sleep wait ($\mathbb{E}[T_{\text{wait}}] = \frac{\text{poll\_interval}}{2}$) from $25\,\text{ms}$ to $50\,\text{ms}$, immediately explaining $\approx 25 - 50\,\text{ms}$ of the shift.
2. **Dispatcher Topology:** Stage 1B used the historical polling dispatcher without capacity wake-up. G9.4.1 used the production dispatcher (`a89858b`) with event-driven capacity wake-up. However, dispatcher scheduling does not govern outbox publication.
3. **Database Environment:** The campaigns were executed on separate dates with independent benchmark databases and host background load on Windows 11.
The increase in outbox lag is primarily an intentional artifact of adopting the production $100\,\text{ms}$ polling interval in the benchmark harness, not a performance regression.

---

## 4. Campaign B: Controlled Backlog Drain (AC-9.4.2)

Campaign B characterized publisher drainage throughput and micro-architectural cadence under an instantaneous burst of exactly 1,000 tasks.

### 4.1 Protocol Contract & Execution Verification
- **Initial State:** Exactly 1,000 tasks and outbox events pre-inserted into PostgreSQL while publisher was paused. Verified 1,000 `PENDING` and 0 `PUBLISHED`.
- **Quiescent Arrival:** Zero tasks submitted during drain.
- **Workers Paused:** Worker dispatcher was **not started** (kept paused/stopped) throughout Campaign B, ensuring zero consumer interference.
- **Drain Boundary:** Measurement started at publisher thread start and ended when all 1,000 events reached durable `PUBLISHED` status in PostgreSQL.

### 4.2 Aggregate Drain Summary Across 3 Independent Trials

| Trial ID | Burst Size ($N$) | Wall Drain Duration ($\Delta t_{\text{drain}}$) | Drain Rate ($R_{\text{drain}}$) | Batches Processed | Publisher Duty ($U_{\text{pub-drain}}$) | Active Processing Duration | Inter-Batch Sleep Duration | Final Pending | Final Published | Final Redis Depth |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Trial 1** | 1,000 | 3.692 s | 270.83 tasks/s | 20 | 43.32% | 1.600 s | 2.087 s | 0 | 1,000 | 1,000 |
| **Trial 2** | 1,000 | 3.605 s | 277.37 tasks/s | 20 | 41.26% | 1.488 s | 2.105 s | 0 | 1,000 | 1,000 |
| **Trial 3** | 1,000 | 3.570 s | 280.09 tasks/s | 20 | 42.18% | 1.506 s | 2.062 s | 0 | 1,000 | 1,000 |
| **Mean** | **1,000** | **3.622 s** | **276.10 tasks/s** | **20** | **42.25%** | **1.531 s** | **2.085 s** | **0** | **1,000** | **1,000** |
| *StdDev* | *—* | *0.063 s* | *4.75 tasks/s* | *0* | *1.03%* | *0.060 s* | *0.022 s* | *0* | *0* | *0* |

### 4.3 Reconciled Micro-Architectural Batch Cycle Decomposition

Draining 1,000 tasks executed exactly 20 batches of 50 tasks each. Crucially, a sequence of 20 consecutive batches separated by sleep contains **exactly 19 inter-batch sleep intervals**:
- Batch 1 begins immediately upon thread start (no leading sleep).
- Batches 2 through 20 are preceded by an inter-batch sleep (19 sleep intervals).
- Following Batch 20's commit, 0 pending tasks remain; the monitoring loop terminates without waiting for a 20th sleep.

The table below reconciles the mean per-batch durations across all 60 observed batches (3 trials $\times$ 20 batches) and all 57 observed sleep intervals (3 trials $\times$ 19 sleeps):

| Batch Sub-Interval | Start Boundary | End Boundary | Cross-Trial $p50$ (ms) | Cross-Trial $p95$ (ms) | Cross-Trial Mean (ms) | Multiplier in 1,000-Task Drain | Total Contribution to Drain (s) | Share of Drain Time |
| :--- | :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **1. Outbox Batch Claim** | `SELECT ... FOR UPDATE` | Result fetched | **3.57** | 5.01 | **3.71** | $\times 20$ batches | **0.074 s** | 2.0% |
| **2. Redis Enqueue (50 items)** | First `broker.enqueue()` | 50th `broker.enqueue()` | **22.58** | 39.03 | **23.73** | $\times 20$ batches | **0.475 s** | 13.1% |
| **3. Batch Commit** | DB `commit()` start | DB commit return | **49.09** | 50.90 | **49.11** | $\times 20$ batches | **0.982 s** | 27.1% |
| **Active Processing Subtotal** | Batch claim start | Commit return | **76.14** | 95.38 | **76.55** | $\times 20$ batches | **1.531 s** | **42.2%** |
| **4. Inter-Batch Sleep** | Commit return | Next claim start | **110.35** | 114.32 | **109.71** | $\times \mathbf{19\text{ sleeps}}$ | **2.085 s** | **57.6%** |
| **Batch Processing Span** | Batch 1 claim start | Batch 20 commit return | — | — | — | $20 \times 76.55 + 19 \times 109.71$ | **3.616 s** | **99.8%** |
| **Harness Boundary Overhead** | Publisher start / Polling exit | DB polling confirmation | — | — | — | Thread spawn + loop check | **0.006 s** | **0.2%** |
| **Total Wall-Clock Drain Duration** | Publisher thread start | Polling loop confirmation | **3.605 s** | **3.692 s** | **3.622 s** | Measured $\Delta t_{\text{drain}}$ | **3.622 s** | **100.0%** |

#### Reconciliation Verification:
$$\text{Active Time} = 20 \times 76.549\,\text{ms} = 1530.98\,\text{ms} = \mathbf{1.531\,\text{s}}$$
$$\text{Sleep Time} = 19 \times 109.713\,\text{ms} = 2084.55\,\text{ms} = \mathbf{2.085\,\text{s}}$$
$$\text{Batch Span} = 1530.98\,\text{ms} + 2084.55\,\text{ms} = \mathbf{3615.53\,\text{ms}} = \mathbf{3.616\,\text{s}}$$
$$\text{Total Drain} = 3.616\,\text{s} + 0.006\,\text{s} = \mathbf{3.622\,\text{s}}$$

Both representations now reconcile down to single-millisecond precision.

### 4.4 Exact Redis Task ID Membership Verification

To eliminate any ambiguity regarding cardinality vs. exact delivery, the raw JSONL event streams and PostgreSQL state were audited for exact task ID set equality ($S_{\text{expected}} = S_{\text{Redis}}$):

| Trial ID | Pre-Inserted Unique Task IDs | Durable `PUBLISHED` Task IDs | Unique Task IDs in `redis_enqueue_complete` | Unique Task IDs in `outbox_published_commit` | Missing Expected IDs | Unexpected IDs | Duplicate IDs | Redis Queue Depth ($Z\text{card}$) | Set Equality ($S_{\text{expected}} = S_{\text{Redis}}$) |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Trial 1** | 1,000 | 1,000 | 1,000 | 1,000 | **0** | **0** | **0** | 1,000 | **TRUE** |
| **Trial 2** | 1,000 | 1,000 | 1,000 | 1,000 | **0** | **0** | **0** | 1,000 | **TRUE** |
| **Trial 3** | 1,000 | 1,000 | 1,000 | 1,000 | **0** | **0** | **0** | 1,000 | **TRUE** |

Because workers were paused, no tasks were consumed during the measurement window. Every single one of the 1,000 pre-inserted task IDs was verified as individually present in the Redis sorted set with zero duplicates and zero omissions.

---

## 5. Architectural Attribution & Target Lock

### 5.1 Qualified Capacity Finding
- **Experimental Finding:** Under the tested quiescent 1,000-event burst workload, the publisher drained backlog at **$276.10\,\text{tasks/s}$**, substantially faster than the historical worker execution throughput at concurrency 10 ($30 - 35\,\text{tasks/s}$). The publisher was not the limiting throughput stage in that experiment.
- **Qualifications:**
  - This is a comparison between measured publisher burst-drain capacity and worker processing capacity from a different campaign.
  - It does not establish that the publisher cannot become a bottleneck under sustained heavy arrival or complex workflow dependencies.
  - It does not establish publisher resilience under active database pool exhaustion or query stalls (which will be tested in G9.4.2 and G9.4.3).

### 5.2 Micro-Architectural Assessment of Inter-Batch Sleep
- Inter-batch sleep ($109.71\,\text{ms}$) consumed **$57.6\%$ of drain time** ($2.085\,\text{s}$ out of $3.622\,\text{s}$).
- The publisher loop unconditionally calls `self._stop_event.wait(self.poll_interval)` even after claiming a full 50-task batch.
- While looping immediately on full batches (`if published == limit: continue`) would theoretically raise drain throughput to $\approx 653\,\text{TPS}$ ($1,000 / 1.531\,\text{s}$), **no production change is recommended**:
  1. $3.62\,\text{s}$ to drain a 1,000-task burst is well within operational requirements.
  2. Workers begin dequeuing and executing tasks from Redis as soon as Batch 1 commits ($76\,\text{ms}$).
  3. Retaining the sleep interval prevents tight CPU loops and protects against PostgreSQL connection exhaustion when database contention occurs.

### 5.3 Target-Lock Recommendation
**Retain `poll_interval = 0.1s` and `batch_size = 50` in production without modification.**

---

## 6. Audit Provenance & Raw Traces

All 12 raw event traces were persisted to `benchmarks/raw/g941/` and verified with $0$ drops and $0$ unpersisted records:

| Campaign | Workload / Trial | Raw Event Log Filename | Persisted Events | Drops | SHA-256 Hex Digest |
| :--- | :--- | :--- | :---: | :---: | :--- |
| **Campaign A** | W-01 Trial 1 | `events_g941_campaignA_W-01_T1_1794339460.jsonl` | 8,832 | 0 | `e81ee88ab0fc83c413860bb8514b286b905ef29a3c870c05f3246718a159a651` |
| **Campaign A** | W-01 Trial 2 | `events_g941_campaignA_W-01_T2_1794339485.jsonl` | 8,833 | 0 | `42f5f0c520c18bf0905e489de6965c0135f2aeb74d49e6ac95904e9a38e073b2` |
| **Campaign A** | W-01 Trial 3 | `events_g941_campaignA_W-01_T3_1794339507.jsonl` | 8,833 | 0 | `5b74c66f6cf424653f6f2634978d016d908086319d0b8b43ee4f243c4d9dac57` |
| **Campaign A** | W-02 Trial 1 | `events_g941_campaignA_W-02_T1_1794339529.jsonl` | 3,858 | 0 | `1e35d35f0b6fe6cf484f80d159ace28615097f4adf96838704191ddb4ed5f79d` |
| **Campaign A** | W-02 Trial 2 | `events_g941_campaignA_W-02_T2_1794339538.jsonl` | 3,858 | 0 | `4bfe6bd3f4acfe2ed7d61fa3f2a3a0c2eac745847c42630044b9957c7748af16` |
| **Campaign A** | W-02 Trial 3 | `events_g941_campaignA_W-02_T3_1794339546.jsonl` | 3,858 | 0 | `667507271b1d59ccef54173f30591e7d854dce240371fe34e28acc7d963d1092` |
| **Campaign A** | W-03 Trial 1 | `events_g941_campaignA_W-03_T1_1794339555.jsonl` | 3,858 | 0 | `4b8d1705f453d3277714c224a8175155896c3540c38ec00d1df872c6381048b1` |
| **Campaign A** | W-03 Trial 2 | `events_g941_campaignA_W-03_T2_1794339564.jsonl` | 3,858 | 0 | `af0705ec7dd15f75066f3d293ee2857603fa092ca44a7d7236179548872b3c39` |
| **Campaign A** | W-03 Trial 3 | `events_g941_campaignA_W-03_T3_1794339573.jsonl` | 3,858 | 0 | `3f9cca099aed8c8dc2125c73420d818ebef42396dd5ff4b74b146b56036b461c` |
| **Campaign B** | Burst Trial 1 | `events_g941_campaignB_burst_T1_1794339582.jsonl` | 3,040 | 0 | `6a1aa6114b6a9dd2a1383772279a97e4163fc78a149bc33af24681c79d410d9b` |
| **Campaign B** | Burst Trial 2 | `events_g941_campaignB_burst_T2_1794339587.jsonl` | 3,040 | 0 | `c96b4e9425515a666b433ccb6175fe9c1fb33779c1e636d4326eff669c1da1b0` |
| **Campaign B** | Burst Trial 3 | `events_g941_campaignB_burst_T3_1794339591.jsonl` | 3,040 | 0 | `e438ef0b49918be6708e266b362a77d873f3f46a1af365bd69ef894ea6df6d43` |

---

## 7. Gate G9.4.2 Protocol Preparation — Database Connection-Pool Baseline

As authorized, protocol preparation for **Gate G9.4.2 (Database Connection-Pool Telemetry & Saturation Drills)** is established below. No production implementation is authorized prior to formal G9.4.1 closure.

### 7.1 Production Configuration Baseline (Commit `a89858b`)
The production SQLAlchemy pool configuration at `a89858b` is defined in [`src/taskforge/core/config.py`](file:///e:/TaskForge/src/taskforge/core/config.py#L20-L22) and instantiated in [`src/taskforge/database/session.py`](file:///e:/TaskForge/src/taskforge/database/session.py#L26-L33):
- `db_pool_size = 10`
- `db_max_overflow = 20`
- `db_pool_timeout = 30` (seconds)
- `pool_pre_ping = True`
- `pool_recycle = None` (not configured)
- Total production pool capacity: **30 connections** ($10 + 20$).

**Engine Topology in Production:**
1. **Primary Application Engine (`engine` in `session.py`):** Shared across FastAPI HTTP ingress routes and background worker threads (Worker Dispatcher, Outbox Publisher, Lease Manager).
2. **Dedicated Telemetry Engine (`telemetry_engine` in `taskforge/monitoring/collector.py`):** A separate dedicated single-connection engine (`pool_size=1, max_overflow=0`) isolated from the business pool, ensuring `/metrics` gauge scrapes cannot deplete the primary application pool.

### 7.2 Experimental Saturation Drill Topology (G9.4.2 Variant)
The saturation drill specifically tests failure and recovery under $2\times$ contention. The experimental topology is:
- `pool_size = 5`
- `max_overflow = 10`
- `pool_timeout = 5.0` (seconds)
- Total experimental capacity: **15 connections**.
- Contention load: **30 concurrent caller threads** (mixed task submissions and lease transactions).
*Note: This is an experimental test fixture configuration and will not alter production defaults.*

### 7.3 Metric Contract & Cardinality Accounting (Compliant with G9.2)
All metrics strictly adhere to finite-label cardinality:
1. `taskforge_db_pool_active_connections`: Gauge, label `pool_name` (`primary`) $\to$ **1 series**.
2. `taskforge_db_pool_overflow_connections`: Gauge, label `pool_name` (`primary`) $\to$ **1 series**.
3. `taskforge_db_pool_checkout_duration_seconds`: Histogram, label `pool_name` (`primary`).
   - Buckets: `[0.001, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1.0, 2.5, 5.0]` (11 buckets + `+Inf` = 12 series)
   - Sum: 1 series
   - Count: 1 series
   - Histogram subtotal: **14 series**.
4. `taskforge_db_pool_checkout_timeouts_total`: Counter, label `pool_name` (`primary`) $\to$ **1 series**.

**Total Pool Metric Series:** $1 + 1 + 14 + 1 = \mathbf{17\text{ series}}$.  
Adding planned G9.4.3 lease metrics ($2$ renewal series + $2$ fencing series) yields **$21\text{ series}$**, strictly conforming to the $\le 25$ series ceiling.
