# Gate G9.4.1 Diagnostic Report — Outbox Publisher Profiling & Controlled Backlog Drain (Final Reconciliation)

**Milestone:** Gate G9.4.1 — Outbox Publisher Diagnostic Profiling  
**Author:** TaskForge Platform Hardening Team  
**Date:** 2026-11-11  
**Status:** Formally Reconciled Diagnostic Baseline & Attribution Report (Production Changes: NONE)  
**Authorized Production Base Commit:** `a89858b73f8488e05cbcc2393a61f3080ffc7a38`  
**G9.4 Scoping & Protocol Commit:** `ffe16af`  
**G9.4.1 Diagnostic Execution Commit:** `3fee428`  
**G9.4.1 Reconciliation Commit:** `f835287`  
**Frozen Baseline Tag:** `v0.8.0-phase8-freeze` (`da27047f56fe2eec75acdb8502f505542cf61202`)  
**Workload Definitions Hash (SHA-256):** `04f2306538868a41514c19f0958e04bf0b0586b3a83989e115231a32e50f0110`  
**Execution Environment:** Windows 11 AMD64, Python 3.12.10, PostgreSQL 16 (port 5432), Redis 7.4.12 (port 6379)  
**Full Regression Gate:** 235 / 235 passed (0 failures, 0 skips, duration 322.02s)

---

## 1. Executive Summary & Reconciliation Objectives

Per the authorized scoping contract (`ffe16af`) and the formal review findings for Gate G9.4.1, this final report reconciles all empirical measurements, micro-architectural timing decompositions, and Redis delivery-accounting verifications for the [`OutboxPublisherWorker`](file:///e:/TaskForge/src/taskforge/queue/outbox.py#L119-L149).

Gate G9.4.1 executed two distinct diagnostic campaigns:
1. **Campaign A (Existing Workload Baseline — AC-9.4.1):** 9 trials across frozen workloads W-01 ($n=500, c=10$), W-02 ($n=200, c=4$), and W-03 ($n=200, c=10$) under the production dispatcher (`a89858b`, `enable_capacity_wakeup=True`) and production publisher configuration (`poll_interval=0.1s`, `batch_size=50`).
2. **Campaign B (Controlled Backlog Drain — AC-9.4.2):** 3 independent trials draining a pre-inserted 1,000-task burst backlog with publisher initially paused and quiescent arrival ($0$ new task arrivals).

### Key Final Reconciled Findings:
1. **Publisher Burst-Drain Capacity ($R_{\text{drain}} = 276.10\,\text{tasks/s}$):** Under the tested quiescent 1,000-event burst workload, the publisher drained the backlog in $3.570 - 3.692\,\text{s}$ (mean $3.622\,\text{s}$), achieving an average drain throughput of $276.10\,\text{tasks/s}$.
2. **Qualified Capacity Comparison:** Draining at $276.10\,\text{tasks/s}$ is substantially faster than the historical worker execution capacity at concurrency 10 ($30 - 35\,\text{tasks/s}$). The publisher was not the limiting throughput stage in this experiment. This comparison is explicitly qualified: it reflects an isolated burst drain under quiescent arrival, and does not establish that the publisher cannot constrain throughput under sustained heavy arrival or severe database pool degradation.
3. **Statistical Scopes in Batch-Cycle Timing Reconciled:**
   - **Scope 1 (Trial 1 Single-Trial Cadence):** Draining 1,000 tasks executed 20 batches ($79.97\,\text{ms}$ active mean $\implies 1.600\,\text{s}$) and 19 inter-batch sleeps ($109.82\,\text{ms}$ sleep mean $\implies 2.087\,\text{s}$), totaling $3.686\,\text{s}$ batch processing span ($3.692\,\text{s}$ wall duration).
   - **Scope 2 (Cross-Trial Mean across all 60 Batches & 57 Sleeps):** Active processing averaged **$76.55\,\text{ms}$ per batch** ($20 \times 76.55\,\text{ms} = \mathbf{1.531\,\text{s}}$) and inter-batch sleep averaged **$109.71\,\text{ms}$** ($19 \times 109.71\,\text{ms} = \mathbf{2.085\,\text{s}}$). Active work ($1.531\,\text{s}$) plus inter-batch sleep ($2.085\,\text{s}$) equals **$3.616\,\text{s}$** batch span, matching the **$3.622\,\text{s}$** wall drain duration down to single-millisecond precision ($0.006\,\text{s}$ startup and polling boundary overhead).
4. **Exact Redis Task ID Membership Proven ($S_{\text{expected}} = S_{\text{actual ZSET}}$):**
   - In the initial campaign (`3fee428`), delivery was verified via application trace events and cardinality ($Z\text{card} = 1000$), but a raw member snapshot was not captured.
   - To provide undeniable evidence, a dedicated, isolated verification drill ([`verify_g941_redis_membership.py`](file:///e:/TaskForge/benchmarks/scenarios/g94/verify_g941_redis_membership.py)) was executed across 3 independent trials under the identical paused-dispatcher contract. An atomic read-only snapshot (`ZRANGE ... 0 -1`) decoded all 1,000 members, proving exact set equality ($S_{\text{expected}} = S_{\text{actual ZSET}}$): exactly 1,000 expected IDs, 1,000 actual Redis IDs, 0 missing, 0 unexpected, and 0 duplicates across all 3 trials.
5. **Outbox Commit Defined & Reconciled:** Outbox Commit ($T_{\text{outbox-commit}}$) is formally defined as the composite per-task interval from task $i$'s `redis_enqueue_complete` through enqueuing remaining batch items and committing `status='PUBLISHED'` in PostgreSQL (`outbox_published_commit`). Across Campaign A workloads, mean $p50$ commit duration spans **$75.43 - 218.78\,\text{ms}$** (W-01: $203.85\,\text{ms}$, W-02: $77.38\,\text{ms}$, W-03: $184.22\,\text{ms}$). Pure isolated Redis enqueue round-trip is negligible ($3.30 - 10.08\,\text{ms}$).
6. **Stage 1B Polling Interval Comparison Corrected:** The expected increase in polling wait from doubling `poll_interval` from $50\,\text{ms}$ to $100\,\text{ms}$ under a uniform arrival phase is $\Delta \mathbb{E}[T_{\text{wait}}] = \frac{100 - 50}{2} = \mathbf{25\,\text{ms}}$ (not 25–50 ms). This 25 ms shift provides a plausible methodological explanation for part of the observed difference ($+28.74\,\text{ms}$ in W-02), while the remainder reflects database commit queuing under 10 concurrent HTTP clients and host timing.
7. **Production Configuration Retained:** Retain `poll_interval=0.1s` and `batch_size=50` in production without modification.

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

### 3.2 Definition of Distinct Timing Metrics & Attribution Clarification
To prevent terminological ambiguity, distinct stage names and event boundaries are strictly maintained:
1. **Pure Redis Enqueue Duration ($T_{\text{redis-enq}}$):** $t_{\text{redis-enqueue-complete}, i} - t_{\text{redis-enqueue-start}, i}$. Measures client-observed round-trip latency for evaluating the Lua `ZADD` enqueue script for task $i$.
2. **Outbox Commit Duration ($T_{\text{outbox-commit}}$):** $t_{\text{outbox-published-commit}, i} - t_{\text{redis-enqueue-complete}, i}$. This is a **composite per-task interval**. It measures the elapsed duration from task $i$'s Redis enqueue return through the completion of remaining tasks in the batch plus the batch PostgreSQL transaction commit (`db.commit()`).
3. **Pure Batch Database Commit Duration ($T_{\text{batch-commit}}$):** In Campaign B, where no concurrent worker transactions exist, pure batch DB commit latency averages **$48.40 - 49.09\,\text{ms}$** ($p50$).
4. **Eligible Outbox Lag ($T_{\text{outbox-lag}}$):** $t_{\text{outbox-claim-start}, \text{batch}} - t_{\text{task-transaction-commit}, i}$. Measures dwell from task commit return until the start of the first outbox claim cycle that claimed it.

Across Campaign A workloads, mean $p50$ Outbox Commit is **$77.38\,\text{ms}$** for W-02 ($c=4$), **$184.22\,\text{ms}$** for W-03 ($c=10$), and **$203.85\,\text{ms}$** for W-01 ($c=10$). Commit durations are longer at $c=10$ due to database connection pool contention when 10 concurrent HTTP worker threads execute simultaneous task insertions.

### 3.3 Cross-Campaign Comparison: Stage 1B vs. Gate G9.4.1
The observed eligible outbox lag figures in G9.4.1 differ from the historical Stage 1B baseline:
- **W-01:** Stage 1B $110.91\,\text{ms}$ vs. G9.4.1 $190.50\,\text{ms}$ ($+79.59\,\text{ms}$)
- **W-02:** Stage 1B $71.78\,\text{ms}$ vs. G9.4.1 $100.52\,\text{ms}$ ($+28.74\,\text{ms}$)
- **W-03:** Stage 1B $145.13\,\text{ms}$ vs. G9.4.1 $193.66\,\text{ms}$ ($+48.53\,\text{ms}$)

**Methodological Differences:**
1. **Publisher Polling Interval:** Stage 1B configured `poll_interval = 0.05s` ($50\,\text{ms}$) in its benchmark harness. G9.4.1 strictly adhered to the authorized production contract `poll_interval = 0.1s` ($100\,\text{ms}$). Under uniform arrival, the expected wait before claim is:
   $$\mathbb{E}[T_{\text{wait}}] = \frac{\text{poll\_interval}}{2}$$
   For $50\,\text{ms}$ polling, $\mathbb{E}[T_{\text{wait}}] = 25\,\text{ms}$. For $100\,\text{ms}$ polling, $\mathbb{E}[T_{\text{wait}}] = 50\,\text{ms}$. The expected difference is **$25\,\text{ms}$**. This $25\,\text{ms}$ shift in polling wait provides a plausible methodological explanation for part of the observed difference (closely matching the $+28.74\,\text{ms}$ shift in W-02). It does not explain the entire shift in W-01 and W-03.
2. **Dispatcher Topology:** Stage 1B used the historical polling dispatcher without capacity wake-up. G9.4.1 used the production dispatcher (`a89858b`) with event-driven capacity wake-up. However, dispatcher scheduling does not govern outbox publication; additional lag in W-01 and W-03 is attributable to database commit queuing under 10 concurrent HTTP clients and host timing jitter.

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

Draining 1,000 tasks executed exactly 20 batches of 50 tasks each, containing **exactly 19 inter-batch sleep intervals** (no leading sleep before Batch 1; immediate termination after Batch 20 commit).

Below, both statistical scopes are clearly retained and distinguished:

#### Statistical Scope 1: Trial 1 Single-Trial Cadence (20 Batches, 19 Sleeps)
- **Batch Claim Mean:** $3.79\,\text{ms}$ ($\times 20 = 0.076\,\text{s}$)
- **Redis Enqueue Mean (50 items):** $25.67\,\text{ms}$ ($\times 20 = 0.513\,\text{s}$)
- **Batch Commit Mean:** $50.51\,\text{ms}$ ($\times 20 = 1.010\,\text{s}$)
- **Trial 1 Active Processing Subtotal:** **$79.97\,\text{ms}$** per batch ($\times 20 = \mathbf{1.599\,\text{s}}$)
- **Trial 1 Inter-Batch Sleep Mean:** **$109.82\,\text{ms}$** per sleep ($\times 19 = \mathbf{2.087\,\text{s}}$)
- **Trial 1 Batch Span:** $1.599\,\text{s} + 2.087\,\text{s} = \mathbf{3.686\,\text{s}}$ (Harness wall duration: $3.692\,\text{s}$)

#### Statistical Scope 2: Cross-Trial Mean (Across All 60 Batches & 57 Sleeps)

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

#### Reconciliation Arithmetic:
$$\text{Active Time} = 20 \times 76.549\,\text{ms} = 1530.98\,\text{ms} = \mathbf{1.531\,\text{s}}$$
$$\text{Sleep Time} = 19 \times 109.713\,\text{ms} = 2084.55\,\text{ms} = \mathbf{2.085\,\text{s}}$$
$$\text{Batch Span} = 1530.98\,\text{ms} + 2084.55\,\text{ms} = \mathbf{3615.53\,\text{ms}} = \mathbf{3.616\,\text{s}}$$
$$\text{Total Drain} = 3.616\,\text{s} + 0.006\,\text{s} = \mathbf{3.622\,\text{s}}$$

---

### 4.4 Exact Redis Task ID Membership Verification ($S_{\text{expected}} = S_{\text{actual ZSET}}$)

#### Qualification of Historical Campaign Evidence
In the initial Campaign B run (commit `3fee428`), exact set membership was inferred from application enqueue events (`redis_enqueue_complete`) and cardinality verification ($Z\text{card} = 1000$). However, a direct read-only Redis member dump (`ZRANGE ... 0 -1`) was not captured.

#### Isolated Redis ZSET Snapshot Audit
To close this evidence gap and prove exact membership without inferring it from application logs, a dedicated, isolated verification drill ([`benchmarks/scenarios/g94/verify_g941_redis_membership.py`](file:///e:/TaskForge/benchmarks/scenarios/g94/verify_g941_redis_membership.py)) was executed across 3 independent trials under the identical paused-dispatcher contract:
1. **Identifier Mapping:** In TaskForge models, each outbox event has `id` (primary key UUID), `aggregate_id = task_id`, and `payload['task_id'] = task_id`. This establishes a strict one-to-one mapping between outbox event IDs and task IDs.
2. **Snapshot Mechanism:** Immediately upon the publisher updating the 1,000th task to `PUBLISHED`, a read-only snapshot was captured via `redis_client.zrange(broker.queue_name, 0, -1)`. All member JSON payloads were decoded to extract the embedded `task_id`.
3. **Audit Results ([`g941_redis_membership_audit.json`](file:///e:/TaskForge/benchmarks/scenarios/g94/g941_redis_membership_audit.json)):**

| Verification Dimension | Audit Trial 1 | Audit Trial 2 | Audit Trial 3 | Required Condition | Audit Result |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **Initial unique eligible event IDs inserted** | 1,000 | 1,000 | 1,000 | 1,000 | **Verified** |
| **Durable `PUBLISHED` event IDs in PostgreSQL** | 1,000 | 1,000 | 1,000 | 1,000 | **Verified** |
| **Expected unique task IDs ($S_{\text{expected}}$)** | 1,000 | 1,000 | 1,000 | 1,000 | **Verified** |
| **Actual unique Redis task IDs ($S_{\text{actual ZSET}}$)** | 1,000 | 1,000 | 1,000 | 1,000 | **Verified** |
| **Missing expected task IDs** | **0** | **0** | **0** | **0** | **Passed** |
| **Unexpected task IDs** | **0** | **0** | **0** | **0** | **Passed** |
| **Duplicate task IDs** | **0** | **0** | **0** | **0** | **Passed** |
| **Final Redis depth ($Z\text{card}$)** | 1,000 | 1,000 | 1,000 | 1,000 | **Verified** |
| **Exact Set Equality ($S_{\text{expected}} = S_{\text{actual ZSET}}$)** | **TRUE** | **TRUE** | **TRUE** | **TRUE** | **Passed** |
| **Dispatcher State During Snapshot** | PAUSED | PAUSED | PAUSED | PAUSED | **Verified** |

SHA-256 of audit artifact (`g941_redis_membership_audit.json`): `47e88c9e49cb9dc1e9ada94e1e078a73103bfef339596e397ac81ea2b70c6436`.

---

## 5. Architectural Attribution & Target Lock

### 5.1 Qualified Capacity Finding
- **Experimental Finding:** Under the tested quiescent 1,000-event burst workload, the publisher drained backlog at **$276.10\,\text{tasks/s}$**, substantially faster than the historical worker execution throughput at concurrency 10 ($30 - 35\,\text{tasks/s}$). The publisher was not the limiting throughput stage in that experiment.
- **Qualifications:**
  - This is a comparison between measured publisher burst-drain capacity and worker processing capacity from a different campaign.
  - It does not establish that the publisher cannot become a constraint under sustained heavy arrival or complex workflow dependencies.
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

All 12 raw event traces from the primary campaign were persisted to `benchmarks/raw/g941/` and verified with $0$ drops and $0$ unpersisted records:

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

Protocol preparation for **Gate G9.4.2 (Database Connection-Pool Telemetry & Saturation Drills)** is established below. No production implementation is authorized prior to formal G9.4.1 closure.

### 7.1 Production Configuration Baseline (Commit `a89858b`)
The production SQLAlchemy pool configuration at `a89858b` is defined in [`src/taskforge/core/config.py`](file:///e:/TaskForge/src/taskforge/core/config.py#L20-L22) and instantiated in [`src/taskforge/database/session.py`](file:///e:/TaskForge/src/taskforge/database/session.py#L26-L33):
- `db_pool_size = 10`
- `db_max_overflow = 20`
- `db_pool_timeout = 30` (seconds)
- `pool_pre_ping = True`
- `pool_recycle = None` (unset)
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
- *Strictly documented as an experimental test fixture variant that will not modify production defaults.*

### 7.3 Metric Contract, Semantics & Cardinality Accounting (Compliant with G9.2)
1. `taskforge_db_pool_active_connections`: Gauge, label `pool_name` (`primary`). **Exact semantics:** Counts currently checked-out connections actively in use by a transaction/session from the pool (measured via SQLAlchemy `pool.checkedout()`), distinct from total open TCP connections.
2. `taskforge_db_pool_overflow_connections`: Gauge, label `pool_name` (`primary`). Counts allocated overflow connections beyond base pool size.
3. `taskforge_db_pool_checkout_duration_seconds`: Histogram, label `pool_name` (`primary`). Latency waiting to acquire a connection from the pool.
   - Buckets: `[0.001, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1.0, 2.5, 5.0]` (11 buckets + `+Inf` = 12 series)
   - Sum: 1 series
   - Count: 1 series
   - Base Histogram series: **14 series**.
4. `taskforge_db_pool_checkout_timeouts_total`: Counter, label `pool_name` (`primary`). Total connection pool checkout timeout exceptions.

**Runtime Exposition Budget Accounting:**
- Base Metric Series: $1 + 1 + 14 + 1 = \mathbf{17\text{ series}}$.
- Generated `_created` Timestamp Series (Prometheus Python Client):
  - Histogram `_created`: 1 series.
  - Counter `_created`: 1 series.
  - Runtime pool metric total: $17 + 2 = \mathbf{19\text{ series}}$.
- Adding planned G9.4.3 lease metrics ($2$ renewal series + $2$ fencing series $+ 2$ `_created` = $6$ series) yields **$25\text{ total runtime series}$**, strictly conforming to the $\le 25$ series ceiling.

### 7.4 Saturation Drill Execution Requirements (G9.4.2)
Prior to implementation authorization, the test protocol establishes:
1. **Contention:** 30 concurrent callers against the experimental 15-connection capacity.
2. **Barrier-Based Simultaneous Holding:** A barrier-synchronized workload holding connections to force genuine pool saturation rather than loose concurrent starts.
3. **Bounded Wait:** Configured checkout timeout of $5.0\,\text{s}$ strictly enforced.
4. **Observable Behavior:** Timeouts raise `TimeoutError` and increment `checkout_timeouts_total`.
5. **No Indefinite Blocking:** Zero hung worker threads.
6. **Recovery:** System returns to baseline active connection count within $5.0\,\text{s}$ after contention ceases.
7. **No Resource Leaks:** Zero orphaned connections or permanently exhausted pool slots.
8. **Telemetry Isolation:** Dedicated telemetry engine remains responsive and scrapes clean during full saturation.
