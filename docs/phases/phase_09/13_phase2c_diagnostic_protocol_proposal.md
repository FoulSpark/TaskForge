# Gate G9.3 — Stage 2 Phase 2C Diagnostic Rebaselining & Sequential Dequeue Investigation Protocol

**Milestone:** Gate G9.3 — Stage 2 Phase 2C (Diagnostic Rebaselining & Target-Lock Investigation)  
**Author:** TaskForge Platform Hardening Team  
**Date:** 2026-11-10  
**Status:** Formally Authorized Diagnostic Protocol (Amended per Review Decisions)  
**Authorized Production Base Commit:** `a89858b73f8488e05cbcc2393a61f3080ffc7a38`  
**Frozen Baseline Tag:** `v0.8.0-phase8-freeze` (`da27047f56fe2eec75acdb8502f505542cf61202`)  
**Workload Definitions Hash (SHA-256):** `04f2306538868a41514c19f0958e04bf0b0586b3a83989e115231a32e50f0110`  
**Scope Boundary:** Strictly Diagnostic Rebaselining. Zero batch dequeue implementation. Zero changes to queue contracts, lease fencing, worker concurrency, or PostgreSQL/Redis authoritative state boundaries.

---

## 1. Executive Summary & Investigation Objective

With the formal approval and production adoption of Gate G9.3 Stage 2 Phase 2B (`a89858b`), the dispatcher scheduling sleep lag was reduced by $94.9\% - 98.7\%$ (dropping $T_{\text{sleep-lag}}$ from $\sim 24-26\,\text{ms}$ down to $0.35-1.21\,\text{ms}$), and queue starvation collapsed by $86.4\% - 93.5\%$ (dropping $R_{\text{starve}}$ from $25\%-31\%$ down to $1.7\%-4.0\%$).

Because dispatcher scheduling dynamics have fundamentally changed, historical Stage 1B and Phase 2A timing distributions can no longer serve as an accurate bottleneck attribution profile. 

The objective of Phase 2C is **not** to implement batch dequeue. Rather, Phase 2C is an authorized **diagnostic target-lock investigation** to:
1. Rebaseline the production-adopted dispatcher (`enable_capacity_wakeup=True`) under identical workload conditions.
2. Measure client-observed Redis dequeue round-trip ($T_{\text{dequeue-exec}}$) and slot replenishment latency ($T_{\text{replenish-lag}}$) using unambiguous matching and cohort analysis.
3. Capture Redis commandstats snapshots before and after every trial to decouple client-observed latency from Redis server-side execution time.
4. Calculate dispatcher active-duty ratio ($U_{\text{disp}}$) strictly within queue-positive observation windows alongside whole-process CPU utilization.
5. Determine whether sequential single-element dequeue limits sustained throughput under standard and elevated concurrency ($c = 4, 10, 20$).
6. Provide an evidence-grounded target-lock recommendation on whether batch dequeue warrants an isolated experiment or must be rejected.

---

## 2. Pre-Registered Hypotheses & Corrected Decision Rules

The diagnostic investigation evaluates three hypotheses against pre-registered, mathematically sound decision criteria:

### Hypothesis H2-A: Sequential Dequeue Bottleneck
- **Mechanism:** With polling sleep lag eliminated, client-observed Redis round-trip latency ($T_{\text{dequeue-exec}}$) becomes the primary bottleneck in replenishment delay ($T_{\text{replenish-lag}}$), delaying worker slot replenishment when slots become free and bounding throughput while work is available.
- **Pre-Registered Decision Criteria:** Supported if:
  1. Per-interval matched dequeue contribution ratio ($r_i = T_{\text{dequeue-exec}, i} / T_{\text{replenish}, i}$) has a median ($p50$) **$> 40\%$**.
  2. Sustained dispatch throughput is demonstrably constrained by sequential broker round-trips while queue depth remains positive ($Q(t) > 0$).
- **Outcome if Supported:** Justifies defining an isolated, bounded batch-dequeue experiment for formal architectural review.

### Hypothesis H2-B: Broker Latency Non-Dominant / Non-Bottleneck
- **Mechanism:** Redis single-element dequeue round-trip ($1.5 - 4.3\,\text{ms}$) is negligible relative to worker execution time and database transactions. Sequential dequeue supplies tasks faster than workers consume them, leaving dispatcher active-duty low and worker occupancy high ($W \approx c$).
- **Pre-Registered Decision Criteria:** Supported if:
  1. Per-interval matched dequeue contribution ratio $r_i$ has a median ($p50$) **$< 20\%$**.
  2. Worker occupancy remains saturated ($W(t) \ge c$ for **$> 75\%$** of queue-positive time).
  3. Dispatcher active-duty ratio $U_{\text{disp}}$ remains **$< 25\%$**.
- **Outcome if Supported:** **Batch dequeue is rejected** within the tested workload, concurrency, and infrastructure envelope. (This avoids speculative complexity, preserving single-element lease and priority contracts).

### Hypothesis H2-C: Concurrency-Scaled Serialization
- **Mechanism:** Sequential dequeue is efficient at low concurrency ($c=4$), but introduces serialized queuing delay at higher concurrency ($c=10, 20$) because $N$ released slots require $N$ sequential round-trips to replenish.
- **Pre-Registered Decision Criteria:** Supported if:
  1. Replenishment latency increases materially with concurrency from $c=4$ to $c=20$.
  2. Dispatcher active-duty ratio $U_{\text{disp}}$ increases significantly at $c=20$ (approaching thread saturation).
- **Outcome if Supported:** Supports investigating sequential dequeue specifically under high-concurrency operating regimes.

*(Note: These hypotheses are evaluated with full trial-level distributions and variability; H2-B and H2-C are not mutually exclusive across different concurrency levels).*

---

## 3. Rigorous Measurement Contract & Mathematical Definitions

### 3.1 Unambiguous Replenishment Matching & Cohort Analysis
To prevent distortion from concurrent worker completions, replenishment latency is calculated through two distinct, mathematically sound mechanisms:

1. **Unambiguous 1-to-1 Matched Replenishment Interval ($T_{\text{replenish}, i}$):**
   - Defined strictly when a single `worker_slot_released` event is followed by a `dispatcher_task_submitted` event without an intervening release.
   - Per-interval dequeue contribution ratio:
     $$r_i = \frac{T_{\text{dequeue-exec}, i}}{T_{\text{replenish}, i}}$$
   - Percentiles ($p50, p95, p99$) are computed over the distribution of $r_i$, strictly avoiding un-additive percentile division.
2. **Concurrent Release Cohort Analysis:**
   - When multiple workers release slots within an overlapping interval before the dispatcher completes replenishment, the releases form a **release cohort** $C_k = \{e_1, e_2, \dots, e_m\}$.
   - **Time to First Replacement:**
     $$T_{\text{first-replenish}} = t_{\text{dispatcher\_task\_submitted}, 1} - \min_{e \in C_k}(t_{\text{slot\_released}})$$
   - **Time to Full Cohort Replenishment:**
     $$T_{\text{full-replenish}} = t_{\text{dispatcher\_task\_submitted}, m} - \min_{e \in C_k}(t_{\text{slot\_released}})$$
   - This explicitly captures sequential queueing delays under concurrency without arbitrary 1-to-1 pairing.

### 3.2 Dispatcher Active-Duty Ratio ($U_{\text{disp}}$)
Dispatcher active-duty measures the fraction of time the dispatcher spends actively executing dequeue and loop operations during queue-positive periods:
$$U_{\text{disp}} = \frac{\sum_{j \in \text{QP}} (T_{\text{dequeue-exec}, j} + T_{\text{loop-overhead}, j})}{T_{\text{queue-positive-wall-clock}}}$$
- **Boundary Rules:**
  1. Only dispatcher intervals $(t_{\text{start}}, t_{\text{end}})$ that overlap queue-positive periods ($Q(t) > 0$) are included.
  2. Intervals outside queue-positive windows are strictly excluded.
  3. Non-overlapping execution intervals are enforced before summing.
  4. Both raw numerator (total active seconds) and raw denominator (queue-positive duration) are reported alongside the ratio.
  5. Process CPU time is reported separately via `time.process_time()`.

### 3.3 Redis Commandstats Attribution
To decouple client network/driver round-trip from Redis engine processing:
- Snapshots of `INFO commandstats` are captured immediately before and after each trial.
- Per-trial deltas are recorded for:
  - `cmdstat_evalsha` / `cmdstat_eval`: total calls, total microseconds (`usec`), and execution latency per call (`usec_per_call`).
  - Total Redis commands executed during the trial.
- Both client-observed round-trip ($T_{\text{dequeue-exec}}$) and Redis engine execution time are reported side-by-side.

---

## 4. Evaluation Matrix (18 Trials Total)

| Series | Workload | Tasks ($n$) | Concurrency ($c$) | Trials | Objective |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Series 1** | W-01 (`noop`) | 500 | 10 | 3 | Post-adoption baseline & consistency reference |
| | W-02 (`prime`, limit 1000) | 200 | 4 | 3 | Compute-heavy post-adoption baseline |
| | W-03 (`io`, 10 KB) | 200 | 10 | 3 | I/O-heavy post-adoption baseline |
| **Series 2** | W-01 (`noop`) | 500 | 4 | 3 | Concurrency scaling at low concurrency ($c=4$) |
| | W-01 (`noop`) | 500 | 10 | 3 | Concurrency scaling consistency check ($c=10$) |
| | W-01 (`noop`) | 500 | 20 | 3 | Concurrency scaling at high concurrency ($c=20$) |

Full infrastructure resets (PostgreSQL database truncation and Redis `FLUSHDB` on isolated test database/Redis instances) will occur between every trial.

---

## 5. Invariant Checks & Verification Rules

Every trial must strictly satisfy all operational invariants:
1. Reconstructed logical queue depth $Q(t) \ge 0$ at all sample points.
2. Reconstructed active worker occupancy $W(t) \ge 0$ and $W(t) \le c$ at all sample points.
3. Event completeness: $\sum \text{enqueues} = \sum \text{dequeues} = \sum \text{acks}$.
4. Zero duplicate task submissions.
5. Zero diagnostic event drops or unpersisted records.
6. Full regression test suite passing (235/235).

---

## 6. Target-Lock Recommendation Contract

At the completion of the 18-trial campaign, the findings will be evaluated against H2-A, H2-B, and H2-C:
- If **H2-B** is supported: Formally document that batch dequeue is not justified within the tested operating envelope, and close Stage 2 without further broker changes.
- If **H2-A** or **H2-C** is supported: Formally present the empirical constraint and submit a Phase 2C experimental design for user gate authorization.
