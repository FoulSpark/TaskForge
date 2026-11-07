# TaskForge Performance Benchmarking

This document details the performance characteristics, benchmarking methodology, and experimental results for the TaskForge asynchronous task processing platform.

---

## 1. Benchmarking Methodology

To ensure statistical rigor and eliminate fluke measurements, TaskForge enforces a strict **Five-Run Methodology**:

1. **Commit Pinning:** All benchmark runs record the exact Git commit hash and environment specifications.
2. **Warmup Phase:** The application processes a small warmup batch before benchmark timing begins.
3. **Repeated Trials:** Each workload profile is executed for 5 consecutive independent trials.
4. **Metrics Capture:**
   - Ingestion Throughput (req/s)
   - Task Completion Throughput (tasks/s)
   - Latency Percentiles: P50, P95, P99
   - Error Rate (%)
5. **Statistical Aggregation:** Raw metrics are exported to JSON in `benchmarks/raw/` and summarized via median, mean, and standard deviation in `benchmarks/reports/`.
6. **No Result Replacement:** Historical benchmarks are permanently preserved in the repository.

---

## 2. Workload Profiles

| Profile | Total Tasks | Target Concurrency | Purpose |
|---|---:|---:|---|
| **Smoke** | 100 | 5 | Quick validation and regression checks |
| **Baseline** | 1,000 | 20 | Standard benchmark for comparative analysis |
| **Moderate** | 5,000 | 50 | Sustained load testing and connection pool verification |
| **Heavy** | 10,000 | 100 | Stress testing, queue saturation, and backpressure checks |

---

## 3. Benchmark Execution Commands

```powershell
# Run Baseline Benchmark (5 trials)
python benchmarks/runner/run_benchmarks.py --url http://127.0.0.1:8000 --profile baseline

# Run Smoke Benchmark (5 trials)
python benchmarks/runner/run_benchmarks.py --url http://127.0.0.1:8000 --profile smoke

# Benchmark CPU-Intensive Prime Computation
python benchmarks/runner/run_benchmarks.py --url http://127.0.0.1:8000 --profile baseline --task-type compute_prime
```

---

## 4. Benchmark Results Summary

### Baseline Workload (1,000 Tasks × 5 Trials)

| Metric | Trial 1 | Trial 2 | Trial 3 | Trial 4 | Trial 5 | Median | Mean | Stdev |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| **Throughput (req/s)** | 312.4 | 318.1 | 315.6 | 309.8 | 314.2 | **314.2** | 314.0 | 3.12 |
| **P50 Latency (ms)** | 2.1 | 2.0 | 2.1 | 2.2 | 2.0 | **2.1** | 2.08 | 0.08 |
| **P95 Latency (ms)** | 4.8 | 4.6 | 4.7 | 5.1 | 4.7 | **4.7** | 4.78 | 0.19 |
| **P99 Latency (ms)** | 8.2 | 7.9 | 8.1 | 8.6 | 8.0 | **8.1** | 8.16 | 0.27 |
| **Error Count** | 0 | 0 | 0 | 0 | 0 | **0** | 0 | 0.00 |

### Worker Scaling Comparison (1,000 Tasks)

| Worker Configuration | Completion Throughput | Speedup Factor | Queue Drain Time |
|---|---:|---:|---:|
| **1 Worker Process** | 42.1 tasks/sec | 1.00x | 23.7 s |
| **2 Worker Processes** | 78.4 tasks/sec | 1.86x | 12.7 s |
| **4 Worker Processes** | 148.9 tasks/sec | 3.53x | 6.7 s |
| **8 Worker Processes** | 212.3 tasks/sec | 5.04x | 4.7 s |

---

## 5. Limitations & Caveats

1. **Hardware Dependent:** Measurements reflect local host hardware (AMD64 8-Core processor). Production deployments on cloud virtual machines will vary depending on network and storage IOPS.
2. **SQLite vs PostgreSQL:** In-memory SQLite measurements exhibit near-instant write latencies but lack true concurrent write scaling compared to PostgreSQL multi-connection pools.

