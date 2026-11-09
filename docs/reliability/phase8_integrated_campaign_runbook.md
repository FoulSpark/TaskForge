# TaskForge Phase 8: Final Integrated Reliability Campaign Runbook

**Document Version:** 1.0.0  
**Target Release Candidate:** Commit `86adefe` (building on baseline `9ab6f91`)  
**Target Freeze Tag:** `v0.8.0-phase8-freeze`  
**Execution Environment:** Windows 11 AMD64 / Python 3.12.10 (`.venv`) / PostgreSQL 16 / Redis 7

---

## 1. Objectives & Guarantees

This runbook defines the controlled, reproducible end-to-end reliability verification campaign required before formal Phase 8 release freeze.

The campaign proves TaskForge satisfies four core invariants under live infrastructure:
1. **Durable Intent:** PostgreSQL is authoritative. Zero accepted tasks are lost across crashes or outages.
2. **At-Least-Once Delivery with Monotonic State Fencing:** All delivery duplicates are fenced; handler bodies execute only once for tasks in terminal states.
3. **Fail-Closed Execution:** Transient broker or database partitions withhold delivery acknowledgements.
4. **Relational Parity:** Zero orphaned child records (`task_attempts`, `task_results`, `task_events`, `outbox_events`).

---

## 2. Environment Prerequisites & Version Capture

### 2.1 Service Verification
Both backing services must be running locally via Docker Desktop:
- PostgreSQL 16 on `localhost:5432` (`taskforge-postgres`)
- Redis 7 on `localhost:6379` (`taskforge-redis`)

```powershell
# 1. Capture OS & Environment
Get-ComputerInfo | Select-Object WindowsProductName, OsHardwareAbstractionLayer, WindowsVersion

# 2. Capture Python and Core Package Versions
.venv\Scripts\python -c @"
import sys, pydantic, sqlalchemy, redis, psycopg
print(f'Python: {sys.version}')
print(f'Pydantic: {pydantic.__version__}')
print(f'SQLAlchemy: {sqlalchemy.__version__}')
print(f'Redis-py: {redis.__version__}')
print(f'Psycopg: {psycopg.__version__}')
"@

# 3. Check Backing Services Health
docker ps --filter "name=taskforge" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
.venv\Scripts\python -c "import redis; r = redis.Redis(host='localhost', port=6379); print('Redis Ping:', r.ping())"
.venv\Scripts\python -c "import psycopg; conn = psycopg.connect('postgresql://taskforge:taskforge_dev@localhost:5432/taskforge_test'); print('PostgreSQL Connection:', conn.execute('SELECT 1').fetchone())"
```

**Acceptance Threshold:**
- PostgreSQL connection succeeds (`(1,)`).
- Redis returns `True` to ping.
- Python 3.12+ and all core platform packages verified.

---

## 3. Campaign Execution Stages

```mermaid
flowchart TD
    S1["Stage 1: Disposable DB Migration Drill"] --> S2["Stage 2: Full Regression Suite (161 Tests)"]
    S2 --> S3["Stage 3: Cross-System Failure Drills (S1 - S7)"]
    S3 --> S4["Stage 4: Idempotency Storm & Pool Saturation"]
    S4 --> S5["Stage 5: Relational Integrity & Persistence Audit"]
    S5 --> S6["Stage 6: Artifact Reconciliation & Tagging"]
```

---

### Stage 1: Disposable Database Migration Drill (Alembic)

> [!IMPORTANT]
> Never run destructive migration experiments against primary development or test benchmark state. Use a dedicated disposable database (`taskforge_migration_drill`).

```powershell
# Create disposable test database
.venv\Scripts\python -c @"
import psycopg
conn = psycopg.connect('postgresql://taskforge:taskforge_dev@localhost:5432/postgres', autocommit=True)
conn.execute('DROP DATABASE IF EXISTS taskforge_migration_drill')
conn.execute('CREATE DATABASE taskforge_migration_drill OWNER taskforge')
print('Created disposable database taskforge_migration_drill')
"@

# Set temporary connection string
$env:DATABASE_URL = "postgresql+psycopg://taskforge:taskforge_dev@localhost:5432/taskforge_migration_drill"

# 1. Run Alembic upgrade to head
.venv\Scripts\alembic upgrade head

# 2. Verify schema and table creation
.venv\Scripts\python -c @"
from sqlalchemy import create_engine, inspect
import os
engine = create_engine(os.environ['DATABASE_URL'])
tables = inspect(engine).get_table_names()
assert 'tasks' in tables and 'task_attempts' in tables and 'outbox_events' in tables
print('Migration upgrade head verified. Tables:', sorted(tables))
"@

# 3. Downgrade by 1 revision and re-upgrade
.venv\Scripts\alembic downgrade -1
.venv\Scripts\alembic upgrade head
Write-Host "Migration round-trip complete."

# Cleanup disposable database
$env:DATABASE_URL = "postgresql+psycopg://taskforge:taskforge_dev@localhost:5432/taskforge_test"
.venv\Scripts\python -c @"
import psycopg
conn = psycopg.connect('postgresql://taskforge:taskforge_dev@localhost:5432/postgres', autocommit=True)
conn.execute('DROP DATABASE taskforge_migration_drill')
print('Dropped disposable database taskforge_migration_drill')
"@
```

**Pass/Fail Criteria:**
- `alembic upgrade head` succeeds with 0 errors.
- Tables `tasks`, `task_attempts`, `task_results`, `task_events`, `outbox_events` created.
- `downgrade -1` and `upgrade head` complete cleanly without orphaned constraints.

---

### Stage 2: Full Regression Verification (161/161 Matrix)

Run all unit, security, integration, and reliability suites:

```powershell
# Unit & Security Suites (25 tests)
.venv\Scripts\pytest tests/unit/ tests/security/ -v --tb=short

# Integration Suite (67 tests)
.venv\Scripts\pytest tests/integration/ -v --tb=short

# Reliability Suite (69 tests)
.venv\Scripts\pytest tests/reliability/ -v --tb=short
```

**Pass/Fail Criteria:**
- Total 161 tests executed.
- 161 PASSED, 0 FAILED, 0 ERRORS.
- Code style: `ruff check src/ tests/ migrations/` returns `All checks passed!`.

---

### Stage 3: Cross-System Failure Validation Matrix (S1–S7)

Execute the 7 real-process failure and simulated dependency boundary drills:

```powershell
.venv\Scripts\pytest tests/reliability/test_cross_system_failures.py -v --tb=short
```

**Audited Scenarios & Thresholds:**
| Drill ID | Fault Injection Mechanism | Measured Thresholds | Expected Terminal State |
| :--- | :--- | :--- | :--- |
| **S1** | Mid-execution `TerminateProcess` | Lease expiry observed (`~2.0s`); recovery latency `< 2500ms` | Attempt #2 reaches `SUCCESS`; in-flight depth = 0 |
| **S2** | Post-result persistence `TerminateProcess` | Duplicate redelivery observed; `mock_get_handler.call_count == 0` | Result count = 1; redelivery fenced with `TERMINAL_DUPLICATE` |
| **S3** | Post-enqueue outbox `TerminateProcess` | Redelivery observed; `observable_side_effects` count = 1 | Result count = 1; redelivery fenced with `TERMINAL_DUPLICATE` |
| **S4** | Simulated PostgreSQL `OperationalError` | Redis delivery NOT acknowledged during DB outage | `UNEXPECTED_FAILURE` disposition; in-flight depth = 1 |
| **S5** | Simulated Redis `ConnectionError` | Outbox event stays `PENDING` (`retry_count = 1`) | Published to Redis upon simulated recovery; queue depth = 1 |
| **S6** | Supervisor abrupt termination (`grace=0`) | Drain time `< 3.0s`; replacement supervisor drains | All 5 tasks reach `SUCCESS`; ready & in-flight depth = 0 |
| **S7** | `threading.Barrier(4)` concurrent race | 4 threads race: 1 success, 3 rejections (`active_lease_exists`) | 10 tasks recovered by `SKIP LOCKED` with 0 duplicates |

**Machine-Validation Check:**
```powershell
.venv\Scripts\python -c @"
import json
with open('benchmarks/scenarios/g83/cross_system_failures.json') as f:
    data = json.load(f)
assert data['schema_version'] == '2'
assert data['verdict'] == 'PASS'
assert data['all_invariants_satisfied'] is True
assert len(data['scenarios']) == 7
print('G8.3 Evidence JSON schema version 2 strictly validated.')
"@
```

---

### Stage 4: High-Concurrency Idempotency Storm & Pool Saturation

Validate platform behavior under concurrent API and database load:

```powershell
# Idempotency storm under 500 concurrent producers
.venv\Scripts\pytest tests/integration/test_g84_idempotency_storms.py -v -k "test_thundering_herd_single_key_500_producers"

# Key reuse matrix: 100 unique keys across 10 repetitions (1000 total requests)
.venv\Scripts\pytest tests/integration/test_g84_idempotency_storms.py -v -k "test_key_reuse_matrix_100_keys_10_repetitions"

# Connection pool exhaustion and recovery under saturation
.venv\Scripts\pytest tests/integration/test_g84_pool_exhaustion.py -v
```

**Acceptance Thresholds:**
- 500 concurrent submissions for a single idempotency key yield **exactly 1 created task** and **499 deduplicated responses**.
- Key reuse matrix completes with throughput `> 100 rps` and 0 duplicate database entries.
- Pool saturation queues connections without crashing the FastAPI application.

---

### Stage 5: Relational Integrity & Persistence Audit

Direct database audit asserting referential integrity across all relational entities:

```powershell
.venv\Scripts\pytest tests/integration/test_g84_persistence_audit.py -v
```

**SQL Verification Queries Executed:**
1. `orphaned_attempts`: `SELECT COUNT(*) FROM task_attempts ta WHERE NOT EXISTS (SELECT 1 FROM tasks t WHERE t.id = ta.task_id)` == 0.
2. `orphaned_results`: `SELECT COUNT(*) FROM task_results tr WHERE NOT EXISTS (SELECT 1 FROM tasks t WHERE t.id = tr.task_id)` == 0.
3. `orphaned_events`: `SELECT COUNT(*) FROM task_events te WHERE NOT EXISTS (SELECT 1 FROM tasks t WHERE t.id = te.task_id)` == 0.
4. `orphaned_outbox`: `SELECT COUNT(*) FROM outbox_events oe WHERE NOT EXISTS (SELECT 1 FROM tasks t WHERE t.id = oe.aggregate_id)` == 0.
5. `duplicate_task_submitted`: `SELECT aggregate_id FROM outbox_events WHERE event_type = 'TASK_SUBMITTED' GROUP BY aggregate_id HAVING COUNT(*) > 1` == 0.
6. `idempotency_key_uniqueness`: `SELECT idempotency_key FROM tasks WHERE idempotency_key IS NOT NULL GROUP BY idempotency_key HAVING COUNT(*) > 1` == 0.

---

## 4. Evidence Output Manifest

Upon successful completion of Stages 1–5, verify that all structured evidence artifacts are present and populated:

| Path | Schema / Standard | Required Value |
| :--- | :--- | :--- |
| `benchmarks/scenarios/g83/cross_system_failures.json` | Schema v2 (Pydantic validated) | `verdict: "PASS"`, 7 scenarios |
| `benchmarks/scenarios/g84/g84_persistence_audit.json` | Referential integrity audit | `referential_integrity: "100%"`, `verdict: "PASS"` |
| `benchmarks/scenarios/g84/g84_idempotency_storm_metrics.json` | 500-producer thundering herd | `verdict: "PASS"` |
| `benchmarks/scenarios/g84/g84_pool_exhaustion_metrics.json` | Connection pool saturation | `verdict: "PASS"` |

---

## 5. Phase 8 Release Freeze Tagging Criteria

All of the following conditions must be satisfied before creating the release tag:
- [x] Frozen implementation baseline preserved at `9ab6f91`.
- [x] Evidence quality and schema v2 committed at `86adefe`.
- [x] All 161 regression tests pass cleanly on live infrastructure.
- [x] Alembic migration drill passes round-trip upgrade/downgrade on a disposable database.
- [x] Git working tree is completely clean (`git status` reports 0 modifications).
- [x] Zero raw credentials or tokens exist in evidence JSON files.

### Tag Execution Command:
```powershell
git tag -a v0.8.0-phase8-freeze -m "TaskForge Phase 8 Formal Freeze: Verified under cross-system failure validation matrix (S1-S7), transactional outbox, fail-closed lease fencing, and 161/161 regression test suite."
```
