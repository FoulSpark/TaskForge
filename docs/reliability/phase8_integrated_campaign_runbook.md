# TaskForge Phase 8: Final Integrated Reliability Campaign Runbook

**Document Version:** 1.0.0  
**Target Release Candidate:** Commit `2329047` (building on baseline `9ab6f91`)  
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
$origDbUrl = $env:DATABASE_URL
try {
    # 1. Create disposable test database
    .venv\Scripts\python -c @"
import psycopg
conn = psycopg.connect('postgresql://taskforge:taskforge_dev@localhost:5432/postgres', autocommit=True)
conn.execute('DROP DATABASE IF EXISTS taskforge_migration_drill WITH (FORCE)')
conn.execute('CREATE DATABASE taskforge_migration_drill OWNER taskforge')
print('Created disposable database taskforge_migration_drill')
"@

    # 2. Set temporary connection string
    $env:DATABASE_URL = "postgresql+psycopg://taskforge:taskforge_dev@localhost:5432/taskforge_migration_drill"

    # 3. Run Alembic upgrade to head
    .venv\Scripts\alembic upgrade head

    # 4. Verify schema and table creation
    .venv\Scripts\python -c @"
from sqlalchemy import create_engine, inspect
import os
engine = create_engine(os.environ['DATABASE_URL'])
tables = inspect(engine).get_table_names()
assert 'tasks' in tables and 'task_attempts' in tables and 'outbox_events' in tables
print('Migration upgrade head verified. Tables:', sorted(tables))
"@

    # 5. Downgrade by 1 revision and re-upgrade
    .venv\Scripts\alembic downgrade -1
    .venv\Scripts\alembic upgrade head
    Write-Host "Migration round-trip complete."
}
finally {
    # Unconditional cleanup of disposable database
    $env:DATABASE_URL = if ($origDbUrl) { $origDbUrl } else { "postgresql+psycopg://taskforge:taskforge_dev@localhost:5432/taskforge_test" }
    .venv\Scripts\python -c @"
import psycopg
try:
    conn = psycopg.connect('postgresql://taskforge:taskforge_dev@localhost:5432/postgres', autocommit=True)
    conn.execute('DROP DATABASE IF EXISTS taskforge_migration_drill WITH (FORCE)')
    print('Unconditionally dropped disposable database taskforge_migration_drill')
except Exception as e:
    print('Cleanup warning:', e)
"@
}
```

**Pass/Fail Criteria:**
- `alembic upgrade head` succeeds with 0 errors.
- Tables `tasks`, `task_attempts`, `task_results`, `task_events`, `outbox_events` created.
- `downgrade -1` and `upgrade head` complete cleanly without orphaned constraints.

---

### Stage 2: Full Regression Verification (Dynamic Test Suite Derivation)

Execute all unit, security, integration, and reliability test suites. Test counts are derived dynamically from the pytest collection rather than hardcoded, enforcing zero unhandled warnings, zero skips, zero xfails, and zero failures:

```powershell
# Execute full test suite with dynamic collection derivation and strict warning handling
.venv\Scripts\python -c @"
import subprocess, sys

cmd = [
    sys.executable, '-m', 'pytest',
    'tests/unit/', 'tests/security/', 'tests/integration/', 'tests/reliability/',
    '-v', '--tb=short',
    '-W', 'error',
    '-W', 'ignore::starlette.exceptions.StarletteDeprecationWarning',
]

print('Running full regression matrix...')
res = subprocess.run(cmd, capture_output=True, text=True)
print(res.stdout[-1500:])

if res.returncode != 0:
    print('REGRESSION SUITE FAILED!')
    sys.exit(res.returncode)

# Parse output to enforce zero skipped and zero xfailed
for line in res.stdout.splitlines():
    if 'passed in' in line:
        assert 'skipped' not in line, f'Skipped tests detected: {line}'
        assert 'xfailed' not in line, f'Xfailed tests detected: {line}'
        print(f'Test execution summary: {line.strip()}')

print('ALL COLLECTED TESTS PASSED CLEANLY WITH ZERO WARNING ERRORS AND ZERO SKIPS/XFAILS.')
"@

# Verify formatting and linter standards
.venv\Scripts\ruff check src/ tests/ migrations/
```

**Pass/Fail Criteria:**
- Test runner executes all collected tests dynamically (100% pass rate).
- 0 FAILED, 0 ERRORS, 0 SKIPPED, 0 XFAILED.
- 0 unhandled warnings (`-W error` active, third-party testclient deprecation ignored).
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
| **S4** | Simulated PostgreSQL `OperationalError` | Redis delivery NOT acknowledged during simulated DB outage | `UNEXPECTED_FAILURE` disposition; in-flight depth = 1 |
| **S5** | Simulated Redis `ConnectionError` | Outbox event stays `PENDING` (`retry_count = 1`) | Published to Redis upon simulated recovery; queue depth = 1 |
| **S6** | Supervisor abrupt termination (`grace=0`) | Drain time `< 3.0s`; in-flight verified `>= 1` before kill; replacement supervisor drains | All 5 tasks reach `SUCCESS`; ready & in-flight depth = 0 |
| **S7** | `threading.Barrier(4)` concurrent race | 3 rounds of 4-thread race: 12 attempted acquisitions, 3 successes, 9 rejections (`active_unexpired_lease_held_by_competitor`); 3 non-overlapping PostgreSQL `clock_timestamp()` ownership intervals (`LEASE_ACQUIRED`/`LEASE_RELEASED`); `max_concurrent_valid_leases_measured == 1` via sweep-line | 10 tasks recovered by `SKIP LOCKED` with 0 duplicates |

**Machine-Validation Check:**
```powershell
.venv\Scripts\python -c @"
import json
from tests.reliability.test_cross_system_failures import MatrixEvidenceSchema

with open('benchmarks/scenarios/g83/cross_system_failures.json', encoding='utf-8') as f:
    raw = json.load(f)

# Pydantic model validation with extra='forbid' and campaign consistency
validated = MatrixEvidenceSchema.model_validate(raw)
assert validated.schema_version == '2'
assert validated.verdict == 'PASS'
assert validated.all_invariants_satisfied is True
assert len(validated.scenarios) == 7
print(f'G8.3 Evidence JSON schema version 2 strictly validated via Pydantic (campaign {validated.campaign_id}).')
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
6. `missing_task_submitted`: `SELECT t.id FROM tasks t WHERE NOT EXISTS (SELECT 1 FROM outbox_events oe WHERE oe.aggregate_id = t.id AND oe.event_type = 'TASK_SUBMITTED')` == 0.
7. `submission_parity`: `COUNT(tasks) == COUNT(DISTINCT aggregate_id) WHERE event_type = 'TASK_SUBMITTED'`.
8. `idempotency_key_uniqueness`: `SELECT idempotency_key FROM tasks WHERE idempotency_key IS NOT NULL GROUP BY idempotency_key HAVING COUNT(*) > 1` == 0.

---

### Stage 6: Artifact Reconciliation, Schema Verification & Pre-Tag Audit

Before applying the Phase 8 release tag, execute the automated pre-freeze verification script:

```powershell
.venv\Scripts\python -c @"
import json, os, re, subprocess, sys
from pathlib import Path

os.environ['TASKFORGE_TESTING'] = '1'
os.environ['APP_ENV'] = 'test'

# 1. Verify all 4 required evidence files exist and are populated
required_artifacts = [
    Path('benchmarks/scenarios/g83/cross_system_failures.json'),
    Path('benchmarks/scenarios/g84/g84_persistence_audit.json'),
    Path('benchmarks/scenarios/g84/g84_idempotency_storm_metrics.json'),
    Path('benchmarks/scenarios/g84/g84_pool_exhaustion_metrics.json'),
]

for p in required_artifacts:
    assert p.exists(), f'Missing required artifact: {p}'
    assert p.stat().st_size > 50, f'Artifact empty or truncated: {p}'

print('[1/5] All 4 required benchmark and reliability artifacts exist and non-empty.')

# 2. Pydantic Schema v2 validation of cross_system_failures.json
from tests.reliability.test_cross_system_failures import MatrixEvidenceSchema

cs_path = required_artifacts[0]
with open(cs_path, encoding='utf-8') as f:
    cs_data = json.load(f)

validated = MatrixEvidenceSchema.model_validate(cs_data)
assert validated.schema_version == '2', f'Invalid schema version: {validated.schema_version}'
assert validated.verdict == 'PASS', f'Campaign verdict is not PASS: {validated.verdict}'
assert validated.all_invariants_satisfied is True, 'Not all invariants satisfied'
assert len(validated.scenarios) == 7, f'Expected 7 scenarios, got {len(validated.scenarios)}'

campaign_id = validated.campaign_id
assert campaign_id, 'Campaign ID missing from matrix'
for s_id, s in validated.scenarios.items():
    assert s.campaign_id == campaign_id, f'Scenario {s_id} campaign_id mismatch: {s.campaign_id} != {campaign_id}'
    assert s.invariants_passed is True, f'Scenario {s_id} did not pass invariants'

print(f'[2/5] Cross-system failure matrix schema v2 strictly validated via Pydantic (campaign {campaign_id}).')

# 3. Persistence audit verification
pa_path = required_artifacts[1]
with open(pa_path, encoding='utf-8') as f:
    pa_data = json.load(f)

assert pa_data['referential_integrity'] == '100%', 'Referential integrity not 100%'
assert pa_data['verdict'] == 'PASS', 'Persistence audit verdict not PASS'
assert pa_data['orphaned_attempts'] == 0
assert pa_data['orphaned_results'] == 0
assert pa_data['orphaned_events'] == 0
assert pa_data['orphaned_outbox'] == 0
assert pa_data.get('missing_outbox_count', 0) == 0
print('[3/5] Referential integrity audit strictly verified (0 orphaned rows, 100% integrity).')

# 4. Raw credential and token fingerprint audit across all artifacts
token_pattern = re.compile(r'token-[a-zA-Z0-9_-]{10,}')
for p in required_artifacts:
    text = p.read_text(encoding='utf-8')
    assert 'taskforge_dev' not in text, f'Database password leak detected in {p}!'
    assert not token_pattern.search(text), f'Plaintext raw token pattern detected in {p}!'

print('[4/5] Zero credentials or raw plaintext tokens leaked across artifacts (all SHA-256 fingerprinted).')

# 5. Git working tree hygiene
status_out = subprocess.check_output(['git', 'status', '--porcelain'], text=True).strip()
untracked_unstaged = [line for line in status_out.splitlines() if not line.startswith('?? benchmarks/')]
if untracked_unstaged:
    print('WARNING: Git working tree contains unstaged modifications:')
    for line in untracked_unstaged:
        print(' ', line)
else:
    print('[5/5] Git working tree clean.')

print('ALL STAGE 6 PRE-FREEZE CHECKS PASSED CLEANLY.')
"@
```

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
- [ ] Frozen implementation baseline preserved at `9ab6f91`.
- [ ] Evidence quality and schema v2 committed on release candidate.
- [ ] All regression tests pass cleanly on live infrastructure with zero skips, zero xfails, and zero unhandled warnings.
- [ ] Alembic migration drill passes round-trip upgrade/downgrade on a disposable database with unconditional cleanup.
- [ ] Git working tree is completely clean (`git status` reports 0 modifications).
- [ ] Zero raw credentials or tokens exist in evidence JSON files.

### Tag Execution Command:
```powershell
git tag -a v0.8.0-phase8-freeze -m "TaskForge Phase 8 Formal Freeze: Verified under cross-system failure validation matrix (S1-S7), transactional outbox, fail-closed lease fencing, and 161/161 regression test suite."
```

