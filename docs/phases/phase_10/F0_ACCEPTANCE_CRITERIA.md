# Gate F0 — Acceptance Criteria & Verification Plan

**Milestone:** Phase 10 — TaskForge Web Console  
**Gate:** `F0` — Architecture, API Contract Audit & Design Lock  
**Branch:** `phase-10-frontend`  

---

## 1. Gate `F0` Mandatory Deliverables Checklist

- [x] **1. Frontend Specification:** [`PHASE_10_FRONTEND_SPEC.md`](PHASE_10_FRONTEND_SPEC.md) defining architecture, page scope (`Overview`, `Submit`, `Explorer`, `Details`, `Monitoring`), polling strategy (`2–3s`), and `F0`–`F8` roadmap.
- [x] **2. Audited API Contract:** [`FRONTEND_API_CONTRACT.md`](FRONTEND_API_CONTRACT.md) auditing every existing FastAPI route/schema (`schemas.py`, `tasks.py`, `metrics.py`, `workers.py`, `operator.py`, `errors.py`, `main.py`) and locking Phase 10 additive contracts (`/auth/me`, tenant-scoped `/stats`, `/monitoring/summary`, `/task-types`, `admin:operator` system endpoints).
- [x] **3. Frontend Security Model:** [`FRONTEND_SECURITY_MODEL.md`](FRONTEND_SECURITY_MODEL.md) locking in-memory API-key policy, zero browser persistence (`localStorage`/`sessionStorage` forbidden), server-side tenant isolation, RBAC enforcement, `lease_token` omission, and `429`/`503` backoff.
- [x] **4. Frontend Design System:** [`FRONTEND_DESIGN_SYSTEM.md`](FRONTEND_DESIGN_SYSTEM.md) specifying color/contrast tokens, all 8 `TaskStatus` & 4 `TaskPriority` semantics, tabular numerals, UX error/outage states, responsive breakpoints, and WCAG accessibility rules.
- [x] **5. Frontend Architecture Diagram:** [`diagrams/f0_frontend_architecture.mmd`](diagrams/f0_frontend_architecture.mmd) showing Vite dev proxy / same-origin prod hosting, in-memory auth, typed client, visibility-aware poller, 5 views, Phase 10 FastAPI contracts, PostgreSQL 16, Redis 7, and Prometheus/Grafana alongside.
- [x] **6. Explicit Decisions on All Five Audit Gaps (`G1`–`G5`):** Documented and justified in `PHASE_10_FRONTEND_SPEC.md`, `FRONTEND_API_CONTRACT.md`, and `FRONTEND_SECURITY_MODEL.md`.
- [x] **7. `F0` Verification Plan & Baseline Integrity:** Automated Mermaid render validation (`mmdc`), link/path contract test suite, and verified preservation of Phase 9 `v0.9.0-phase9-freeze` (`fb31546`) and frozen `src/`.

---

## 2. Gate `F0` Verification Plan

| Check ID | Verification Method | Expected Result |
| :---: | :--- | :--- |
| **`V-F0-1`** | **Mermaid Diagram Validation:** Run `@mermaid-js/mermaid-cli` (`mmdc`) on `diagrams/f0_frontend_architecture.mmd` | Exit code `0`, valid SVG generated, zero syntax errors, matches embedded block in `PHASE_10_FRONTEND_SPEC.md`. |
| **`V-F0-2`** | **Contract & Link Audit Test:** Execute automated unit test verifying all 7 `F0` deliverables exist, gaps `G1`–`G5` are explicitly decided, all internal Markdown links resolve, and cited backend routes/files exist in `src/taskforge/` | 100% pass rate, zero broken links or stale source paths. |
| **`V-F0-3`** | **Baseline Freeze Verification:** Confirm `v0.9.0-phase9-freeze` (`fb31546`) is unchanged, `phase-9-hardening` is untouched, and `git diff --stat v0.9.0-phase9-freeze..HEAD -- src/ benchmarks/scenarios/` is empty (zero code changes in `F0`) | Empty diff against `src/` and `benchmarks/scenarios/`; clean working tree. |

---

## 3. Master Phase 10 Gate Acceptance Summary (`F1` – `F8` Preview)

Each subsequent Phase 10 implementation gate will be closed only against concrete verification evidence:
- **`F1`:** Backend additive endpoints (`/auth/me`, tenant-scoped `/stats`, `/monitoring/summary`, `/task-types`) pass pytest unit + strict-auth cross-tenant isolation tests (`0` regression on Phase 9 tests); Vite + React + TS compiles cleanly with zero hardcoded credentials.
- **`F2`–`F6`:** Vitest / React Testing Library component & state-machine tests verify zero hardcoded dashboard data, loading/empty/error/`429`/`503` states, terminal task immutability guards, pagination, and live polling cancellation.
- **`F7`:** Security audit (zero `localStorage`/`sessionStorage` token writes, XSS escaping, scoped action hiding paired with `403` enforcement), keyboard accessibility, and responsive layout checks.
- **`F8`:** End-to-end Playwright (or browser) suite driving live FastAPI + PostgreSQL + Redis (`submit -> queue -> execute -> inspect result`), simulated service outage recovery UI check, and single-command Docker Compose startup documentation.

