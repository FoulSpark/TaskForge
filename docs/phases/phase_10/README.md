# Phase 10: TaskForge Web Console (`F0` – `F8`)

This directory contains architecture specifications, audited API contracts, security models, design systems, and gate verification records for **Phase 10: TaskForge Web Console (React + TypeScript + Vite)** on branch `phase-10-frontend`.

> **Baseline Integrity:** Phase 9 remains formally closed and frozen at tag `v0.9.0-phase9-freeze` (`fb31546`). Phase 10 work is strictly additive.

---

## Gate `F0` Document Index

1. [`PHASE_10_FRONTEND_SPEC.md`](PHASE_10_FRONTEND_SPEC.md): Master frontend architecture, explicit resolutions for gaps `G1`–`G5`, console page specifications, polling strategy, and `F0`–`F8` roadmap.
2. [`FRONTEND_API_CONTRACT.md`](FRONTEND_API_CONTRACT.md): Full audit of existing FastAPI routes/schemas, Phase 10 additive endpoint blueprints (`/auth/me`, tenant-scoped `/stats`, `/monitoring/summary`, `/task-types`), error envelopes, and TypeScript interfaces.
3. [`FRONTEND_SECURITY_MODEL.md`](FRONTEND_SECURITY_MODEL.md): In-memory API key policy (zero browser storage), server-side tenant isolation, RBAC enforcement matrix, `lease_token` omission, and `429`/`503` backoff rules.
4. [`FRONTEND_DESIGN_SYSTEM.md`](FRONTEND_DESIGN_SYSTEM.md): Visual tokens, semantic styling across all 8 `TaskStatus` and 4 `TaskPriority` states, tabular typography, responsive breakpoints, UX state matrix, and accessibility standards.
5. [`F0_ACCEPTANCE_CRITERIA.md`](F0_ACCEPTANCE_CRITERIA.md): Formal Gate `F0` acceptance checklist, verification plan, and `F1`–`F8` exit preview.
6. [`diagrams/f0_frontend_architecture.mmd`](diagrams/f0_frontend_architecture.mmd): Mermaid source for the Phase 10 Web Console architecture diagram.

