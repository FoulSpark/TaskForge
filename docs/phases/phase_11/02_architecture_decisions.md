# Phase 11 (`G11.0`) — Architecture Decisions & Version-Pinning Lock

**Branch:** `phase-11-custom-task-foundation`  
**Target Gates:** `G11.1` – `G11.6`

---

## ADR-11.1: Portable ID, Tenant & JSON Column Types
- **Decision:** Use `String(36)` for `task_definitions.id`, `task_definition_versions.id`, and `definition_audit_events.id`; `String(64)` for `tenant_id`; `String(128)` for `created_by` / `actor_id`; and portable SQLAlchemy `JSON` (`sa.JSON()`) for `configuration_json`, `input_schema_json`, `output_schema_json`, `execution_policy_json`, `validation_report_json`, and `metadata_json`.
- **Rationale:** Matches existing `tasks`, `task_events`, `outbox_events`, and `api_keys` tables (`001`–`003`) so Alembic migrations work identically on PostgreSQL 16 and SQLite in-memory test harnesses without dialect divergence.

## ADR-11.2: Circular Foreign-Key Resolution (`task_definitions` ↔ `task_definition_versions`)
- **Decision:** Create `task_definitions` with nullable `active_version_id`, create `task_definition_versions` referencing `task_definitions.id` (`ON DELETE CASCADE`), and attach `fk_task_definitions_active_version` (`use_alter=True`, `ON DELETE SET NULL`) after `task_definition_versions` exists. Drop `fk_task_definitions_active_version` explicitly before dropping tables in `downgrade()`.
- **Rationale:** Prevents PostgreSQL DDL ordering errors during Alembic `upgrade`/`downgrade` and SQLAlchemy `Base.metadata.create_all()` warnings.

## ADR-11.3: Atomic Version Pinning & Composite Task Integrity
- **Decision:**
  1. `tasks` gains nullable `definition_id` (`String(36)`) and `definition_version_id` (`String(36)`), guarded by:
     - `CheckConstraint("(definition_id IS NULL AND definition_version_id IS NULL) OR (definition_id IS NOT NULL AND definition_version_id IS NOT NULL)")`
     - Composite `ForeignKeyConstraint(["definition_version_id", "definition_id"], ["task_definition_versions.id", "task_definition_versions.definition_id"], ondelete="RESTRICT")` backed by `UniqueConstraint("id", "definition_id", name="uq_task_def_versions_id_def")` on `task_definition_versions`.
  2. During `POST /api/v1/definitions/{definition_id}/execute`:
     - Check existing `(tenant_id, idempotency_key)` first: if a task already exists for this `definition_id` and matches the original submitted request parameters, return it immediately (`HTTP 200`) with its original pinned `definition_version_id` (even if a newer definition version has since been activated).
     - Otherwise, lock `task_definitions` (`SELECT ... FOR UPDATE` / `FOR SHARE` inside the submission transaction), verify `status == "ACTIVE"` and `active_version_id IS NOT NULL`, resolve the active version, validate runtime `input` against `input_schema_json` and handler constraints, merge payload deterministically, and insert `Task` (`definition_id`, `definition_version_id`), `TaskEvent`, `OutboxEvent`, and `DefinitionAuditEvent(action="DEFINITION_EXECUTED")` in **one atomic database commit**.
- **Rationale:** Eliminates active-version TOCTOU races, prevents cross-definition version mismatches at the DB layer, and preserves AC-11.7 idempotent replay provenance across version upgrades.

## ADR-11.4: Provider-Independent Execution Type Contract
- **Decision:** Store `execution_type` (`VARCHAR(32)`) on both `task_definitions` and `task_definition_versions`, validating strictly against `SUPPORTED_EXECUTION_TYPES = {"builtin"}` in Phase 11. Future types (`http`, `workflow`, `python_sandbox`, `ai_generated`) are recognized as reserved/unsupported extension identifiers rather than executable adapters.
- **Rationale:** Keeps Gemini/LLM integration decoupled behind future proposal -> validation -> immutable version flows without locking Phase 11 into any AI provider.

## ADR-11.5: Version Immutability Enforcement
- **Decision:** `task_definition_versions` rows are append-only in repository/service code (no `UPDATE` on executable columns after insert; `validation_status` / `validation_report_json` may either be computed at creation or stored cleanly with immutable executable content hash, and any executable change requires `POST /versions` creating `max(version) + 1`).
- **Rationale:** Guarantees tasks pinned to `version N` always match their recorded `content_hash` and schema contract.
