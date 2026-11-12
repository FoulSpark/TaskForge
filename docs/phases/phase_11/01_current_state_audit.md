# Phase 11 (`G11.0`) — Current State Repository Audit

**Branch:** `phase-11-custom-task-foundation`  
**Base Frozen Baseline:** `v0.10.0-phase10-freeze` (`51318dd48f2e997ddb3bf92f8219bbf958ba3d45`)  
**Phase 9 Freeze:** `v0.9.0-phase9-freeze` (`fb31546788d565194860568cce87a6f6fa9945a0`)  
**Scope:** Read-only repository audit prior to G11.1 persistence implementation.

---

## 1. Existing Data Model & Migration Baseline
Audited `src/taskforge/database/models.py` and `migrations/versions/`:
- **Alembic Chain (`alembic.ini`, `migrations/versions/`):**
  - `001_initial_schema.py` -> `002_g83_outbox_available_at.py` -> `5a361f84b61f_003_api_keys_and_tenant_support.py` (current head: `5a361f84b61f`).
- **Actual Column Types in Production ORM (`models.py`):**
  - Primary/Foreign IDs (`Task.id`, `OutboxEvent.id`, `ApiKey.id`): **`String(36)`** storing UUIDv4 strings (`str(uuid.uuid4())`), **not** PostgreSQL native `UUID` type.
  - Tenant identifier (`Task.tenant_id`, `TaskEvent.tenant_id`, `ApiKey.tenant_id`): **`String(64)`** (`default="default"`, indexed).
  - Principal / actor identifier (`TaskEvent.actor_id`): **`String(128)`** nullable (`ApiKey.key_id` is `String(32)`).
  - Structured JSON columns (`Task.payload`, `TaskResult.return_value`, `TaskEvent.metadata_json`, `OutboxEvent.payload`, `ApiKey.scopes`): SQLAlchemy portable **`JSON`** (`sa.JSON()` in Alembic migrations), enabling identical ORM schema creation in PostgreSQL 16 and SQLite in-memory tests.

## 2. Task Submission Transaction & Idempotency Audit
Audited `src/taskforge/api/routes/tasks.py`, `src/taskforge/tasks/service.py`, and `src/taskforge/database/repositories/task_repository.py`:
- **Transaction Boundary:**
  - `TaskService.submit_task()` delegates to `TaskRepository.create_task()`, which performs `db.add(task)`, `db.add(event)`, `db.add(outbox)`, and calls **`self.db.commit()`** inside `create_task()` (line 228).
  - Therefore, any definition-version pinning fields (`definition_id`, `definition_version_id`) and definition execution audit event MUST be passed into and committed inside that same atomic transaction boundary.
- **Idempotency Implementation & Race Path Findings:**
  1. `Task.idempotency_key` currently carries a **table-wide `unique=True`** constraint (`tasks_idempotency_key_key`), not tenant-scoped `(tenant_id, idempotency_key)`.
  2. Fast path (`task_repository.py:168`) queries `get_by_idempotency_key(idempotency_key, tenant_id=tenant_id)`, whereas the `IntegrityError` (`SQLSTATE 23505`) race fallback (`task_repository.py:240`) calls `get_by_idempotency_key(idempotency_key)` **without `tenant_id`**. In Phase 11, definition execution must filter idempotency lookups strictly by `tenant_id` (and reject cross-tenant collision cleanly without exposing Tenant A's task).
  3. `TaskService._verify_payload_match()` compares `task_name`, `task_type`, `payload`, `priority`, `timeout_seconds`, `max_retries`, and normalized `scheduled_at`. For **definition execution replay after a newer version is activated**: if `idempotency_key` matches an existing task for that `definition_id` whose original resolved inputs match, replay must return the original task and its **original pinned `definition_version_id`** (`HTTP 200`) rather than failing with `409` because `active_version_id` later moved to `v2`.

## 3. Authentication, Principal & Scope Model
Audited `src/taskforge/core/auth.py` and `src/taskforge/api/dependencies.py`:
- `AuthenticatedPrincipal` carries `key_id`, `tenant_id`, `name`, `scopes`.
- `require_scope(required_scope)` grants access if `required_scope in principal.scopes` **or** `"admin:operator" in principal.scopes`.
- Under `TASKFORGE_TESTING=1` without `TASKFORGE_STRICT_AUTH=1`, `get_current_principal()` falls back to `legacy-test-runner` with `["tasks:read", "tasks:write", "admin:operator"]`, so `admin:operator` automatically covers new `definitions:*` scopes in legacy tests while strict tests enforce explicit scopes (`definitions:read`, `definitions:write`, `definitions:activate`).

## 4. Handler Registry & Dependency Gaps
- `src/taskforge/tasks/registry.py` exposes `register_handler(name)` (`dict[str, Any] -> dict[str, Any]`), `get_handler(name)` (raises `KeyError` if unregistered), and `list_registered_handlers()`.
- `src/taskforge/tasks/catalog.py` exports `SUBMISSION_CONSTRAINTS` (`priority`, `timeout_seconds`, `max_retries`, `task_name`, `idempotency_key`) and `build_task_types_catalog()`.
- `pyproject.toml` does **not** currently include `jsonschema`; `G11.3` (`WP11-D`) will add `jsonschema` with local-only validation (remote `$ref` blocked).

## 5. Task Response Serialization
- `TaskResponse` (`src/taskforge/api/schemas.py:69-89`) uses `model_config = ConfigDict(from_attributes=True)`. Adding nullable `definition_id: str | None = None` and `definition_version_id: int/str | None = None` (plus optional `definition_version: int | None = None` provenance) preserves full backward compatibility for existing built-in tasks (`None`).
