"""Domain service for Phase 11 Custom Task Definition Registry & Version Lifecycle."""

import hashlib
import json
import re
from typing import Any

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from taskforge.api.schemas import DefinitionExecuteRequest
from taskforge.core.auth import AuthenticatedPrincipal
from taskforge.database.models import Task, get_utc_now
from taskforge.definitions.hashing import compute_definition_content_hash
from taskforge.definitions.models import (
    DefinitionAuditEvent,
    TaskDefinition,
    TaskDefinitionVersion,
)
from taskforge.definitions.repository import DefinitionRepository
from taskforge.definitions.validation import (
    DefinitionValidationResult,
    validate_definition_version,
)

SUPPORTED_EXECUTION_TYPES: frozenset[str] = frozenset({"builtin"})
SENSITIVE_CONFIG_KEY_RE = re.compile(
    r"(api_key|secret|password|private_key|access_token|bearer_token|authorization)",
    re.IGNORECASE,
)


class DefinitionServiceError(Exception):
    """Base error for definition service operations."""


class DefinitionNotFoundError(DefinitionServiceError):
    """Raised when a definition or version is missing or outside tenant scope."""


class DefinitionConflictError(DefinitionServiceError):
    """Raised on duplicate definition name or invalid lifecycle state transition."""


class IdempotencyConflictError(DefinitionConflictError):
    """Raised when an idempotency key is reused with mismatched parameters."""


class DefinitionIntegrityError(DefinitionConflictError):
    """Raised when content hash of a definition version fails integrity verification."""


class DefinitionValidationError(DefinitionServiceError):
    """Raised when validation fails or activation is attempted on unvalidated/invalid version."""


class DefinitionPolicyError(DefinitionServiceError):
    """Raised when execution_type or payload violates security/policy constraints."""


def compute_execution_request_fingerprint(
    *,
    definition_id: str,
    definition_version_id: str,
    input_payload: dict[str, Any],
    priority: int,
    timeout_seconds: int | None,
) -> str:
    """Computes a deterministic SHA-256 fingerprint for execution idempotency."""
    fingerprint_data = {
        "definition_id": definition_id,
        "definition_version_id": definition_version_id,
        "input": input_payload,
        "priority": priority,
        "timeout_seconds": timeout_seconds,
    }
    canonical_json = json.dumps(fingerprint_data, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()


def _scan_for_prohibited_secrets(value: Any, path: str = "", *, check_keys: bool = True) -> None:
    """Reject raw credentials or API key strings inside definition JSON, schemas, or reports."""
    if isinstance(value, str):
        if "tf_live_" in value:
            raise DefinitionPolicyError(
                f"Prohibited TaskForge API key secret detected at '{path or 'root'}'."
            )
        return
    if isinstance(value, list):
        for idx, item in enumerate(value):
            _scan_for_prohibited_secrets(item, f"{path}[{idx}]", check_keys=check_keys)
        return
    if isinstance(value, dict):
        for key, item in value.items():
            key_str = str(key)
            next_path = f"{path}.{key_str}" if path else key_str
            if check_keys and SENSITIVE_CONFIG_KEY_RE.search(key_str) and item not in (None, ""):
                raise DefinitionPolicyError(
                    f"Prohibited credential-like configuration field '{next_path}'."
                )
            _scan_for_prohibited_secrets(item, next_path, check_keys=check_keys)


class DefinitionService:
    """Tenant-isolated service managing definition lifecycles, versioning, and audits."""

    def __init__(self, db: Session):
        self.db = db
        self.repo = DefinitionRepository(db)

    @staticmethod
    def _validate_definition_inputs(
        *,
        name: str | None = None,
        description: str | None = None,
        execution_type: str,
        configuration_json: dict[str, Any],
        execution_policy_json: dict[str, Any],
        input_schema_json: dict[str, Any] | None = None,
        output_schema_json: dict[str, Any] | None = None,
    ) -> None:
        if name is not None:
            stripped = name.strip()
            if not stripped or len(stripped) > 160:
                raise DefinitionValidationError(
                    "Definition name must be between 1 and 160 characters."
                )
        if description is not None:
            if len(description) > 4000:
                raise DefinitionValidationError(
                    "Definition description must not exceed 4000 characters."
                )
            if "tf_live_" in description:
                raise DefinitionPolicyError(
                    "Prohibited TaskForge API key secret detected in definition description."
                )
        if execution_type not in SUPPORTED_EXECUTION_TYPES:
            raise DefinitionPolicyError(
                f"Unsupported execution_type '{execution_type}'. "
                f"Supported in Phase 11: {sorted(SUPPORTED_EXECUTION_TYPES)}."
            )
        _scan_for_prohibited_secrets(configuration_json, "configuration_json", check_keys=True)
        _scan_for_prohibited_secrets(
            execution_policy_json, "execution_policy_json", check_keys=True
        )
        if input_schema_json:
            _scan_for_prohibited_secrets(input_schema_json, "input_schema_json", check_keys=False)
        if output_schema_json:
            _scan_for_prohibited_secrets(output_schema_json, "output_schema_json", check_keys=False)

    def create_definition(
        self,
        *,
        principal: AuthenticatedPrincipal,
        name: str,
        description: str | None,
        execution_type: str,
        task_type: str,
        configuration_json: dict[str, Any],
        input_schema_json: dict[str, Any],
        output_schema_json: dict[str, Any] | None = None,
        execution_policy_json: dict[str, Any] | None = None,
    ) -> tuple[TaskDefinition, TaskDefinitionVersion]:
        policy = execution_policy_json or {}
        self._validate_definition_inputs(
            name=name,
            description=description,
            execution_type=execution_type,
            configuration_json=configuration_json,
            execution_policy_json=policy,
            input_schema_json=input_schema_json,
            output_schema_json=output_schema_json,
        )
        try:
            content_hash = compute_definition_content_hash(
                execution_type=execution_type,
                task_type=task_type,
                configuration_json=configuration_json,
                input_schema_json=input_schema_json,
                output_schema_json=output_schema_json,
                execution_policy_json=policy,
            )
        except ValueError as exc:
            raise DefinitionValidationError(str(exc)) from exc

        try:
            defn = self.repo.create_definition(
                tenant_id=principal.tenant_id,
                name=name.strip(),
                description=description,
                execution_type=execution_type,
                created_by=principal.key_id,
                status="DRAFT",
                commit=False,
            )
            ver = self.repo.create_version(
                definition_id=defn.id,
                version=1,
                execution_type=execution_type,
                task_type=task_type,
                configuration_json=configuration_json,
                input_schema_json=input_schema_json,
                output_schema_json=output_schema_json,
                execution_policy_json=policy,
                content_hash=content_hash,
                validation_status="UNVALIDATED",
                validation_report_json={},
                created_by=principal.key_id,
                commit=False,
            )
            self.repo.record_audit_event(
                tenant_id=principal.tenant_id,
                definition_id=defn.id,
                version_id=ver.id,
                actor_id=principal.key_id,
                action="DEFINITION_CREATED",
                metadata_json={"name": defn.name, "execution_type": execution_type},
                commit=False,
            )
            self.repo.record_audit_event(
                tenant_id=principal.tenant_id,
                definition_id=defn.id,
                version_id=ver.id,
                actor_id=principal.key_id,
                action="VERSION_CREATED",
                metadata_json={"version": 1, "content_hash": content_hash},
                commit=False,
            )
            self.db.commit()
            self.db.refresh(defn)
            self.db.refresh(ver)
            return defn, ver
        except IntegrityError as exc:
            self.db.rollback()
            raise DefinitionConflictError(
                f"Definition named '{name.strip()}' already exists for this tenant."
            ) from exc
        except Exception:
            self.db.rollback()
            raise

    def get_definition(
        self, principal: AuthenticatedPrincipal, definition_id: str
    ) -> TaskDefinition:
        defn = self.repo.get_definition(definition_id, principal.tenant_id)
        if defn is None:
            raise DefinitionNotFoundError(
                f"Definition '{definition_id}' not found in tenant scope."
            )
        return defn

    def list_definitions(
        self,
        *,
        principal: AuthenticatedPrincipal,
        status: str | None = None,
        execution_type: str | None = None,
        search: str | None = None,
        sort_by: str = "updated_at",
        limit: int = 20,
        offset: int = 0,
    ) -> tuple[list[TaskDefinition], int]:
        return self.repo.list_definitions(
            tenant_id=principal.tenant_id,
            status=status,
            execution_type=execution_type,
            search=search,
            sort_by=sort_by,
            limit=limit,
            offset=offset,
        )

    def create_version(
        self,
        *,
        principal: AuthenticatedPrincipal,
        definition_id: str,
        task_type: str,
        configuration_json: dict[str, Any],
        input_schema_json: dict[str, Any],
        output_schema_json: dict[str, Any] | None = None,
        execution_policy_json: dict[str, Any] | None = None,
        execution_type: str = "builtin",
        max_concurrency_retries: int = 5,
    ) -> TaskDefinitionVersion:
        """Append an immutable version safely under concurrent callers."""
        policy = execution_policy_json or {}
        self._validate_definition_inputs(
            execution_type=execution_type,
            configuration_json=configuration_json,
            execution_policy_json=policy,
            input_schema_json=input_schema_json,
            output_schema_json=output_schema_json,
        )
        try:
            content_hash = compute_definition_content_hash(
                execution_type=execution_type,
                task_type=task_type,
                configuration_json=configuration_json,
                input_schema_json=input_schema_json,
                output_schema_json=output_schema_json,
                execution_policy_json=policy,
            )
        except ValueError as exc:
            raise DefinitionValidationError(str(exc)) from exc

        for attempt in range(max_concurrency_retries):
            try:
                # Row-level lock on PostgreSQL serializes version allocation per definition.
                defn = self.repo.get_definition(definition_id, principal.tenant_id, for_update=True)
                if defn is None:
                    raise DefinitionNotFoundError(
                        f"Definition '{definition_id}' not found in tenant scope."
                    )

                next_ver = self.repo.get_next_version_number(defn.id)
                ver = self.repo.create_version(
                    definition_id=defn.id,
                    version=next_ver,
                    execution_type=execution_type,
                    task_type=task_type,
                    configuration_json=configuration_json,
                    input_schema_json=input_schema_json,
                    output_schema_json=output_schema_json,
                    execution_policy_json=policy,
                    content_hash=content_hash,
                    validation_status="UNVALIDATED",
                    validation_report_json={},
                    created_by=principal.key_id,
                    commit=False,
                )
                defn.updated_at = get_utc_now()
                self.repo.record_audit_event(
                    tenant_id=principal.tenant_id,
                    definition_id=defn.id,
                    version_id=ver.id,
                    actor_id=principal.key_id,
                    action="VERSION_CREATED",
                    metadata_json={"version": next_ver, "content_hash": content_hash},
                    commit=False,
                )
                self.db.commit()
                self.db.refresh(ver)
                return ver
            except IntegrityError:
                self.db.rollback()
                if attempt == max_concurrency_retries - 1:
                    raise DefinitionConflictError(
                        "Concurrent version creation conflict; please retry."
                    ) from None
            except DefinitionServiceError:
                self.db.rollback()
                raise
            except Exception:
                self.db.rollback()
                raise

        raise DefinitionConflictError("Failed to allocate unique version number.")

    def get_version(
        self,
        principal: AuthenticatedPrincipal,
        definition_id: str,
        version_number: int,
    ) -> TaskDefinitionVersion:
        ver = self.repo.get_version(definition_id, version_number, principal.tenant_id)
        if ver is None:
            raise DefinitionNotFoundError(
                f"Version {version_number} of definition '{definition_id}' not found in tenant scope."
            )
        return ver

    def list_versions(
        self, principal: AuthenticatedPrincipal, definition_id: str
    ) -> list[TaskDefinitionVersion]:
        self.get_definition(principal, definition_id)
        return self.repo.list_versions(definition_id, principal.tenant_id)

    def validate_version(
        self,
        *,
        principal: AuthenticatedPrincipal,
        definition_id: str,
        version_number: int,
    ) -> tuple[TaskDefinitionVersion, DefinitionValidationResult]:
        """Orchestrate schema and registry validation of a version and persist the report."""
        ver = self.get_version(principal, definition_id, version_number)
        val_result = validate_definition_version(
            task_type=ver.task_type,
            execution_type=ver.execution_type,
            configuration_json=ver.configuration_json,
            input_schema_json=ver.input_schema_json,
            output_schema_json=ver.output_schema_json,
        )
        updated_ver = self.record_validation_result(
            principal=principal,
            definition_id=definition_id,
            version_number=version_number,
            is_valid=val_result.is_valid,
            report=val_result.to_dict(),
        )
        return updated_ver, val_result

    def record_validation_result(
        self,
        *,
        principal: AuthenticatedPrincipal,
        definition_id: str,
        version_number: int,
        is_valid: bool,
        report: dict[str, Any],
    ) -> TaskDefinitionVersion:
        """Persist version validation status & report alongside VERSION_VALIDATED audit event."""
        _scan_for_prohibited_secrets(report, "validation_report_json", check_keys=False)
        try:
            ver = self.repo.get_version(definition_id, version_number, principal.tenant_id)
            if ver is None:
                raise DefinitionNotFoundError(
                    f"Version {version_number} of definition '{definition_id}' not found in tenant scope."
                )
            status_str = "VALID" if is_valid else "INVALID"
            self.repo.set_version_validation(
                version=ver,
                validation_status=status_str,
                validation_report_json=report,
                commit=False,
            )
            self.repo.record_audit_event(
                tenant_id=principal.tenant_id,
                definition_id=definition_id,
                version_id=ver.id,
                actor_id=principal.key_id,
                action="VERSION_VALIDATED",
                metadata_json={"version": version_number, "validation_status": status_str},
                commit=False,
            )
            self.db.commit()
            self.db.refresh(ver)
            return ver
        except DefinitionServiceError:
            self.db.rollback()
            raise
        except Exception:
            self.db.rollback()
            raise

    def activate_version(
        self,
        *,
        principal: AuthenticatedPrincipal,
        definition_id: str,
        version_number: int,
    ) -> TaskDefinition:
        """Atomically activate a validated version belonging to this tenant's definition."""
        try:
            defn = self.repo.get_definition(definition_id, principal.tenant_id, for_update=True)
            if defn is None:
                raise DefinitionNotFoundError(
                    f"Definition '{definition_id}' not found in tenant scope."
                )
            ver = self.repo.get_version(definition_id, version_number, principal.tenant_id)
            if ver is None:
                raise DefinitionNotFoundError(
                    f"Version {version_number} does not belong to definition '{definition_id}'."
                )
            if ver.execution_type not in SUPPORTED_EXECUTION_TYPES:
                raise DefinitionPolicyError(
                    f"Cannot activate unsupported execution_type '{ver.execution_type}'."
                )
            if ver.validation_status != "VALID":
                raise DefinitionValidationError(
                    f"Version {version_number} has validation_status '{ver.validation_status}' "
                    "and must be VALID before activation."
                )

            self.repo.set_active_version(
                definition_id=defn.id,
                tenant_id=principal.tenant_id,
                version_id=ver.id,
                status="ACTIVE",
                commit=False,
            )
            self.repo.record_audit_event(
                tenant_id=principal.tenant_id,
                definition_id=defn.id,
                version_id=ver.id,
                actor_id=principal.key_id,
                action="VERSION_ACTIVATED",
                metadata_json={"version": version_number, "content_hash": ver.content_hash},
                commit=False,
            )
            self.db.commit()
            self.db.refresh(defn)
            return defn
        except DefinitionServiceError:
            self.db.rollback()
            raise
        except Exception:
            self.db.rollback()
            raise

    def disable_definition(
        self, *, principal: AuthenticatedPrincipal, definition_id: str
    ) -> TaskDefinition:
        try:
            defn = self.repo.get_definition(definition_id, principal.tenant_id, for_update=True)
            if defn is None:
                raise DefinitionNotFoundError(
                    f"Definition '{definition_id}' not found in tenant scope."
                )
            defn.status = "DISABLED"
            defn.updated_at = get_utc_now()
            self.repo.record_audit_event(
                tenant_id=principal.tenant_id,
                definition_id=defn.id,
                version_id=defn.active_version_id,
                actor_id=principal.key_id,
                action="DEFINITION_DISABLED",
                metadata_json={},
                commit=False,
            )
            self.db.commit()
            self.db.refresh(defn)
            return defn
        except DefinitionServiceError:
            self.db.rollback()
            raise
        except Exception:
            self.db.rollback()
            raise

    def enable_definition(
        self, *, principal: AuthenticatedPrincipal, definition_id: str
    ) -> TaskDefinition:
        try:
            defn = self.repo.get_definition(definition_id, principal.tenant_id, for_update=True)
            if defn is None:
                raise DefinitionNotFoundError(
                    f"Definition '{definition_id}' not found in tenant scope."
                )
            if not defn.active_version_id:
                raise DefinitionConflictError(
                    "Cannot enable definition with no active version; activate a version first."
                )
            active_ver = self.repo.get_version_by_id(defn.active_version_id, principal.tenant_id)
            if active_ver is None or active_ver.validation_status != "VALID":
                raise DefinitionValidationError("Active version is missing or no longer VALID.")
            defn.status = "ACTIVE"
            defn.updated_at = get_utc_now()
            self.repo.record_audit_event(
                tenant_id=principal.tenant_id,
                definition_id=defn.id,
                version_id=defn.active_version_id,
                actor_id=principal.key_id,
                action="DEFINITION_ENABLED",
                metadata_json={"version": active_ver.version},
                commit=False,
            )
            self.db.commit()
            self.db.refresh(defn)
            return defn
        except DefinitionServiceError:
            self.db.rollback()
            raise
        except Exception:
            self.db.rollback()
            raise

    def list_audit_events(
        self, principal: AuthenticatedPrincipal, definition_id: str
    ) -> list[DefinitionAuditEvent]:
        self.get_definition(principal, definition_id)
        return self.repo.list_audit_events(definition_id, principal.tenant_id)

    def execute_definition(
        self,
        *,
        principal: AuthenticatedPrincipal,
        definition_id: str,
        request: DefinitionExecuteRequest,
    ) -> tuple[Task, bool]:
        """Execute a task definition version subject to R1-R3 execution guard constraints.

        Returns:
            tuple[Task, bool]: (task, created) where created=False indicates idempotent replay.
        """
        from taskforge.database.repositories.task_repository import TaskRepository
        from taskforge.definitions.validation import validate_schema_instance
        from taskforge.tasks.registry import TaskRegistry

        task_repo = TaskRepository(self.db)
        effective_timeout = request.timeout_seconds or 60

        # Optimistic fast-path check on idempotency key before acquiring locks
        if request.idempotency_key:
            existing = task_repo.get_by_idempotency_key(
                request.idempotency_key, tenant_id=principal.tenant_id
            )
            if existing:
                if existing.definition_id != definition_id:
                    raise IdempotencyConflictError(
                        "Idempotency key reused for a different definition."
                    )
                if request.version is not None and existing.definition_version_id is not None:
                    existing_ver = self.repo.get_version_by_id(
                        existing.definition_version_id, principal.tenant_id
                    )
                    if existing_ver and existing_ver.version != request.version:
                        raise IdempotencyConflictError(
                            "Idempotency key reused with a different version."
                        )
                expected_fp = compute_execution_request_fingerprint(
                    definition_id=existing.definition_id or definition_id,
                    definition_version_id=existing.definition_version_id or "",
                    input_payload=request.input,
                    priority=request.priority,
                    timeout_seconds=effective_timeout,
                )
                existing_fp = (
                    existing.payload.get("_request_fingerprint")
                    if isinstance(existing.payload, dict)
                    else None
                )
                if existing_fp and existing_fp != expected_fp:
                    raise IdempotencyConflictError(
                        "Idempotency key reused with different execution parameters."
                    )
                return existing, False

        try:
            # R3: Row-level lock on task_definitions to serialize submission with disable/activate
            defn = self.repo.get_definition(definition_id, principal.tenant_id, for_update=True)
            if defn is None:
                raise DefinitionNotFoundError(
                    f"Definition '{definition_id}' not found in tenant scope."
                )

            # Guard G11.4-01: Status must be ACTIVE
            if defn.status != "ACTIVE":
                raise DefinitionConflictError(
                    f"Definition '{definition_id}' is not ACTIVE (current status: {defn.status})."
                )

            # Resolve target version
            if request.version is not None:
                ver = self.repo.get_version(defn.id, request.version, principal.tenant_id)
                if ver is None:
                    raise DefinitionNotFoundError(
                        f"Version {request.version} not found for definition '{defn.id}'."
                    )
            else:
                if not defn.active_version_id:
                    raise DefinitionConflictError(f"Definition '{defn.id}' has no active version.")
                ver = self.repo.get_version_by_id(defn.active_version_id, principal.tenant_id)
                if ver is None:
                    raise DefinitionNotFoundError(
                        f"Active version '{defn.active_version_id}' not found."
                    )

            # Guard: validation status must be VALID
            if ver.validation_status != "VALID":
                raise DefinitionValidationError(
                    f"Definition version {ver.version} is not VALID (current status: {ver.validation_status})."
                )

            # Guard G11.4-02: execution_type must be builtin
            if ver.execution_type != "builtin":
                raise DefinitionPolicyError(
                    f"Execution type '{ver.execution_type}' is not supported. Only 'builtin' is permitted."
                )

            # Guard G11.4-03: Live TaskRegistry check
            if not TaskRegistry.has_handler(ver.task_type):
                raise DefinitionValidationError(
                    f"Handler for task type '{ver.task_type}' is not registered in live TaskRegistry."
                )

            # Guard R1: Content hash integrity verification
            expected_hash = compute_definition_content_hash(
                task_type=ver.task_type,
                execution_type=ver.execution_type,
                configuration_json=ver.configuration_json,
                input_schema_json=ver.input_schema_json,
                output_schema_json=ver.output_schema_json,
                execution_policy_json=ver.execution_policy_json,
            )
            if ver.content_hash != expected_hash:
                raise DefinitionIntegrityError(
                    f"Content hash mismatch for version {ver.id}: expected {expected_hash}, recorded {ver.content_hash}."
                )

            # Guard G11.4-05: Pre-validate input payload against version input schema
            if ver.input_schema_json:
                val_errors = validate_schema_instance(ver.input_schema_json, request.input)
                if val_errors:
                    raise DefinitionValidationError(f"Input validation failed: {val_errors[0]}")

            # R2: Compute deterministic request fingerprint
            fingerprint = compute_execution_request_fingerprint(
                definition_id=defn.id,
                definition_version_id=ver.id,
                input_payload=request.input,
                priority=request.priority,
                timeout_seconds=effective_timeout,
            )

            # Prepare effective task payload
            effective_payload = {
                **ver.configuration_json,
                **request.input,
                "_input": request.input,
                "_request_fingerprint": fingerprint,
            }

            # Create Task under the open definition lock transaction (commit=False)
            max_retries = (
                ver.execution_policy_json.get("max_retries", 3) if ver.execution_policy_json else 3
            )
            create_res = task_repo.create_task(
                task_name=request.task_name or f"{defn.name}:v{ver.version}",
                task_type=ver.task_type,
                payload=effective_payload,
                priority=request.priority,
                idempotency_key=request.idempotency_key,
                timeout_seconds=effective_timeout,
                max_retries=max_retries,
                tenant_id=principal.tenant_id,
                actor_id=principal.key_id,
                definition_id=defn.id,
                definition_version_id=ver.id,
                commit=False,
            )

            if not create_res.created:
                # Idempotency collision during submission
                existing_fp = (
                    create_res.task.payload.get("_request_fingerprint")
                    if isinstance(create_res.task.payload, dict)
                    else None
                )
                if existing_fp and existing_fp != fingerprint:
                    self.db.rollback()
                    raise IdempotencyConflictError(
                        "Idempotency key reused with different execution parameters."
                    )
                self.db.rollback()
                return create_res.task, False

            # G11.4-12: Record audit event in same transaction
            self.repo.record_audit_event(
                tenant_id=principal.tenant_id,
                definition_id=defn.id,
                version_id=ver.id,
                actor_id=principal.key_id,
                action="DEFINITION_EXECUTED",
                metadata_json={
                    "task_id": create_res.task.id,
                    "version": ver.version,
                    "fingerprint": fingerprint,
                },
                commit=False,
            )

            # Atomic commit of definition lock, task, outbox, and audit event
            self.db.commit()
            return create_res.task, True
        except DefinitionServiceError:
            self.db.rollback()
            raise
        except Exception:
            self.db.rollback()
            raise
