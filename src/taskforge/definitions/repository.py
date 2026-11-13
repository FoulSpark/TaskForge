"""Persistence repository for Phase 11 Custom Task Definition Registry."""

import uuid
from typing import Any

from sqlalchemy import desc, func, select
from sqlalchemy.orm import Session

from taskforge.database.models import get_utc_now
from taskforge.definitions.models import (
    DefinitionAuditEvent,
    TaskDefinition,
    TaskDefinitionVersion,
)


class ImmutableVersionError(ValueError):
    """Raised when attempting to mutate an existing immutable TaskDefinitionVersion."""


class DefinitionRepository:
    """Tenant-isolated data access layer for definitions, versions, and audit events."""

    def __init__(self, db: Session):
        self.db = db

    def create_definition(
        self,
        *,
        tenant_id: str,
        name: str,
        description: str | None,
        execution_type: str,
        created_by: str,
        status: str = "DRAFT",
        commit: bool = True,
    ) -> TaskDefinition:
        now = get_utc_now()
        definition = TaskDefinition(
            id=str(uuid.uuid4()),
            tenant_id=tenant_id,
            name=name,
            description=description,
            execution_type=execution_type,
            status=status,
            active_version_id=None,
            created_by=created_by,
            created_at=now,
            updated_at=now,
        )
        self.db.add(definition)
        if commit:
            self.db.commit()
            self.db.refresh(definition)
        else:
            self.db.flush()
        return definition

    def get_definition(
        self, definition_id: str, tenant_id: str, *, for_update: bool = False
    ) -> TaskDefinition | None:
        stmt = select(TaskDefinition).where(
            TaskDefinition.id == definition_id,
            TaskDefinition.tenant_id == tenant_id,
        )
        if for_update:
            stmt = stmt.with_for_update()
        return self.db.scalars(stmt).first()

    def list_definitions(
        self,
        *,
        tenant_id: str,
        status: str | None = None,
        execution_type: str | None = None,
        search: str | None = None,
        sort_by: str = "updated_at",
        limit: int = 20,
        offset: int = 0,
    ) -> tuple[list[TaskDefinition], int]:
        stmt = select(TaskDefinition).where(TaskDefinition.tenant_id == tenant_id)
        count_stmt = select(func.count(TaskDefinition.id)).where(
            TaskDefinition.tenant_id == tenant_id
        )

        if status:
            stmt = stmt.where(TaskDefinition.status == status)
            count_stmt = count_stmt.where(TaskDefinition.status == status)
        if execution_type:
            stmt = stmt.where(TaskDefinition.execution_type == execution_type)
            count_stmt = count_stmt.where(TaskDefinition.execution_type == execution_type)
        if search:
            pattern = f"%{search.strip()}%"
            stmt = stmt.where(TaskDefinition.name.ilike(pattern))
            count_stmt = count_stmt.where(TaskDefinition.name.ilike(pattern))

        total = int(self.db.scalar(count_stmt) or 0)
        order_col = (
            TaskDefinition.created_at if sort_by == "created_at" else TaskDefinition.updated_at
        )
        stmt = stmt.order_by(desc(order_col), desc(TaskDefinition.id)).offset(offset).limit(limit)
        items = list(self.db.scalars(stmt).all())
        return items, total

    def create_version(
        self,
        *,
        definition_id: str,
        version: int,
        execution_type: str,
        task_type: str,
        configuration_json: dict[str, Any],
        input_schema_json: dict[str, Any],
        output_schema_json: dict[str, Any] | None,
        execution_policy_json: dict[str, Any],
        content_hash: str,
        validation_status: str = "UNVALIDATED",
        validation_report_json: dict[str, Any] | None = None,
        created_by: str,
        commit: bool = True,
    ) -> TaskDefinitionVersion:
        ver = TaskDefinitionVersion(
            id=str(uuid.uuid4()),
            definition_id=definition_id,
            version=version,
            execution_type=execution_type,
            task_type=task_type,
            configuration_json=configuration_json,
            input_schema_json=input_schema_json,
            output_schema_json=output_schema_json,
            execution_policy_json=execution_policy_json,
            content_hash=content_hash,
            validation_status=validation_status,
            validation_report_json=validation_report_json or {},
            created_by=created_by,
            created_at=get_utc_now(),
        )
        self.db.add(ver)
        if commit:
            self.db.commit()
            self.db.refresh(ver)
        else:
            self.db.flush()
        return ver

    def update_version(self, *_args: Any, **_kwargs: Any) -> None:
        """Explicitly prohibit mutating historical definition versions."""
        raise ImmutableVersionError(
            "TaskDefinitionVersion records are immutable; create a new version instead."
        )

    def set_version_validation(
        self,
        *,
        version: TaskDefinitionVersion,
        validation_status: str,
        validation_report_json: dict[str, Any],
        commit: bool = True,
    ) -> TaskDefinitionVersion:
        """Update only validation status/report without touching executable content."""
        version.validation_status = validation_status
        version.validation_report_json = validation_report_json
        if commit:
            self.db.commit()
            self.db.refresh(version)
        else:
            self.db.flush()
        return version

    def get_next_version_number(self, definition_id: str) -> int:
        max_ver = self.db.scalar(
            select(func.max(TaskDefinitionVersion.version)).where(
                TaskDefinitionVersion.definition_id == definition_id
            )
        )
        return int(max_ver or 0) + 1

    def get_version(
        self, definition_id: str, version_number: int, tenant_id: str
    ) -> TaskDefinitionVersion | None:
        stmt = (
            select(TaskDefinitionVersion)
            .join(
                TaskDefinition,
                TaskDefinitionVersion.definition_id == TaskDefinition.id,
            )
            .where(
                TaskDefinition.id == definition_id,
                TaskDefinition.tenant_id == tenant_id,
                TaskDefinitionVersion.version == version_number,
            )
        )
        return self.db.scalars(stmt).first()

    def get_version_by_id(self, version_id: str, tenant_id: str) -> TaskDefinitionVersion | None:
        stmt = (
            select(TaskDefinitionVersion)
            .join(
                TaskDefinition,
                TaskDefinitionVersion.definition_id == TaskDefinition.id,
            )
            .where(
                TaskDefinitionVersion.id == version_id,
                TaskDefinition.tenant_id == tenant_id,
            )
        )
        return self.db.scalars(stmt).first()

    def list_versions(self, definition_id: str, tenant_id: str) -> list[TaskDefinitionVersion]:
        stmt = (
            select(TaskDefinitionVersion)
            .join(
                TaskDefinition,
                TaskDefinitionVersion.definition_id == TaskDefinition.id,
            )
            .where(
                TaskDefinition.id == definition_id,
                TaskDefinition.tenant_id == tenant_id,
            )
            .order_by(TaskDefinitionVersion.version.asc())
        )
        return list(self.db.scalars(stmt).all())

    def set_active_version(
        self,
        *,
        definition_id: str,
        tenant_id: str,
        version_id: str | None,
        status: str | None = None,
        commit: bool = True,
    ) -> TaskDefinition | None:
        definition = self.get_definition(definition_id, tenant_id)
        if definition is None:
            return None
        if version_id is not None:
            ver = self.db.scalars(
                select(TaskDefinitionVersion).where(
                    TaskDefinitionVersion.id == version_id,
                    TaskDefinitionVersion.definition_id == definition.id,
                )
            ).first()
            if ver is None:
                raise ValueError(
                    f"Version '{version_id}' does not belong to definition '{definition_id}'."
                )
        definition.active_version_id = version_id
        if status is not None:
            definition.status = status
        definition.updated_at = get_utc_now()
        if commit:
            self.db.commit()
            self.db.refresh(definition)
        else:
            self.db.flush()
        return definition

    def record_audit_event(
        self,
        *,
        tenant_id: str,
        definition_id: str,
        version_id: str | None,
        actor_id: str,
        action: str,
        metadata_json: dict[str, Any] | None = None,
        commit: bool = True,
    ) -> DefinitionAuditEvent:
        evt = DefinitionAuditEvent(
            id=str(uuid.uuid4()),
            tenant_id=tenant_id,
            definition_id=definition_id,
            version_id=version_id,
            actor_id=actor_id,
            action=action,
            metadata_json=metadata_json or {},
            created_at=get_utc_now(),
        )
        self.db.add(evt)
        if commit:
            self.db.commit()
            self.db.refresh(evt)
        else:
            self.db.flush()
        return evt

    def list_audit_events(self, definition_id: str, tenant_id: str) -> list[DefinitionAuditEvent]:
        stmt = (
            select(DefinitionAuditEvent)
            .where(
                DefinitionAuditEvent.definition_id == definition_id,
                DefinitionAuditEvent.tenant_id == tenant_id,
            )
            .order_by(DefinitionAuditEvent.created_at.asc(), DefinitionAuditEvent.id.asc())
        )
        return list(self.db.scalars(stmt).all())
