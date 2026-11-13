"""ORM models for Phase 11 Custom Task Definition Registry, versioning, and audit."""

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    JSON,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from taskforge.database.models import get_utc_now
from taskforge.database.session import Base


class TaskDefinition(Base):
    """Logical tenant-owned custom task definition."""

    __tablename__ = "task_definitions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(160), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    execution_type: Mapped[str] = mapped_column(String(32), nullable=False, default="builtin")
    status: Mapped[str] = mapped_column(String(24), nullable=False, default="DRAFT")
    active_version_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    created_by: Mapped[str] = mapped_column(String(128), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=get_utc_now, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=get_utc_now,
        onupdate=get_utc_now,
        nullable=False,
    )

    versions: Mapped[list["TaskDefinitionVersion"]] = relationship(
        "TaskDefinitionVersion",
        back_populates="definition",
        foreign_keys="TaskDefinitionVersion.definition_id",
        cascade="all, delete-orphan",
        order_by="TaskDefinitionVersion.version",
    )

    __table_args__ = (
        UniqueConstraint("tenant_id", "name", name="uq_task_definitions_tenant_name"),
        ForeignKeyConstraint(
            ["active_version_id", "id"],
            ["task_definition_versions.id", "task_definition_versions.definition_id"],
            name="fk_task_definitions_active_version",
            use_alter=True,
            ondelete="RESTRICT",
        ),
        Index("ix_task_definitions_tenant_status", "tenant_id", "status"),
        Index("ix_task_definitions_tenant_created", "tenant_id", "created_at"),
        Index("ix_task_definitions_tenant_updated", "tenant_id", "updated_at"),
    )


class TaskDefinitionVersion(Base):
    """Immutable executable version belonging to a TaskDefinition."""

    __tablename__ = "task_definition_versions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    definition_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("task_definitions.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    execution_type: Mapped[str] = mapped_column(String(32), nullable=False, default="builtin")
    task_type: Mapped[str] = mapped_column(String(100), nullable=False)
    configuration_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    input_schema_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    output_schema_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    execution_policy_json: Mapped[dict[str, Any]] = mapped_column(
        JSON, nullable=False, default=dict
    )
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    validation_status: Mapped[str] = mapped_column(
        String(24), nullable=False, default="UNVALIDATED"
    )
    validation_report_json: Mapped[dict[str, Any]] = mapped_column(
        JSON, nullable=False, default=dict
    )
    created_by: Mapped[str] = mapped_column(String(128), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=get_utc_now, nullable=False
    )

    definition: Mapped["TaskDefinition"] = relationship(
        "TaskDefinition",
        back_populates="versions",
        foreign_keys=[definition_id],
    )

    __table_args__ = (
        UniqueConstraint("definition_id", "version", name="uq_task_def_versions_def_ver"),
        UniqueConstraint("id", "definition_id", name="uq_task_def_versions_id_def"),
        Index("ix_task_def_versions_def_ver", "definition_id", "version"),
    )


class DefinitionAuditEvent(Base):
    """Tenant-scoped audit trail for definition and version management actions."""

    __tablename__ = "definition_audit_events"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    definition_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("task_definitions.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    version_id: Mapped[str | None] = mapped_column(
        String(36),
        ForeignKey("task_definition_versions.id", ondelete="SET NULL"),
        nullable=True,
    )
    actor_id: Mapped[str] = mapped_column(String(128), nullable=False)
    action: Mapped[str] = mapped_column(String(48), nullable=False)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=get_utc_now, nullable=False
    )

    __table_args__ = (
        Index("ix_def_audit_tenant_created", "tenant_id", "created_at"),
        Index("ix_def_audit_def_created", "definition_id", "created_at"),
    )
