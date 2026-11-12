"""004_g11_definition_registry

Revision ID: 004_g11_definition_registry
Revises: 5a361f84b61f
Create Date: 2026-11-12 15:25:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "004_g11_definition_registry"
down_revision: str | Sequence[str] | None = "5a361f84b61f"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create Phase 11 definition registry tables and additive task provenance columns."""
    op.create_table(
        "task_definitions",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("tenant_id", sa.String(length=64), nullable=False),
        sa.Column("name", sa.String(length=160), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("execution_type", sa.String(length=32), nullable=False),
        sa.Column("status", sa.String(length=24), nullable=False),
        sa.Column("active_version_id", sa.String(length=36), nullable=True),
        sa.Column("created_by", sa.String(length=128), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("tenant_id", "name", name="uq_task_definitions_tenant_name"),
    )
    op.create_index("ix_task_definitions_tenant_id", "task_definitions", ["tenant_id"])
    op.create_index(
        "ix_task_definitions_tenant_status", "task_definitions", ["tenant_id", "status"]
    )
    op.create_index(
        "ix_task_definitions_tenant_created",
        "task_definitions",
        ["tenant_id", "created_at"],
    )
    op.create_index(
        "ix_task_definitions_tenant_updated",
        "task_definitions",
        ["tenant_id", "updated_at"],
    )

    op.create_table(
        "task_definition_versions",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("definition_id", sa.String(length=36), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("execution_type", sa.String(length=32), nullable=False),
        sa.Column("task_type", sa.String(length=100), nullable=False),
        sa.Column("configuration_json", sa.JSON(), nullable=False),
        sa.Column("input_schema_json", sa.JSON(), nullable=False),
        sa.Column("output_schema_json", sa.JSON(), nullable=True),
        sa.Column("execution_policy_json", sa.JSON(), nullable=False),
        sa.Column("content_hash", sa.String(length=64), nullable=False),
        sa.Column("validation_status", sa.String(length=24), nullable=False),
        sa.Column("validation_report_json", sa.JSON(), nullable=False),
        sa.Column("created_by", sa.String(length=128), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["definition_id"],
            ["task_definitions.id"],
            name="fk_task_def_versions_definition_id",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("definition_id", "version", name="uq_task_def_versions_def_ver"),
        sa.UniqueConstraint("id", "definition_id", name="uq_task_def_versions_id_def"),
    )
    op.create_index(
        "ix_task_definition_versions_definition_id",
        "task_definition_versions",
        ["definition_id"],
    )
    op.create_index(
        "ix_task_def_versions_def_ver",
        "task_definition_versions",
        ["definition_id", "version"],
    )

    # Circular composite ownership FK: active_version_id must belong to this same definition_id
    op.create_foreign_key(
        "fk_task_definitions_active_version",
        "task_definitions",
        "task_definition_versions",
        ["active_version_id", "id"],
        ["id", "definition_id"],
        ondelete="RESTRICT",
    )

    op.create_table(
        "definition_audit_events",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("tenant_id", sa.String(length=64), nullable=False),
        sa.Column("definition_id", sa.String(length=36), nullable=False),
        sa.Column("version_id", sa.String(length=36), nullable=True),
        sa.Column("actor_id", sa.String(length=128), nullable=False),
        sa.Column("action", sa.String(length=48), nullable=False),
        sa.Column("metadata_json", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["definition_id"],
            ["task_definitions.id"],
            name="fk_def_audit_definition_id",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["version_id"],
            ["task_definition_versions.id"],
            name="fk_def_audit_version_id",
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_definition_audit_events_tenant_id", "definition_audit_events", ["tenant_id"]
    )
    op.create_index(
        "ix_definition_audit_events_definition_id",
        "definition_audit_events",
        ["definition_id"],
    )
    op.create_index(
        "ix_def_audit_tenant_created",
        "definition_audit_events",
        ["tenant_id", "created_at"],
    )
    op.create_index(
        "ix_def_audit_def_created",
        "definition_audit_events",
        ["definition_id", "created_at"],
    )

    # Additive nullable provenance columns on existing tasks table
    op.add_column("tasks", sa.Column("definition_id", sa.String(length=36), nullable=True))
    op.add_column(
        "tasks", sa.Column("definition_version_id", sa.String(length=36), nullable=True)
    )
    op.create_index("ix_tasks_definition_id", "tasks", ["definition_id"])
    op.create_index("ix_tasks_definition_version_id", "tasks", ["definition_version_id"])
    op.create_check_constraint(
        "ck_tasks_definition_ref_pair",
        "tasks",
        "(definition_id IS NULL AND definition_version_id IS NULL) "
        "OR (definition_id IS NOT NULL AND definition_version_id IS NOT NULL)",
    )
    op.create_foreign_key(
        "fk_tasks_definition_version",
        "tasks",
        "task_definition_versions",
        ["definition_version_id", "definition_id"],
        ["id", "definition_id"],
        ondelete="RESTRICT",
    )


def downgrade() -> None:
    """Drop Phase 11 definition columns and tables in dependency-safe order."""
    op.drop_constraint("fk_tasks_definition_version", "tasks", type_="foreignkey")
    op.drop_constraint("ck_tasks_definition_ref_pair", "tasks", type_="check")
    op.drop_index("ix_tasks_definition_version_id", table_name="tasks")
    op.drop_index("ix_tasks_definition_id", table_name="tasks")
    op.drop_column("tasks", "definition_version_id")
    op.drop_column("tasks", "definition_id")

    op.drop_constraint(
        "fk_task_definitions_active_version", "task_definitions", type_="foreignkey"
    )

    op.drop_index("ix_def_audit_def_created", table_name="definition_audit_events")
    op.drop_index("ix_def_audit_tenant_created", table_name="definition_audit_events")
    op.drop_index(
        "ix_definition_audit_events_definition_id",
        table_name="definition_audit_events",
    )
    op.drop_index(
        "ix_definition_audit_events_tenant_id", table_name="definition_audit_events"
    )
    op.drop_table("definition_audit_events")

    op.drop_index("ix_task_def_versions_def_ver", table_name="task_definition_versions")
    op.drop_index(
        "ix_task_definition_versions_definition_id",
        table_name="task_definition_versions",
    )
    op.drop_table("task_definition_versions")

    op.drop_index("ix_task_definitions_tenant_updated", table_name="task_definitions")
    op.drop_index("ix_task_definitions_tenant_created", table_name="task_definitions")
    op.drop_index("ix_task_definitions_tenant_status", table_name="task_definitions")
    op.drop_index("ix_task_definitions_tenant_id", table_name="task_definitions")
    op.drop_table("task_definitions")

