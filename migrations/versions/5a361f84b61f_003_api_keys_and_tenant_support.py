"""003_api_keys_and_tenant_support

Revision ID: 5a361f84b61f
Revises: 002_g83_outbox_available_at
Create Date: 2026-11-10 01:02:02.327827

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "5a361f84b61f"
down_revision: str | Sequence[str] | None = "002_g83_outbox_available_at"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema: add api_keys table, and tenant_id to tasks and task_events."""
    # 1. Create api_keys table
    op.create_table(
        "api_keys",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("key_id", sa.String(length=32), nullable=False),
        sa.Column("hashed_secret", sa.String(length=64), nullable=False),
        sa.Column("tenant_id", sa.String(length=64), nullable=False),
        sa.Column("name", sa.String(length=128), nullable=False),
        sa.Column("scopes", sa.JSON(), nullable=False),
        sa.Column("is_active", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_used_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("key_id", name="uq_api_keys_key_id"),
    )
    op.create_index("ix_api_keys_key_id", "api_keys", ["key_id"])
    op.create_index("ix_api_keys_tenant_id", "api_keys", ["tenant_id"])

    # 2. Add tenant_id to tasks with default 'default' for backwards compatibility
    op.add_column(
        "tasks",
        sa.Column("tenant_id", sa.String(length=64), server_default="default", nullable=False),
    )
    op.create_index("ix_tasks_tenant_id", "tasks", ["tenant_id"])
    op.create_index("ix_tasks_tenant_status", "tasks", ["tenant_id", "status"])
    op.create_index("ix_tasks_tenant_created", "tasks", ["tenant_id", "created_at"])

    # 3. Add actor_id and tenant_id to task_events for auditability
    op.add_column(
        "task_events",
        sa.Column("tenant_id", sa.String(length=64), server_default="default", nullable=False),
    )
    op.add_column("task_events", sa.Column("actor_id", sa.String(length=128), nullable=True))
    op.create_index("ix_task_events_tenant_id", "task_events", ["tenant_id"])
    op.create_index("ix_task_events_tenant_created", "task_events", ["tenant_id", "created_at"])

    # Backfill/synchronize existing task events from parent task tenant assignment
    op.execute(
        "UPDATE task_events SET tenant_id = tasks.tenant_id FROM tasks WHERE task_events.task_id = tasks.id"
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("ix_task_events_tenant_created", table_name="task_events", if_exists=True)
    op.drop_index("ix_task_events_tenant_id", table_name="task_events", if_exists=True)
    op.drop_column("task_events", "actor_id")
    op.drop_column("task_events", "tenant_id")

    op.drop_index("ix_tasks_tenant_created", table_name="tasks", if_exists=True)
    op.drop_index("ix_tasks_tenant_status", table_name="tasks", if_exists=True)
    op.drop_index("ix_tasks_tenant_id", table_name="tasks", if_exists=True)
    op.drop_column("tasks", "tenant_id")

    op.drop_index("ix_api_keys_tenant_id", table_name="api_keys", if_exists=True)
    op.drop_index("ix_api_keys_key_id", table_name="api_keys", if_exists=True)
    op.drop_table("api_keys")
