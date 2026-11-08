"""002_g83_outbox_available_at

Revision ID: 002_g83_outbox_available_at
Revises: 001_initial_schema
Create Date: 2026-11-08 18:45:00.000000

"""
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = '002_g83_outbox_available_at'
down_revision: str | None = '001_initial_schema'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    columns = [c['name'] for c in inspector.get_columns('outbox_events')]
    if 'available_at' not in columns:
        op.add_column(
            'outbox_events',
            sa.Column('available_at', sa.DateTime(timezone=True), nullable=True)
        )

    # Historical Data Backfill:
    # 1. Backfill available_at from payload scheduled_at for events with future schedules.
    # 2. Backfill remaining unscheduled events to created_at (or NOW() fallback) to preserve immediate eligibility.
    if conn.dialect.name == 'postgresql':
        op.execute(sa.text("""
            UPDATE outbox_events
            SET available_at = (payload->>'scheduled_at')::timestamptz
            WHERE (payload->>'scheduled_at') IS NOT NULL
              AND (payload->>'scheduled_at') != ''
              AND (payload->>'scheduled_at') != 'null';
        """))
        op.execute(sa.text("""
            UPDATE outbox_events
            SET available_at = COALESCE(created_at, NOW())
            WHERE available_at IS NULL;
        """))
        op.alter_column('outbox_events', 'available_at', nullable=False)
    elif conn.dialect.name == 'sqlite':
        op.execute(sa.text("""
            UPDATE outbox_events
            SET available_at = json_extract(payload, '$.scheduled_at')
            WHERE json_extract(payload, '$.scheduled_at') IS NOT NULL
              AND json_extract(payload, '$.scheduled_at') != ''
              AND json_extract(payload, '$.scheduled_at') != 'null';
        """))
        op.execute(sa.text("""
            UPDATE outbox_events
            SET available_at = COALESCE(created_at, CURRENT_TIMESTAMP)
            WHERE available_at IS NULL;
        """))

    indexes = [idx['name'] for idx in inspector.get_indexes('outbox_events')]
    if 'ix_outbox_status_available' not in indexes:
        op.create_index('ix_outbox_status_available', 'outbox_events', ['status', 'available_at'])

    unique_constraints = [c['name'] for c in inspector.get_unique_constraints('task_attempts')]
    attempt_indexes = [idx['name'] for idx in inspector.get_indexes('task_attempts')]
    if 'uq_task_attempts_task_attempt' not in unique_constraints and 'uq_task_attempts_task_attempt' not in attempt_indexes:
        op.create_unique_constraint('uq_task_attempts_task_attempt', 'task_attempts', ['task_id', 'attempt_number'])


def downgrade() -> None:
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    unique_constraints = [c['name'] for c in inspector.get_unique_constraints('task_attempts')]
    if 'uq_task_attempts_task_attempt' in unique_constraints:
        op.drop_constraint('uq_task_attempts_task_attempt', 'task_attempts', type_='unique')
    attempt_indexes = [idx['name'] for idx in inspector.get_indexes('task_attempts')]
    if 'uq_task_attempts_task_attempt' in attempt_indexes:
        op.drop_index('uq_task_attempts_task_attempt', table_name='task_attempts')
    indexes = [idx['name'] for idx in inspector.get_indexes('outbox_events')]
    if 'ix_outbox_status_available' in indexes:
        op.drop_index('ix_outbox_status_available', table_name='outbox_events')
    columns = [c['name'] for c in inspector.get_columns('outbox_events')]
    if 'available_at' in columns:
        op.drop_column('outbox_events', 'available_at')

