"""002_g83_outbox_available_at

Revision ID: 002_g83_outbox_available_at
Revises: 001_initial_schema
Create Date: 2026-11-08 18:45:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '002_g83_outbox_available_at'
down_revision: Union[str, None] = '001_initial_schema'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    columns = [c['name'] for c in inspector.get_columns('outbox_events')]
    if 'available_at' not in columns:
        op.add_column(
            'outbox_events',
            sa.Column('available_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now())
        )
    indexes = [idx['name'] for idx in inspector.get_indexes('outbox_events')]
    if 'ix_outbox_status_available' not in indexes:
        op.create_index('ix_outbox_status_available', 'outbox_events', ['status', 'available_at'])

    unique_constraints = [c['name'] for c in inspector.get_unique_constraints('task_attempts')]
    attempt_indexes = [idx['name'] for idx in inspector.get_indexes('task_attempts')]
    if 'uq_task_attempts_task_attempt' not in unique_constraints and 'uq_task_attempts_task_attempt' not in attempt_indexes:
        op.create_unique_constraint('uq_task_attempts_task_attempt', 'task_attempts', ['task_id', 'attempt_number'])


def downgrade() -> None:
    op.drop_constraint('uq_task_attempts_task_attempt', 'task_attempts', type_='unique')
    op.drop_index('ix_outbox_status_available', table_name='outbox_events')
    op.drop_column('outbox_events', 'available_at')

