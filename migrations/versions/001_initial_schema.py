"""001_initial_schema

Revision ID: 001_initial_schema
Revises: 
Create Date: 2026-11-07 17:38:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '001_initial_schema'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. tasks table
    op.create_table(
        'tasks',
        sa.Column('id', sa.String(length=36), nullable=False),
        sa.Column('task_name', sa.String(length=128), nullable=False),
        sa.Column('task_type', sa.String(length=64), nullable=False),
        sa.Column('payload', sa.JSON(), nullable=False),
        sa.Column('status', sa.String(length=32), nullable=False),
        sa.Column('priority', sa.Integer(), nullable=False),
        sa.Column('idempotency_key', sa.String(length=128), nullable=True),
        sa.Column('timeout_seconds', sa.Integer(), nullable=False),
        sa.Column('max_retries', sa.Integer(), nullable=False),
        sa.Column('retry_count', sa.Integer(), nullable=False),
        sa.Column('scheduled_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('started_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('completed_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('lease_token', sa.String(length=64), nullable=True),
        sa.Column('lease_expires_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('idempotency_key')
    )
    op.create_index('ix_tasks_task_type', 'tasks', ['task_type'])
    op.create_index('ix_tasks_status', 'tasks', ['status'])
    op.create_index('ix_tasks_priority', 'tasks', ['priority'])
    op.create_index('ix_tasks_created_at', 'tasks', ['created_at'])
    op.create_index('ix_tasks_lease_expires_at', 'tasks', ['lease_expires_at'])
    op.create_index('ix_tasks_status_priority', 'tasks', ['status', 'priority'])
    op.create_index('ix_tasks_lease_recovery', 'tasks', ['status', 'lease_expires_at'])

    # 2. task_attempts table
    op.create_table(
        'task_attempts',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('task_id', sa.String(length=36), nullable=False),
        sa.Column('attempt_number', sa.Integer(), nullable=False),
        sa.Column('worker_id', sa.String(length=128), nullable=True),
        sa.Column('status', sa.String(length=32), nullable=False),
        sa.Column('error_message', sa.Text(), nullable=True),
        sa.Column('error_traceback', sa.Text(), nullable=True),
        sa.Column('started_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('completed_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('duration_ms', sa.Float(), nullable=True),
        sa.ForeignKeyConstraint(['task_id'], ['tasks.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('ix_task_attempts_task_id', 'task_attempts', ['task_id'])
    op.create_index('ix_task_attempts_task_attempt', 'task_attempts', ['task_id', 'attempt_number'])

    # 3. task_results table
    op.create_table(
        'task_results',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('task_id', sa.String(length=36), nullable=False),
        sa.Column('return_value', sa.JSON(), nullable=True),
        sa.Column('execution_metadata', sa.JSON(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(['task_id'], ['tasks.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('task_id')
    )
    op.create_index('ix_task_results_task_id', 'task_results', ['task_id'])

    # 4. task_events table
    op.create_table(
        'task_events',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('task_id', sa.String(length=36), nullable=False),
        sa.Column('event_type', sa.String(length=64), nullable=False),
        sa.Column('from_state', sa.String(length=32), nullable=True),
        sa.Column('to_state', sa.String(length=32), nullable=False),
        sa.Column('metadata_json', sa.JSON(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(['task_id'], ['tasks.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('ix_task_events_task_id', 'task_events', ['task_id'])
    op.create_index('ix_task_events_created_at', 'task_events', ['created_at'])

    # 5. outbox_events table
    op.create_table(
        'outbox_events',
        sa.Column('id', sa.String(length=36), nullable=False),
        sa.Column('aggregate_type', sa.String(length=64), nullable=False),
        sa.Column('aggregate_id', sa.String(length=36), nullable=False),
        sa.Column('event_type', sa.String(length=64), nullable=False),
        sa.Column('payload', sa.JSON(), nullable=False),
        sa.Column('status', sa.String(length=32), nullable=False),
        sa.Column('retry_count', sa.Integer(), nullable=False),
        sa.Column('error_message', sa.Text(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('published_at', sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('ix_outbox_events_aggregate_id', 'outbox_events', ['aggregate_id'])
    op.create_index('ix_outbox_events_status', 'outbox_events', ['status'])
    op.create_index('ix_outbox_events_created_at', 'outbox_events', ['created_at'])
    op.create_index('ix_outbox_status_created', 'outbox_events', ['status', 'created_at'])


def downgrade() -> None:
    op.drop_table('outbox_events')
    op.drop_table('task_events')
    op.drop_table('task_results')
    op.drop_table('task_attempts')
    op.drop_table('tasks')
