"""add decision_results table

Revision ID: d4e5f6g7h8i9
Revises: c3d5e6f7g8h9
Create Date: 2026-01-01 00:00:00.000001

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = 'd4e5f6g7h8i9'
down_revision: Union[str, Sequence[str], None] = 'c3d5e6f7g8h9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'decision_results',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('job_id', sa.Integer(), nullable=False),
        sa.Column('decision_label', sa.String(), nullable=False),
        sa.Column('provider_used', sa.String(), nullable=False),
        sa.Column('measurements_snapshot', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column('fallback_used', sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column('fallback_reason', sa.Text(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(['job_id'], ['jobs.id'], ),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('ix_decision_results_job_id', 'decision_results', ['job_id'], unique=False)


def downgrade() -> None:
    op.drop_index('ix_decision_results_job_id', table_name='decision_results')
    op.drop_table('decision_results')
