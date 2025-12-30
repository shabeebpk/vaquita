"""add hypotheses table

Revision ID: b7c3f4a9d1e2
Revises: a1f5c9e2b3d4
Create Date: 2025-12-28 00:00:00.000001

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = 'b7c3f4a9d1e2'
down_revision: Union[str, Sequence[str], None] = 'a1f5c9e2b3d4'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'hypotheses',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('job_id', sa.Integer(), nullable=False),
        sa.Column('source', sa.String(), nullable=False),
        sa.Column('target', sa.String(), nullable=False),
        sa.Column('path', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column('predicates', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column('explanation', sa.Text(), nullable=False),
        sa.Column('confidence', sa.Integer(), nullable=False),
        sa.Column('mode', sa.String(), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['job_id'], ['jobs.id'], ),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('ix_hypotheses_job_id', 'hypotheses', ['job_id'], unique=False)


def downgrade() -> None:
    op.drop_index('ix_hypotheses_job_id', table_name='hypotheses')
    op.drop_table('hypotheses')
