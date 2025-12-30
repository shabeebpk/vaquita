"""add reasoning_queries table and query_id on hypotheses

Revision ID: c3d5e6f7g8h9
Revises: b7c3f4a9d1e2
Create Date: 2025-12-28 00:00:00.000002

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = 'c3d5e6f7g8h9'
down_revision: Union[str, Sequence[str], None] = 'b7c3f4a9d1e2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create reasoning_queries table
    op.create_table(
        'reasoning_queries',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('job_id', sa.Integer(), nullable=False),
        sa.Column('query_text', sa.Text(), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['job_id'], ['jobs.id'], ),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('ix_reasoning_queries_job_id', 'reasoning_queries', ['job_id'], unique=False)

    # Add nullable query_id to hypotheses
    op.add_column('hypotheses', sa.Column('query_id', sa.Integer(), nullable=True))
    op.create_foreign_key('fk_hypotheses_query_id', 'hypotheses', 'reasoning_queries', ['query_id'], ['id'])


def downgrade() -> None:
    # Drop FK and column
    op.drop_constraint('fk_hypotheses_query_id', 'hypotheses', type_='foreignkey')
    op.drop_column('hypotheses', 'query_id')

    # Drop reasoning_queries table
    op.drop_index('ix_reasoning_queries_job_id', table_name='reasoning_queries')
    op.drop_table('reasoning_queries')
