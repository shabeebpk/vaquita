"""add semantic_graphs table

Revision ID: a1f5c9e2b3d4
Revises: db9c870c0f1d
Create Date: 2025-12-28 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = 'a1f5c9e2b3d4'
down_revision: Union[str, Sequence[str], None] = 'db9c870c0f1d'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema: create semantic_graphs table with JSONB column and indexes."""
    op.create_table(
        'semantic_graphs',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('job_id', sa.Integer(), nullable=False),
        sa.Column('graph', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column('node_count', sa.Integer(), nullable=False),
        sa.Column('edge_count', sa.Integer(), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(['job_id'], ['jobs.id'], ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('job_id'),
    )
    # Create index on job_id for efficient lookups
    op.create_index(
        'ix_semantic_graphs_job_id',
        'semantic_graphs',
        ['job_id'],
        unique=False,
    )


def downgrade() -> None:
    """Downgrade schema: drop semantic_graphs table."""
    op.drop_index('ix_semantic_graphs_job_id', table_name='semantic_graphs')
    op.drop_table('semantic_graphs')
