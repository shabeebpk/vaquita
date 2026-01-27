"""add FETCH_MORE literature pipeline tables

Revision ID: 068_fetch_more
Revises: 067894ffcddf
Create Date: 2026-01-10 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '068_fetch_more'
down_revision: Union[str, Sequence[str], None] = '067894ffcddf'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema: add SearchQuery, SearchQueryRun, and Paper updates."""
    
    # Update Paper table with new columns for FETCH_MORE pipeline
    op.add_column('papers', sa.Column('external_ids', sa.JSON(), nullable=True))
    op.add_column('papers', sa.Column('fingerprint', sa.String(), nullable=True))
    op.add_column('papers', sa.Column('used_for_research', sa.Boolean(), nullable=False, server_default='false'))
    
    # Create index on fingerprint for deduplication lookups
    op.create_index(op.f('ix_papers_fingerprint'), 'papers', ['fingerprint'], unique=False)
    
    # Create SearchQuery table
    op.create_table(
        'search_queries',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('job_id', sa.Integer(), nullable=False),
        sa.Column('hypothesis_signature', sa.String(), nullable=False),
        sa.Column('query_text', sa.Text(), nullable=False),
        sa.Column('resolved_domain', sa.String(), nullable=True),
        sa.Column('status', sa.String(), nullable=False),  # new, reusable, exhausted, blocked
        sa.Column('reputation_score', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('config_snapshot', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['job_id'], ['jobs.id'], ),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_search_queries_hypothesis_signature'), 'search_queries', ['hypothesis_signature'], unique=False)
    op.create_index(op.f('ix_search_queries_job_id'), 'search_queries', ['job_id'], unique=False)
    op.create_index(op.f('ix_search_queries_resolved_domain'), 'search_queries', ['resolved_domain'], unique=False)
    
    # Create SearchQueryRun table (append-only execution log)
    op.create_table(
        'search_query_runs',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('search_query_id', sa.Integer(), nullable=False),
        sa.Column('job_id', sa.Integer(), nullable=False),
        sa.Column('provider_used', sa.String(), nullable=False),
        sa.Column('reason', sa.String(), nullable=False),  # initial_attempt, reuse, expansion
        sa.Column('candidates_fetched', sa.Integer(), nullable=False),
        sa.Column('candidates_accepted', sa.Integer(), nullable=False),
        sa.Column('candidates_rejected', sa.Integer(), nullable=False),
        sa.Column('signal_delta', sa.Integer(), nullable=True),  # computed later: 1, 0, -1
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['job_id'], ['jobs.id'], ),
        sa.ForeignKeyConstraint(['search_query_id'], ['search_queries.id'], ),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_search_query_runs_job_id'), 'search_query_runs', ['job_id'], unique=False)
    op.create_index(op.f('ix_search_query_runs_search_query_id'), 'search_query_runs', ['search_query_id'], unique=False)


def downgrade() -> None:
    """Downgrade schema: remove FETCH_MORE tables and Paper column updates."""
    
    # Drop SearchQueryRun table
    op.drop_index(op.f('ix_search_query_runs_search_query_id'), table_name='search_query_runs')
    op.drop_index(op.f('ix_search_query_runs_job_id'), table_name='search_query_runs')
    op.drop_table('search_query_runs')
    
    # Drop SearchQuery table
    op.drop_index(op.f('ix_search_queries_resolved_domain'), table_name='search_queries')
    op.drop_index(op.f('ix_search_queries_job_id'), table_name='search_queries')
    op.drop_index(op.f('ix_search_queries_hypothesis_signature'), table_name='search_queries')
    op.drop_table('search_queries')
    
    # Remove columns from Paper table
    op.drop_index(op.f('ix_papers_fingerprint'), table_name='papers')
    op.drop_column('papers', 'used_for_research')
    op.drop_column('papers', 'fingerprint')
    op.drop_column('papers', 'external_ids')
