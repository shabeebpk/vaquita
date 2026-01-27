"""rename_search_run_candidates_to_ids

Revision ID: bedaad4f039a
Revises: 23785768504c
Create Date: 2026-01-18 21:05:36.933595

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'bedaad4f039a'
down_revision: Union[str, Sequence[str], None] = '23785768504c'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.alter_column('search_query_runs', 'candidates_fetched', new_column_name='fetched_paper_ids')
    op.alter_column('search_query_runs', 'candidates_accepted', new_column_name='accepted_paper_ids')
    op.alter_column('search_query_runs', 'candidates_rejected', new_column_name='rejected_paper_ids')


def downgrade() -> None:
    """Downgrade schema."""
    op.alter_column('search_query_runs', 'fetched_paper_ids', new_column_name='candidates_fetched')
    op.alter_column('search_query_runs', 'accepted_paper_ids', new_column_name='candidates_accepted')
    op.alter_column('search_query_runs', 'rejected_paper_ids', new_column_name='candidates_rejected')
