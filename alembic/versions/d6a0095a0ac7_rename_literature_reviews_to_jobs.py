"""rename literature_reviews to jobs

Revision ID: d6a0095a0ac7
Revises: edcb3585d81c
Create Date: 2025-12-23 13:26:13.632930

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd6a0095a0ac7'
down_revision: Union[str, Sequence[str], None] = 'edcb3585d81c'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.rename_table("literature_reviews", "jobs")
    op.alter_column("files", "literature_review_id", new_column_name="job_id")
    op.drop_constraint("files_literature_review_id_fkey", "files", type_="foreignkey")
    op.create_foreign_key(
        None,
        "files",
        "jobs",
        ["job_id"],
        ["id"]
    )



def downgrade() -> None:
    op.drop_constraint(None, "files", type_="foreignkey")
    op.alter_column("files", "job_id", new_column_name="literature_review_id")
    op.create_foreign_key(
        "files_literature_review_id_fkey",
        "files",
        "literature_reviews",
        ["literature_review_id"],
        ["id"]
    )
    op.rename_table("jobs", "literature_reviews")

