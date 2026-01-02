"""merge filter and decision migrations

Revision ID: 52c7d552ab4b
Revises: d2284c118fb0, d4e5f6g7h8i9
Create Date: 2026-01-01 22:39:52.284304

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '52c7d552ab4b'
down_revision: Union[str, Sequence[str], None] = ('d2284c118fb0', 'd4e5f6g7h8i9')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
