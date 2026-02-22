"""remove_iteraration_total_column

Revision ID: 301c5db61dcf
Revises: e81ca578744e
Create Date: 2026-02-19 11:45:04.675342

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '301c5db61dcf'
down_revision: Union[str, Sequence[str], None] = 'e81ca578744e'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Remove iteraration_total column."""
    op.drop_column('product', 'iteraration_total')


def downgrade() -> None:
    """Add back iteraration_total column."""
    op.add_column('product', sa.Column('iteraration_total', sa.Integer(), nullable=True))
