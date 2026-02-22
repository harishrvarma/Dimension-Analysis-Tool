"""updated_iteraration_closed_column_to_iteration_closed

Revision ID: e81ca578744e
Revises: 34c5e4d31996
Create Date: 2026-02-19 11:11:39.926325

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = 'e81ca578744e'
down_revision: Union[str, Sequence[str], None] = '34c5e4d31996'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Rename iteraration_closed to iteration_closed."""
    op.alter_column('product', 'iteraration_closed', 
                    new_column_name='iteration_closed',
                    existing_type=sa.Integer(),
                    existing_nullable=True)


def downgrade() -> None:
    """Rename iteration_closed back to iteraration_closed."""
    op.alter_column('product', 'iteration_closed', 
                    new_column_name='iteraration_closed',
                    existing_type=sa.Integer(),
                    existing_nullable=True)
