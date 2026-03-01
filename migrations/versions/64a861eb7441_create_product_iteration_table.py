"""create_product_iteration_table

Revision ID: 64a861eb7441
Revises: a08cf1daa1a2
Create Date: 2026-02-23 13:21:59.258270

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision: str = '64a861eb7441'
down_revision: Union[str, Sequence[str], None] = 'a08cf1daa1a2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Add index on system_product_id column in product table
    op.create_index('idx_product_system_product_id', 'product', ['system_product_id'])
    
    # Create product_iteration table
    op.create_table(
        'product_iteration',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('system_product_id', sa.String(length=100), nullable=False),
        sa.Column('iteration_number', sa.Integer(), nullable=False),
        sa.Column('algo_id', sa.String(length=50), nullable=False),
        sa.Column('brand', sa.String(length=255), nullable=True),
        sa.Column('category', sa.String(length=255), nullable=True),
        sa.Column('eps', sa.Numeric(precision=10, scale=2), nullable=True),
        sa.Column('sample', sa.Integer(), nullable=True),
        sa.Column('cluster', sa.String(length=50), nullable=True),
        sa.Column('outlier_mode', mysql.TINYINT(), nullable=True, comment='0=Auto, 1=Manual'),
        sa.Column('status', mysql.TINYINT(), nullable=True, comment='0=Outlier, 1=Normal'),
        sa.Column('timezone', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['system_product_id'], ['product.system_product_id']),
        sa.PrimaryKeyConstraint('id')
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table('product_iteration')
    op.drop_index('idx_product_system_product_id', 'product')
