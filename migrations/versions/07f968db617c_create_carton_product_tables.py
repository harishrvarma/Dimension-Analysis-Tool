"""create carton product tables

Revision ID: 07f968db617c
Revises: 755823327965
Create Date: 2026-02-25 14:22:12.769014

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision: str = '07f968db617c'
down_revision: Union[str, Sequence[str], None] = '755823327965'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table('carton_product',
    sa.Column('id', mysql.BIGINT(unsigned=True), autoincrement=True, nullable=False),
    sa.Column('product_id', mysql.INTEGER(unsigned=True), nullable=False),
    sa.Column('web_id', sa.String(length=50), nullable=False),
    sa.Column('sku', sa.String(length=100), nullable=False),
    sa.Column('part_number', sa.String(length=255), nullable=True),
    sa.Column('category_id', mysql.INTEGER(unsigned=True), nullable=True),
    sa.Column('category', sa.String(length=255), nullable=True),
    sa.Column('product_type', sa.String(length=255), nullable=True),
    sa.Column('brand_id', mysql.INTEGER(unsigned=True), nullable=True),
    sa.Column('brand_name', sa.String(length=255), nullable=True),
    sa.Column('name', sa.String(length=255), nullable=False),
    sa.Column('product_url', sa.String(length=512), nullable=True),
    sa.Column('total_of_cartons', mysql.SMALLINT(unsigned=True), nullable=False),
    sa.Column('image_url', sa.String(length=512), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('product_id', name='uniq_product_id')
    )
    op.create_index(op.f('ix_carton_product_brand_id'), 'carton_product', ['brand_id'], unique=False)
    op.create_index(op.f('ix_carton_product_category_id'), 'carton_product', ['category_id'], unique=False)
    op.create_index(op.f('ix_carton_product_sku'), 'carton_product', ['sku'], unique=False)
    op.create_index(op.f('ix_carton_product_web_id'), 'carton_product', ['web_id'], unique=False)
    op.create_table('carton_product_part',
    sa.Column('product_part_id', mysql.BIGINT(unsigned=True), autoincrement=True, nullable=False),
    sa.Column('product_id', mysql.INTEGER(unsigned=True), nullable=False),
    sa.Column('carton_number', mysql.SMALLINT(unsigned=True), nullable=False),
    sa.Column('carton_part_number', sa.String(length=255), nullable=True),
    sa.Column('width', mysql.DECIMAL(precision=10, scale=2), nullable=True),
    sa.Column('length', mysql.DECIMAL(precision=10, scale=2), nullable=True),
    sa.Column('height', mysql.DECIMAL(precision=10, scale=2), nullable=True),
    sa.Column('weight', mysql.DECIMAL(precision=10, scale=2), nullable=True),
    sa.ForeignKeyConstraint(['product_id'], ['carton_product.product_id'], onupdate='RESTRICT', ondelete='RESTRICT'),
    sa.PrimaryKeyConstraint('product_part_id')
    )
    op.create_index(op.f('ix_carton_product_part_product_id'), 'carton_product_part', ['product_id'], unique=False)


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table('carton_product_part')
    op.drop_table('carton_product')
