from sqlalchemy import Column, String, Index, UniqueConstraint
from sqlalchemy.dialects.mysql import INTEGER, SMALLINT, BIGINT
from sqlalchemy.orm import relationship
from models.base.base_model import BaseModel


class CartonProduct(BaseModel):
    __tablename__ = "carton_product"

    __table_args__ = (
        UniqueConstraint('product_id', name='uniq_product_id'),
    )

    id = Column(BIGINT(unsigned=True), primary_key=True, autoincrement=True)

    product_id = Column(INTEGER(unsigned=True), nullable=False)
    web_id = Column(String(50), nullable=False, index=True)
    sku = Column(String(100), nullable=False, index=True)

    part_number = Column(String(255), nullable=True)
    category_id = Column(INTEGER(unsigned=True), nullable=True, index=True)
    category = Column(String(255), nullable=True)
    product_type = Column(String(255), nullable=True)

    brand_id = Column(INTEGER(unsigned=True), nullable=True, index=True)
    brand_name = Column(String(255), nullable=True)

    name = Column(String(255), nullable=False)

    product_url = Column(String(512), nullable=True)
    total_of_cartons = Column(SMALLINT(unsigned=True), nullable=False, default=1)
    image_url = Column(String(512), nullable=True)

    parts = relationship(
        "CartonProductPart",
        back_populates="product",
        cascade="all, delete-orphan"
    )
