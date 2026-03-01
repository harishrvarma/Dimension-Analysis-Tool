from sqlalchemy import Column, String, ForeignKey
from sqlalchemy.dialects.mysql import INTEGER, SMALLINT, BIGINT, DECIMAL
from sqlalchemy.orm import relationship
from models.base.base_model import BaseModel


class CartonProductPart(BaseModel):
    __tablename__ = "carton_product_part"

    product_part_id = Column(BIGINT(unsigned=True), primary_key=True, autoincrement=True)

    product_id = Column(
        INTEGER(unsigned=True),
        ForeignKey("carton_product.product_id", ondelete="RESTRICT", onupdate="RESTRICT"),
        nullable=False,
        index=True
    )

    carton_number = Column(SMALLINT(unsigned=True), nullable=False)

    carton_part_number = Column(String(255), nullable=True)

    width = Column(DECIMAL(10, 2), nullable=True)
    length = Column(DECIMAL(10, 2), nullable=True)
    height = Column(DECIMAL(10, 2), nullable=True)
    weight = Column(DECIMAL(10, 2), nullable=True)

    product = relationship("CartonProduct", back_populates="parts")
