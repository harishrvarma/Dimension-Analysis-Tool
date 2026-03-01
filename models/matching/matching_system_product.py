from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, Text
from sqlalchemy.orm import relationship

from models.base.base_model import BaseModel


class MatchingSystemProduct(BaseModel):
    __tablename__ = "matching_system_product"

    product_id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)

    system_product_id = Column(Integer, nullable=False)

    name = Column(String(255), nullable=False)
    sku = Column(String(150), nullable=False)
    part_number = Column(String(150), nullable=True, default=None)

    price = Column(Float, nullable=False)

    url = Column(Text, nullable=True, default=None)

    competitor_product_id = Column(
        Integer,
        ForeignKey("matching_competitor_product.competitor_product_id"),
        nullable=True,
        default=None
    )

    matched_date = Column(DateTime, nullable=True, default=None)
    review_status = Column(Integer, nullable=False, default=0)
    # ----------------------------------------
    # relationship
    # ----------------------------------------

    matched_competitor = relationship(
        "MatchingCompetitorProduct",
        back_populates="matched_system_products"
    )
