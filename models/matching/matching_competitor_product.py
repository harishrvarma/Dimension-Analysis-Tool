from sqlalchemy import Column, Integer, String, Float, Text ,ForeignKey
from sqlalchemy.orm import relationship

from models.base.base_model import BaseModel


class MatchingCompetitorProduct(BaseModel):
    __tablename__ = "matching_competitor_product"

    competitor_product_id = Column(Integer, nullable=False, primary_key=True, autoincrement=True)

    system_product_id = Column(Integer,nullable=False)

    competitor_id = Column(Integer, nullable=False)

    sku = Column(String(150), nullable=False)
    part_number = Column(String(150), nullable=True, default=None)

    price = Column(Float, nullable=False)

    url = Column(Text, nullable=True, default=None)

    score = Column(Float, nullable=True, default=None)
    score_status = Column(String(20), nullable=True, default=None)
    
    sku_score = Column(Float, nullable=True, default=None)
    url_score = Column(Float, nullable=True, default=None)
    price_score = Column(Float, nullable=True, default=None)

    # ----------------------------------------
    # relationship
    # ----------------------------------------

    matched_system_products = relationship(
        "MatchingSystemProduct",
        back_populates="matched_competitor"
    )
