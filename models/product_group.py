from models.base.base import Base
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.orm import relationship


class ProductGroup(Base):
    __tablename__ = "product_group"

    group_id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    name = Column(String(255), nullable=False)
    product_count = Column(Integer, nullable=True, default=None)
    created_date = Column(DateTime, nullable=True, default=None)

    products = relationship("Product", back_populates="group")
