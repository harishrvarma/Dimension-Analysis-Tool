from models.base.base import Base
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.dialects.mysql import TINYINT
from sqlalchemy.orm import relationship


class ProductGroup(Base):
    __tablename__ = "product_group"

    group_id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    name = Column(String(255), nullable=False)
    product_count = Column(Integer, nullable=True, default=None)
    created_date = Column(DateTime, nullable=True, default=None)
    default_selected = Column(TINYINT(1), nullable=False, default=0, server_default="0", comment="Yes=1, No=0")

    products = relationship("Product", back_populates="group")
