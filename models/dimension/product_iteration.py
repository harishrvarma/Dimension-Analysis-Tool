from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Numeric
from models.base.base_model import BaseModel


class ProductIteration(BaseModel):
    __tablename__ = "dimension_product_iteration"

    iteration_id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    product_group_id = Column(Integer, ForeignKey("dimension_product_group.group_id"), nullable=False)
    algorithm = Column(String(50), nullable=False)
    brand = Column(String(255), nullable=True)
    category = Column(String(255), nullable=True)
    product_type = Column(String(255), nullable=True)
    eps = Column(Numeric(10, 1), nullable=True)
    sample = Column(Integer, nullable=True)
    timestamp = Column(DateTime, nullable=False)
    unique_number = Column(String(50), nullable=True)
    total_items = Column(Integer, nullable=True)
    analyzed_items = Column(Integer, nullable=True)
    pending_items = Column(Integer, nullable=True)
    outlier_items = Column(Integer, nullable=True)

