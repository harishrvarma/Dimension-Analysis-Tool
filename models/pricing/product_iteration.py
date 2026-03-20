from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Numeric
from sqlalchemy.orm import relationship
from models.base.base_model import BaseModel


class ProductIteration(BaseModel):
    __tablename__ = "pricing_product_iteration"

    iteration_id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    product_group_id = Column(Integer, ForeignKey("pricing_product_group.group_id"), nullable=False)
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
    x_axis = Column(Integer, ForeignKey("pricing_product_column.column_id"), nullable=True)
    y_axis = Column(Integer, ForeignKey("pricing_product_column.column_id"), nullable=True)
    z_axis = Column(Integer, ForeignKey("pricing_product_column.column_id"), nullable=True)
    x_axis_com = Column(Integer, ForeignKey("pricing_product_column.column_id"), nullable=True)
    y_axis_com = Column(Integer, ForeignKey("pricing_product_column.column_id"), nullable=True)
    z_axis_com = Column(Integer, ForeignKey("pricing_product_column.column_id"), nullable=True)

    x_axis_col = relationship("ProductColumn", foreign_keys=[x_axis])
    y_axis_col = relationship("ProductColumn", foreign_keys=[y_axis])
    z_axis_col = relationship("ProductColumn", foreign_keys=[z_axis])
    x_axis_com_col = relationship("ProductColumn", foreign_keys=[x_axis_com])
    y_axis_com_col = relationship("ProductColumn", foreign_keys=[y_axis_com])
    z_axis_com_col = relationship("ProductColumn", foreign_keys=[z_axis_com])

