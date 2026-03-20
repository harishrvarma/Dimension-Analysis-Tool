from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.mysql import TINYINT
from models.base.base_model import BaseModel


class ProductInsightConfig(BaseModel):
    __tablename__ = "pricing_product_insight_config"

    insight_config_id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    name = Column(String(100), nullable=True, default=None)
    x_axis = Column(Integer, ForeignKey("pricing_product_column.column_id"), nullable=True, default=None)
    y_axis = Column(Integer, ForeignKey("pricing_product_column.column_id"), nullable=True, default=None)
    z_axis = Column(Integer, ForeignKey("pricing_product_column.column_id"), nullable=True, default=None)
    x_axis_com = Column(Integer, ForeignKey("pricing_product_column.column_id"), nullable=True, default=None)
    y_axis_com = Column(Integer, ForeignKey("pricing_product_column.column_id"), nullable=True, default=None)
    z_axis_com = Column(Integer, ForeignKey("pricing_product_column.column_id"), nullable=True, default=None)
    is_default = Column(TINYINT, nullable=False, default=0)

    x_axis_column = relationship("models.pricing.product_column.ProductColumn", foreign_keys=[x_axis])
    y_axis_column = relationship("models.pricing.product_column.ProductColumn", foreign_keys=[y_axis])
    z_axis_column = relationship("models.pricing.product_column.ProductColumn", foreign_keys=[z_axis])
    x_axis_com_column = relationship("models.pricing.product_column.ProductColumn", foreign_keys=[x_axis_com])
    y_axis_com_column = relationship("models.pricing.product_column.ProductColumn", foreign_keys=[y_axis_com])
    z_axis_com_column = relationship("models.pricing.product_column.ProductColumn", foreign_keys=[z_axis_com])
