from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.dialects.mysql import TINYINT
from models.base.base_model import BaseModel


class DimensionProductIterationItem(BaseModel):
    __tablename__ = "dimension_product_iteration_item"

    id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    iteration_id = Column(Integer, ForeignKey("dimension_product_iteration.iteration_id"), nullable=False)
    system_product_id = Column(String(100), ForeignKey("dimension_product.system_product_id"), nullable=False)
    brand = Column(String(255), nullable=True)
    category = Column(String(255), nullable=True)
    product_type = Column(String(255), nullable=True)
    cluster = Column(String(50), nullable=True)
    outlier_mode = Column(TINYINT, nullable=True, default=None, comment="0=Auto, 1=Manual")
    status = Column(TINYINT, nullable=True, default=None, comment="0=Outlier, 1=Normal")
