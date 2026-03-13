from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, Index, Numeric
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.mysql import TINYINT, DECIMAL, INTEGER
from models.base.base_model import BaseModel


class Product(BaseModel):
    __tablename__ = "dimension_product"

    __table_args__ = (
        Index('idx_product_system_product_id', 'system_product_id'),
    )

    product_id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    group_id = Column(Integer, ForeignKey("dimension_product_group.group_id"), nullable=False)

    system_product_id = Column(String(100), nullable=True, default=None)
    brand = Column(String(255), nullable=True, default=None)
    category = Column(String(255), nullable=True, default=None)
    product_type = Column(String(255), nullable=True, default=None)
    qb_code = Column(String(100), nullable=True, default=None)
    name = Column(String(500), nullable=True, default=None)

    
    ori_height = Column(Float, nullable=True, default=None)
    ori_width = Column(Float, nullable=True, default=None)
    ori_depth = Column(Float, nullable=True, default=None)

    height = Column(Float, nullable=True, default=None)
    width = Column(Float, nullable=True, default=None)
    depth = Column(Float, nullable=True, default=None)
    weight = Column(Float, nullable=True, default=None)

    base_image_url = Column(String(1000), nullable=True, default=None)
    product_url = Column(String(1000), nullable=True, default=None)

    created_date = Column(DateTime, nullable=True, default=None)

    dbs_status = Column(TINYINT, nullable=True, default=None)
    iqr_status = Column(TINYINT, nullable=True, default=None)
    iqr_height_status = Column(TINYINT, nullable=True, default=None)
    iqr_width_status = Column(TINYINT, nullable=True, default=None)
    iqr_depth_status = Column(TINYINT, nullable=True, default=None)

    eps = Column(DECIMAL(10, 2, unsigned=True), nullable=True, default=None)
    sample = Column(INTEGER(unsigned=True), nullable=True, default=None)

    final_status = Column(TINYINT, nullable=True, default=None)
    skip_status = Column(TINYINT, nullable=True, default=None)

    skip_status_updated_date = Column(DateTime, nullable=True, default=None)
    analyzed_date = Column(DateTime, nullable=True, default=None)

    dimension_status = Column(String(50), nullable=True, default=None)
    dimension_failed = Column(String(50), nullable=True, default=None)

    iteration_closed = Column(Integer, nullable=True, default=None)
    outlier_mode = Column(TINYINT, nullable=True, default=None, comment="0=Autometic, 1=Manually")

    group = relationship("ProductGroup", back_populates="products")
    eps = Column(Numeric(10, 2), nullable=True)
    sample = Column(Integer, nullable=True)

