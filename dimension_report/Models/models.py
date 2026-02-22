from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey
from sqlalchemy.orm import relationship
from datetime import datetime
from Models.database import Base

class ProductGroup(Base):
    __tablename__ = "product_group"
    
    group_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False)
    product_count = Column(Integer, default=0)
    created_date = Column(DateTime, default=datetime.utcnow)
    
    products = relationship("Product", back_populates="product_group")

class Product(Base):
    __tablename__ = "product"
    
    product_id = Column(Integer, primary_key=True, autoincrement=True)
    group_id = Column(Integer, ForeignKey("product_group.group_id"), nullable=False)
    system_product_id = Column(String(100))
    brand = Column(String(255))
    category = Column(String(255))
    product_type = Column(String(255))
    qb_code = Column(String(100))
    name = Column(String(500))
    height = Column(Float)
    width = Column(Float)
    depth = Column(Float)
    weight = Column(Float)
    base_image_url = Column(String(1000))
    product_url = Column(String(1000))
    created_date = Column(DateTime, default=datetime.utcnow)
    
    # Action flag columns
    dbs_status = Column(Integer)
    dbs_height_status = Column(Integer)
    dbs_width_status = Column(Integer)
    dbs_depth_status = Column(Integer)
    iqr_status = Column(Integer)
    iqr_height_status = Column(Integer)
    iqr_width_status = Column(Integer)
    iqr_depth_status = Column(Integer)
    final_status = Column(Integer)
    skip_status = Column(String(50))
    is_analyzed = Column(Integer)
    
    product_group = relationship("ProductGroup", back_populates="products")
