from sqlalchemy import Column, Integer, String
from models.base.base_model import BaseModel


class ProductColumn(BaseModel):
    __tablename__ = "pricing_product_column"

    column_id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    name = Column(String(100), nullable=True, default=None)
    code = Column(String(100), nullable=True, default=None)
    symbol = Column(String(50), nullable=True, default=None)
