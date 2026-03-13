from sqlalchemy import Column, Integer, String, Float, Enum
from models.base.base_model import BaseModel


class MatchingAttribute(BaseModel):
    __tablename__ = "matching_attribute"

    attribute_id = Column(Integer, primary_key=True, autoincrement=True)
    attribute_name = Column(String(100), nullable=False)
    default_weightage = Column(Float, nullable=False, default=0.0)
    attribute_type = Column(Enum('default', 'price', 'status'), nullable=False, default='default')
    competitor_attribute = Column(String(25), nullable=False, default='')
    data_type = Column(String(25), nullable=False, default='string')
