from sqlalchemy import Column, Integer, String, Float
from models.base.base_model import BaseModel


class MatchingConfigurationGroup(BaseModel):
    __tablename__ = "matching_configuration_group"

    configuration_id = Column(Integer, primary_key=True, autoincrement=True)
    algorithm_id = Column(String(50), nullable=False)
    attribute_id = Column(Integer, nullable=False)
    attribute_value = Column(Float, nullable=False)
