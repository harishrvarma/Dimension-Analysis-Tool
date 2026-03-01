from sqlalchemy import Column, Integer, String
from models.base.base_model import BaseModel


class MatchingConfigurationGroup(BaseModel):
    __tablename__ = "matching_configuration_group"

    configuration_id = Column(Integer, primary_key=True, autoincrement=True)
    matching_attribute_id = Column(Integer, nullable=False)
    attribute_value = Column(String(255), nullable=False)
    group_id = Column(String(50), nullable=False)
