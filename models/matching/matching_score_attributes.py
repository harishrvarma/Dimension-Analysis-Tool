from sqlalchemy import Column, Integer, Float, String, ForeignKey
from sqlalchemy.orm import relationship
from models.base.base_model import BaseModel


class MatchingScoreAttribute(BaseModel):
    __tablename__ = "matching_score_attributes"

    score_attribute_id = Column(Integer, primary_key=True, autoincrement=True)
    score_id = Column(Integer, ForeignKey("matching_scores.score_id"), nullable=False)
    attribute_id = Column(Integer, nullable=False)
    system_product_id = Column(Integer, nullable=False)
    competitor_product_id = Column(Integer, nullable=False)
    algorithm_id = Column(String(50), nullable=False)
    score = Column(Float, nullable=True)

    matching_score = relationship("MatchingScore", back_populates="score_attributes")
