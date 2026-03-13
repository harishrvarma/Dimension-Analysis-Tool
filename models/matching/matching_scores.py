from sqlalchemy import Column, Integer, Float, String
from sqlalchemy.orm import relationship
from models.base.base_model import BaseModel


class MatchingScore(BaseModel):
    __tablename__ = "matching_scores"

    score_id = Column(Integer, primary_key=True, autoincrement=True)
    system_product_id = Column(Integer, nullable=False)
    competitor_product_id = Column(Integer, nullable=False)
    algorithm_id = Column(String(50), nullable=False)
    total_score = Column(Float, nullable=True)
    score_status = Column(String(20), nullable=True)

    score_attributes = relationship("MatchingScoreAttribute", back_populates="matching_score", cascade="all, delete-orphan", foreign_keys="[MatchingScoreAttribute.score_id]")
