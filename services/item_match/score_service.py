from models.base.base import SessionLocal
from models.matching.matching_scores import MatchingScore
from models.matching.matching_score_attributes import MatchingScoreAttribute
from sqlalchemy import text


class ScoreService:
    """Service to manage score storage in new schema"""
    
    def __init__(self, session=None):
        self.session = session or SessionLocal()
        self._should_close = session is None
    
    def __del__(self):
        if self._should_close and hasattr(self, 'session'):
            self.session.close()
    
    def save_score(self, system_product_id, competitor_product_id, configuration_group_id, 
                   total_score, score_status, attribute_scores):
        """
        Save score in new schema (check if exists, update or insert)
        
        Args:
            system_product_id: matching_system_product.product_id
            competitor_product_id: matching_competitor_product.competitor_product_id
            configuration_group_id: configuration group id
            total_score: calculated total score
            score_status: 'Matched', 'Review', 'Not Matched'
            attribute_scores: dict {matching_attribute_id: score_value}
        """
        # Check if score already exists
        existing_score = self.session.query(MatchingScore).filter(
            MatchingScore.system_product_id == system_product_id,
            MatchingScore.competitor_product_id == competitor_product_id,
            MatchingScore.configuration_group_id == configuration_group_id
        ).first()
        
        if existing_score:
            # Update existing score
            existing_score.total_score = total_score
            existing_score.score_status = score_status
            matching_score_id = existing_score.score_id
            
            # Delete old attribute scores
            self.session.query(MatchingScoreAttribute).filter(
                MatchingScoreAttribute.matching_score_id == matching_score_id
            ).delete()
        else:
            # Insert new score
            new_score = MatchingScore(
                system_product_id=system_product_id,
                competitor_product_id=competitor_product_id,
                configuration_group_id=configuration_group_id,
                total_score=total_score,
                score_status=score_status
            )
            self.session.add(new_score)
            self.session.flush()
            matching_score_id = new_score.score_id
        
        # Insert attribute scores
        for attr_id, score_val in attribute_scores.items():
            attr_score = MatchingScoreAttribute(
                matching_score_id=matching_score_id,
                attribute_id=attr_id,
                system_product_id=system_product_id,
                competitor_product_id=competitor_product_id,
                score=score_val
            )
            self.session.add(attr_score)
        
        self.session.commit()
        return matching_score_id
    
    def get_scores(self, system_product_id):
        """Get all scores for a system product"""
        return self.session.query(MatchingScore).filter(
            MatchingScore.system_product_id == system_product_id
        ).all()
    
    def get_score_with_attributes(self, system_product_id, competitor_product_id):
        """Get score with all attribute scores"""
        score = self.session.query(MatchingScore).filter(
            MatchingScore.system_product_id == system_product_id,
            MatchingScore.competitor_product_id == competitor_product_id
        ).first()
        
        if not score:
            return None
        
        attr_scores = self.session.query(MatchingScoreAttribute).filter(
            MatchingScoreAttribute.matching_score_id == score.score_id
        ).all()
        
        return {
            'total_score': score.total_score,
            'score_status': score.score_status,
            'attributes': {attr.attribute_id: attr.score for attr in attr_scores}
        }
    
    def reset_scores(self, product_ids=None):
        """Reset all scores or for specific products"""
        if product_ids:
            self.session.query(MatchingScore).filter(
                MatchingScore.system_product_id.in_(product_ids)
            ).delete(synchronize_session=False)
        else:
            self.session.query(MatchingScore).delete()
        self.session.commit()
