from models.base.base import SessionLocal
from models.matching.matching_scores import MatchingScore
from models.matching.matching_score_attributes import MatchingScoreAttribute
from sqlalchemy import text


class ScoreService:
    """Service to manage score storage"""
    
    def __init__(self, session=None):
        self.session = session or SessionLocal()
        self._should_close = session is None
    
    def __del__(self):
        if self._should_close and hasattr(self, 'session'):
            self.session.close()
    
    def save_score(self, system_product_id, competitor_product_id, algorithm_id, 
                   total_score, score_status, attribute_scores, hard_refresh=False):
        """
        Save score:
        - matching_scores: ONE record per (product, competitor) - stores final score
        - matching_score_attributes: MULTIPLE records per algorithm
        """
        # Check if main score exists (unique on product + competitor only)
        existing_score = self.session.query(MatchingScore).filter(
            MatchingScore.system_product_id == system_product_id,
            MatchingScore.competitor_product_id == competitor_product_id
        ).first()
        
        if existing_score:
            # Record exists - update algorithm_id and total_score for the latest run
            existing_score.algorithm_id = algorithm_id
            existing_score.total_score = total_score
            existing_score.score_status = score_status
            self.session.flush()
            matching_score_id = existing_score.score_id
        else:
            # Insert new main score
            new_score = MatchingScore(
                system_product_id=system_product_id,
                competitor_product_id=competitor_product_id,
                algorithm_id=algorithm_id,
                total_score=total_score,
                score_status=score_status
            )
            self.session.add(new_score)
            self.session.flush()
            matching_score_id = new_score.score_id
        
        # Insert/Update attribute scores for THIS algorithm
        for attr_id, score_val in attribute_scores.items():
            check_sql = text(
                "SELECT score_attribute_id FROM matching_score_attributes "
                "WHERE system_product_id = :prod_id "
                "AND competitor_product_id = :comp_id "
                "AND attribute_id = :attr_id "
                "AND algorithm_id = :algo_id "
                "LIMIT 1"
            )
            result = self.session.execute(check_sql, {
                'prod_id': system_product_id,
                'comp_id': competitor_product_id,
                'attr_id': attr_id,
                'algo_id': algorithm_id
            }).fetchone()
            
            if result:
                # Update existing attribute score
                update_sql = text(
                    "UPDATE matching_score_attributes "
                    "SET score = :score "
                    "WHERE score_attribute_id = :score_attr_id"
                )
                self.session.execute(update_sql, {
                    'score': score_val,
                    'score_attr_id': result[0]
                })
            else:
                # Insert new attribute score
                insert_sql = text(
                    "INSERT INTO matching_score_attributes "
                    "(score_id, attribute_id, system_product_id, competitor_product_id, algorithm_id, score) "
                    "VALUES (:score_id, :attr_id, :prod_id, :comp_id, :algo_id, :score)"
                )
                self.session.execute(insert_sql, {
                    'score_id': matching_score_id,
                    'attr_id': attr_id,
                    'prod_id': system_product_id,
                    'comp_id': competitor_product_id,
                    'algo_id': algorithm_id,
                    'score': score_val
                })
        
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
            MatchingScoreAttribute.score_id == score.score_id
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
