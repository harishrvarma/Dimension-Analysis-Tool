from models.base.base import SessionLocal
from models.matching.matching_configuration_group import MatchingConfigurationGroup
from sqlalchemy import text


class ConfigurationService:
    """Service to manage algorithm configurations"""
    
    def __init__(self, session=None):
        self.session = session or SessionLocal()
        self._should_close = session is None
    
    def __del__(self):
        if self._should_close and hasattr(self, 'session'):
            self.session.close()
    
    def update_configuration(self, algorithm_id, weights, thresholds, price_config=None):
        """
        Update configuration for an algorithm (update existing or insert new)
        
        Args:
            algorithm_id: algorithm identifier (e.g., 'tfidf', 'custom')
            weights: dict {attribute_name: weight_value}
            thresholds: dict {threshold_name: threshold_value}
            price_config: dict {price_param_name: value}
        """
        conn = self.session.connection()
        
        # Get attribute IDs for all attributes being updated
        attr_map = {}
        all_attrs = list(weights.keys()) + list(thresholds.keys())
        if price_config:
            all_attrs.extend(price_config.keys())
        
        for attr_name in all_attrs:
            attr_query = text("SELECT attribute_id FROM matching_attribute WHERE attribute_name = :name")
            result = conn.execute(attr_query, {'name': attr_name})
            row = result.fetchone()
            if row:
                attr_map[attr_name] = row[0]
        
        # Update or insert configuration for each attribute
        all_config = {**weights, **thresholds}
        if price_config:
            all_config.update(price_config)
        
        for attr_name, value in all_config.items():
            if attr_name in attr_map:
                attr_id = attr_map[attr_name]
                
                # Check if exists
                check_query = text("SELECT 1 FROM matching_configuration_group WHERE algorithm_id = :algo_id AND attribute_id = :attr_id")
                exists = conn.execute(check_query, {'algo_id': algorithm_id, 'attr_id': attr_id}).fetchone()
                
                if exists:
                    # Update existing
                    update_query = text("UPDATE matching_configuration_group SET attribute_value = :value WHERE algorithm_id = :algo_id AND attribute_id = :attr_id")
                    conn.execute(update_query, {'value': float(value), 'algo_id': algorithm_id, 'attr_id': attr_id})
                else:
                    # Insert new
                    insert_query = text("""INSERT INTO matching_configuration_group 
                        (algorithm_id, attribute_id, attribute_value) 
                        VALUES (:algo_id, :attr_id, :value)""")
                    conn.execute(insert_query, {'algo_id': algorithm_id, 'attr_id': attr_id, 'value': float(value)})
        
        self.session.commit()
