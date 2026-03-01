from models.base.base import SessionLocal
from models.matching.matching_configuration_group import MatchingConfigurationGroup
from sqlalchemy import text


class ConfigurationService:
    """Service to manage configuration groups"""
    
    def __init__(self, session=None):
        self.session = session or SessionLocal()
        self._should_close = session is None
    
    def __del__(self):
        if self._should_close and hasattr(self, 'session'):
            self.session.close()
    
    def get_or_create_config_group(self, weights, thresholds, price_config):
        """
        Get existing or create new configuration group
        Returns group_id
        """
        import time
        import random
        from sqlalchemy.exc import IntegrityError
        
        # Build attribute mapping
        all_configs = {}
        for attr_name, weight_val in weights.items():
            attr_query = text("SELECT attribute_id FROM matching_attribute WHERE attribute_name = :name")
            result = self.session.execute(attr_query, {'name': attr_name})
            row = result.fetchone()
            if row:
                all_configs[row[0]] = str(weight_val)
        
        for attr_name, threshold_val in thresholds.items():
            attr_query = text("SELECT attribute_id FROM matching_attribute WHERE attribute_name = :name")
            result = self.session.execute(attr_query, {'name': attr_name})
            row = result.fetchone()
            if row:
                all_configs[row[0]] = str(threshold_val)
        
        for attr_name, price_val in price_config.items():
            attr_query = text("SELECT attribute_id FROM matching_attribute WHERE attribute_name = :name")
            result = self.session.execute(attr_query, {'name': attr_name})
            row = result.fetchone()
            if row:
                all_configs[row[0]] = str(price_val)
        
        # Check if exact configuration already exists
        if all_configs:
            # Get all distinct group_ids
            groups_query = text("SELECT DISTINCT group_id FROM matching_configuration_group")
            result = self.session.execute(groups_query)
            existing_groups = [r[0] for r in result.fetchall()]
            
            # Check each group
            for group_id in existing_groups:
                # Get all configs for this group
                group_query = text(
                    "SELECT matching_attribute_id, attribute_value FROM matching_configuration_group WHERE group_id = :gid"
                )
                result = self.session.execute(group_query, {'gid': group_id})
                group_configs = {r[0]: r[1] for r in result.fetchall()}
                
                # Compare
                if group_configs == all_configs:
                    return group_id
        
        # Create new group_id with timestamp + random 6 digits
        for _ in range(5):  # Try up to 5 times
            new_group_id = f"{int(time.time())}{random.randint(100000, 999999)}"
            
            try:
                for attr_id, attr_val in all_configs.items():
                    new_config = MatchingConfigurationGroup(
                        matching_attribute_id=attr_id,
                        attribute_value=attr_val,
                        group_id=new_group_id
                    )
                    self.session.add(new_config)
                
                self.session.commit()
                return new_group_id
            except IntegrityError:
                self.session.rollback()
                time.sleep(0.01)  # Small delay before retry
                continue
        
        # If all retries fail, raise error
        raise Exception("Failed to create unique configuration group after 5 attempts")
