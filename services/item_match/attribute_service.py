from models.base.base import SessionLocal
from models.matching.matching_attribute import MatchingAttribute
from sqlalchemy import text


class AttributeService:
    """Service to manage dynamic attributes from database"""
    
    def __init__(self, session=None):
        self.session = session or SessionLocal()
        self._should_close = session is None
    
    def __del__(self):
        if self._should_close and hasattr(self, 'session'):
            self.session.close()
    
    def get_all_attributes(self):
        """Fetch all attributes ordered by attribute_id"""
        return self.session.query(MatchingAttribute).order_by(MatchingAttribute.attribute_id).all()
    
    def get_attributes_by_type(self, attr_type):
        """Get attributes filtered by type"""
        return self.session.query(MatchingAttribute).filter(MatchingAttribute.attribute_type == attr_type).order_by(MatchingAttribute.attribute_id).all()
    
    def get_attribute_dict(self):
        """Return attributes as dict: {attribute_name: {id, weightage, type}}"""
        attrs = self.get_all_attributes()
        return {
            attr.attribute_name: {
                'id': attr.attribute_id,
                'weightage': attr.default_weightage,
                'type': attr.attribute_type
            }
            for attr in attrs
        }
    
    def get_attribute_names(self):
        """Return list of attribute names"""
        return [attr.attribute_name for attr in self.get_all_attributes()]
    
    def get_default_weights(self):
        """Return dict of {attribute_name: default_weightage}"""
        attrs = self.get_all_attributes()
        return {attr.attribute_name: attr.default_weightage for attr in attrs}
