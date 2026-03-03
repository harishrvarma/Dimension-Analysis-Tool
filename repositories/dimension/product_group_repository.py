from models.base.base_repository import BaseRepository
from models.dimension.product_group import ProductGroup
import pandas as pd


class ProductGroupRepository(BaseRepository):

    def __init__(self, db):
        super().__init__(db, ProductGroup)

    def get_all_groups(self):
        """Get all product groups ordered by name"""
        query = """
            SELECT group_id, name, product_count, default_selected
            FROM dimension_product_group 
            ORDER BY name
        """
        result = self.fetch_all(query)
        
        if result:
            df = pd.DataFrame(result, columns=['group_id', 'name', 'product_count', 'default_selected'])
            return df
        return pd.DataFrame()

    def get_by_name(self, name: str):
        return (
            self.db.query(ProductGroup)
            .filter(ProductGroup.name == name)
            .first()
        )