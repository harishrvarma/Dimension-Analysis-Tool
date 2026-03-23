from models.base.base_repository import BaseRepository
from models.pricing.product_column import ProductColumn


class ProductColumnRepository(BaseRepository):

    def __init__(self, db):
        super().__init__(db, ProductColumn)

    def get_all(self):
        return self.db.query(ProductColumn).order_by(ProductColumn.sort_order, ProductColumn.name).all()
