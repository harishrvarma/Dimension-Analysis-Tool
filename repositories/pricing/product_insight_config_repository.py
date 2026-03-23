from models.base.base_repository import BaseRepository
from models.pricing.product_insight_config import ProductInsightConfig


class ProductInsightConfigRepository(BaseRepository):

    def __init__(self, db):
        super().__init__(db, ProductInsightConfig)

    def get_all(self):
        return self.db.query(ProductInsightConfig).order_by(ProductInsightConfig.sort_order, ProductInsightConfig.name).all()

    def get_default(self):
        return (
            self.db.query(ProductInsightConfig)
            .filter(ProductInsightConfig.is_default == 1)
            .first()
        )
