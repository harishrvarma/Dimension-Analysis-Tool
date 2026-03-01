from models.dimension.product_iteration_item import DimensionProductIterationItem
from datetime import datetime
import time
import random


class DimensionProductIterationItemRepository:
    def __init__(self, db):
        self.db = db
        self.model = DimensionProductIterationItem

    def save_items(self, items_data):
        """Save multiple iteration items"""
        try:
            for data in items_data:
                item = DimensionProductIterationItem(
                    iteration_id=data['iteration_id'],
                    system_product_id=data['system_product_id'],
                    brand=data.get('brand'),
                    category=data.get('category'),
                    product_type=data.get('product_type'),
                    cluster=data.get('cluster'),
                    outlier_mode=data.get('outlier_mode'),
                    status=data.get('status')
                )
                self.db.add(item)
            self.db.commit()
            return True
        except Exception as e:
            self.db.rollback()
            print(f"Error saving iteration items: {e}")
            return False

    def delete_by_iteration_id(self, iteration_id):
        """Delete all items for a specific iteration"""
        try:
            self.db.query(DimensionProductIterationItem).filter(
                DimensionProductIterationItem.iteration_id == iteration_id
            ).delete(synchronize_session=False)
            self.db.commit()
            return True
        except Exception as e:
            self.db.rollback()
            print(f"Error deleting iteration items: {e}")
            return False

    def get_aggregated_status_by_product(self, system_product_ids):
        """Get aggregated status for products"""
        from sqlalchemy import func
        
        try:
            results = self.db.query(
                DimensionProductIterationItem.system_product_id,
                func.avg(DimensionProductIterationItem.status).label('avg_status'),
                func.avg(DimensionProductIterationItem.outlier_mode).label('avg_outlier_mode'),
                func.count(DimensionProductIterationItem.id).label('count')
            ).filter(
                DimensionProductIterationItem.system_product_id.in_(system_product_ids)
            ).group_by(
                DimensionProductIterationItem.system_product_id
            ).all()
            
            aggregated = {}
            for row in results:
                # Calculate dbs_status: if avg >= 0.5, status=1 (normal), else 0 (outlier)
                dbs_status = 1 if row.avg_status >= 0.5 else 0
                
                # Calculate outlier_mode: if avg >= 0.5, mode=1 (manual), else 0 (auto)
                outlier_mode = 1 if row.avg_outlier_mode and row.avg_outlier_mode >= 0.5 else None
                
                # If outlier_mode is manual, force final_status to outlier
                final_status = 0 if outlier_mode == 1 else dbs_status
                
                aggregated[row.system_product_id] = {
                    'dbs_status': dbs_status,
                    'final_status': final_status,
                    'outlier_mode': outlier_mode
                }
            
            return aggregated
        except Exception as e:
            print(f"Error getting aggregated status: {e}")
            return {}

    @staticmethod
    def generate_unique_number():
        """Generate unique number from timestamp and random numbers"""
        timestamp = str(int(time.time() * 1000))
        random_num = str(random.randint(1000, 9999))
        return f"{timestamp}{random_num}"

    def get_iteration_summary(self, brand, category):
        """Get iteration summary for brand and category"""
        from sqlalchemy import func
        from models.dimension.product_iteration import ProductIteration
        
        try:
            results = self.db.query(
                ProductIteration.iteration_id,
                ProductIteration.eps,
                ProductIteration.sample
            ).join(
                DimensionProductIterationItem,
                ProductIteration.iteration_id == DimensionProductIterationItem.iteration_id
            ).filter(
                ProductIteration.brand == brand,
                ProductIteration.category == category
            ).group_by(
                ProductIteration.iteration_id,
                ProductIteration.eps,
                ProductIteration.sample
            ).order_by(
                ProductIteration.iteration_id.asc()
            ).all()
            
            summary = []
            for row in results:
                iter_id = row.iteration_id
                
                # Get counts for this iteration
                total = self.db.query(func.count(DimensionProductIterationItem.id)).filter(
                    DimensionProductIterationItem.iteration_id == iter_id
                ).scalar() or 0
                
                normal = self.db.query(func.count(DimensionProductIterationItem.id)).filter(
                    DimensionProductIterationItem.iteration_id == iter_id,
                    DimensionProductIterationItem.status == 1
                ).scalar() or 0
                
                outlier = self.db.query(func.count(DimensionProductIterationItem.id)).filter(
                    DimensionProductIterationItem.iteration_id == iter_id,
                    DimensionProductIterationItem.status == 0
                ).scalar() or 0
                
                manual = self.db.query(func.count(DimensionProductIterationItem.id)).filter(
                    DimensionProductIterationItem.iteration_id == iter_id,
                    DimensionProductIterationItem.outlier_mode == 1,
                    DimensionProductIterationItem.status == 0
                ).scalar() or 0
                
                summary.append({
                    'iteration': iter_id,
                    'iteration_id': iter_id,
                    'eps': float(row.eps) if row.eps else None,
                    'sample': int(row.sample) if row.sample else None,
                    'total_count': int(total),
                    'normal_count': int(normal),
                    'outlier_count': int(outlier),
                    'manual_outlier_count': int(manual)
                })
            
            return summary
        except Exception as e:
            print(f"Error getting iteration summary: {e}")
            return []

    def update_items_status(self, iteration_id, system_product_ids, status, outlier_mode):
        """Update status and outlier_mode for specific items"""
        try:
            self.db.query(DimensionProductIterationItem).filter(
                DimensionProductIterationItem.iteration_id == iteration_id,
                DimensionProductIterationItem.system_product_id.in_(system_product_ids)
            ).update({
                'status': status,
                'outlier_mode': outlier_mode
            }, synchronize_session=False)
            self.db.commit()
            return True
        except Exception as e:
            self.db.rollback()
            print(f"Error updating items status: {e}")
            return False
