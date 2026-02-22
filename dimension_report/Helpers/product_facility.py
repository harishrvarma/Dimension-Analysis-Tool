from Models.database import SessionLocal
from Helpers.product_helper import (
    get_product_by_id, 
    update_product, 
    update_product_status,
    get_products_by_group_id
)

class ProductFacility:
    """
    Facility class for performing various actions on products.
    This class provides methods to update product status flags based on analysis results.
    """
    
    def __init__(self):
        self.db = SessionLocal()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.db.close()
    
    def update_dbscan_status(self, product_id: int, is_outlier: bool, 
                            height_outlier: bool = False, 
                            width_outlier: bool = False, 
                            depth_outlier: bool = False):
        """Update DBSCAN analysis status for a product"""
        status = "outlier" if is_outlier else "normal"
        
        update_product_status(self.db, product_id, "dbs_status", status)
        
        if height_outlier:
            update_product_status(self.db, product_id, "dbs_height_status", "outlier")
        if width_outlier:
            update_product_status(self.db, product_id, "dbs_width_status", "outlier")
        if depth_outlier:
            update_product_status(self.db, product_id, "dbs_depth_status", "outlier")
        
        return True
    
    def update_iqr_status(self, product_id: int, is_outlier: bool,
                         height_outlier: bool = False,
                         width_outlier: bool = False,
                         depth_outlier: bool = False):
        """Update IQR analysis status for a product"""
        status = "outlier" if is_outlier else "normal"
        
        update_product_status(self.db, product_id, "iqr_status", status)
        
        if height_outlier:
            update_product_status(self.db, product_id, "iqr_height_status", "outlier")
        if width_outlier:
            update_product_status(self.db, product_id, "iqr_width_status", "outlier")
        if depth_outlier:
            update_product_status(self.db, product_id, "iqr_depth_status", "outlier")
        
        return True
    
    def skip_status_update(self, product_id: int, skip_status: str):
        """Force a specific status on a product (e.g., 'approved', 'rejected', 'review')"""
        update_product_status(self.db, product_id, "skip_status", skip_status)
        return True
    
    def bulk_update_status_by_group(self, group_id: int, status_field: str, status_value: str):
        """Update status for all products in a group"""
        from Models.models import Product
        self.db.query(Product).filter(Product.group_id == group_id).update(
            {status_field: status_value},
            synchronize_session=False
        )
        self.db.commit()
        return self.db.query(Product).filter(Product.group_id == group_id).count()
    
    def reset_all_statuses(self, product_id: int):
        """Reset all status flags for a product"""
        from Models.models import Product
        self.db.query(Product).filter(Product.product_id == product_id).update({
            'dbs_status': None,
            'dbs_height_status': None,
            'dbs_width_status': None,
            'dbs_depth_status': None,
            'iqr_status': None,
            'iqr_height_status': None,
            'iqr_width_status': None,
            'iqr_depth_status': None,
            'skip_status': None
        }, synchronize_session=False)
        self.db.commit()
        return True


# Example usage:
if __name__ == "__main__":
    # Using context manager (recommended)
    with ProductFacility() as facility:
        # Update DBSCAN status
        facility.update_dbscan_status(
            product_id=1, 
            is_outlier=True, 
            height_outlier=True
        )
        
        # Update IQR status
        facility.update_iqr_status(
            product_id=1,
            is_outlier=False
        )
        
        # Force a status
        facility.skip_status_update(product_id=1, skip_status="approved")
        
        # Bulk update for a group
        count = facility.bulk_update_status_by_group(
            group_id=1, 
            status_field="skip_status", 
            status_value="pending_review"
        )
        print(f"Updated {count} products")
        
        # Reset all statuses
        facility.reset_all_statuses(product_id=1)
