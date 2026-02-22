from models.base.base_repository import BaseRepository
from models.product import Product
import pandas as pd


class ProductRepository(BaseRepository):

    def __init__(self, db):
        super().__init__(db, Product)

    def get_by_qb_code(self, qb_code: str):
        return (
            self.db.query(Product)
            .filter(Product.qb_code == qb_code)
            .first()
        )

    def get_brands_for_group(self, group_id: int):
        """Get brands and their product counts for a specific product group"""
        query = """
            SELECT brand, COUNT(*) as product_count
            FROM product
            WHERE group_id = :group_id 
            AND brand IS NOT NULL
            AND height IS NOT NULL 
            AND width IS NOT NULL 
            AND depth IS NOT NULL
            GROUP BY brand
            ORDER BY brand
        """
        result = self.fetch_all(query, {"group_id": group_id})
        
        if result:
            df = pd.DataFrame(result, columns=['brand', 'product_count'])
            return df
        return pd.DataFrame()

    def get_categories_for_group(self, group_id: int, brands: list = None):
        """Get categories for a group, optionally filtered by brands"""
        if brands and len(brands) > 0:
            placeholders = ','.join([f':brand{i}' for i in range(len(brands))])
            query = f"""
                SELECT category, COUNT(*) as product_count
                FROM product
                WHERE group_id = :group_id 
                AND brand IN ({placeholders})
                AND category IS NOT NULL
                AND height IS NOT NULL 
                AND width IS NOT NULL 
                AND depth IS NOT NULL
                GROUP BY category
                ORDER BY product_count DESC, category
            """
            params = {'group_id': group_id}
            for i, brand in enumerate(brands):
                params[f'brand{i}'] = brand
        else:
            query = """
                SELECT category, COUNT(*) as product_count
                FROM product
                WHERE group_id = :group_id 
                AND category IS NOT NULL
                AND height IS NOT NULL 
                AND width IS NOT NULL 
                AND depth IS NOT NULL
                GROUP BY category
                ORDER BY product_count DESC, category
            """
            params = {'group_id': group_id}
        
        result = self.fetch_all(query, params)
        
        if result:
            df = pd.DataFrame(result, columns=['category', 'product_count'])
            return df
        return pd.DataFrame()

    def get_types_for_group(self, group_id: int, brands: list = None, category: str = None):
        """Get product types for a group, optionally filtered by brands and category"""
        conditions = ["group_id = :group_id", "product_type IS NOT NULL", 
                      "height IS NOT NULL", "width IS NOT NULL", "depth IS NOT NULL"]
        params = {'group_id': group_id}
        
        if brands and len(brands) > 0:
            placeholders = ','.join([f':brand{i}' for i in range(len(brands))])
            conditions.append(f"brand IN ({placeholders})")
            for i, brand in enumerate(brands):
                params[f'brand{i}'] = brand
        
        if category:
            conditions.append("category = :category")
            params['category'] = category
        
        where_clause = " AND ".join(conditions)
        query = f"""
            SELECT product_type, COUNT(*) as product_count
            FROM product
            WHERE {where_clause}
            GROUP BY product_type
            ORDER BY product_count DESC, product_type
        """
        
        result = self.fetch_all(query, params)
        
        if result:
            df = pd.DataFrame(result, columns=['product_type', 'product_count'])
            return df
        return pd.DataFrame()

    def load_products_filtered(self, group_id: int, brands: list = None, 
                               category: str = None, types: list = None):
        """Load product data from database with filters"""
        conditions = ["group_id = :group_id", 
                      "height IS NOT NULL", "width IS NOT NULL", "depth IS NOT NULL"]
        params = {'group_id': group_id}
        
        if brands and len(brands) > 0:
            placeholders = ','.join([f':brand{i}' for i in range(len(brands))])
            conditions.append(f"brand IN ({placeholders})")
            for i, brand in enumerate(brands):
                params[f'brand{i}'] = brand
        
        if category:
            conditions.append("category = :category")
            params['category'] = category
        
        if types and len(types) > 0:
            placeholders = ','.join([f':type{i}' for i in range(len(types))])
            conditions.append(f"product_type IN ({placeholders})")
            for i, ptype in enumerate(types):
                params[f'type{i}'] = ptype
        
        where_clause = " AND ".join(conditions)
        query = f"""
            SELECT 
                product_id,
                qb_code,
                brand,
                category,
                product_type,
                name,
                height,
                width,
                depth,
                weight,
                base_image_url,
                product_url
            FROM product
            WHERE {where_clause}
            ORDER BY product_id
        """
        
        result = self.fetch_all(query, params)
        
        if result:
            df = pd.DataFrame(result, columns=[
                'product_id', 'qb_code', 'brand', 'category', 'product_type', 
                'name', 'height', 'width', 'depth', 'weight', 'base_image_url', 'product_url'
            ])
            return df
        return pd.DataFrame()

    def get_brands_for_chart(self, group_id: int):
        query = """
            SELECT brand, COUNT(*) as count
            FROM product
            WHERE group_id = :group_id AND brand IS NOT NULL
            AND height IS NOT NULL AND width IS NOT NULL AND depth IS NOT NULL
            GROUP BY brand
            ORDER BY brand
        """
        result = self.fetch_all(query, {"group_id": group_id})
        return [{"label": f"{r[0]} ({r[1]})", "value": r[0]} for r in result] if result else []

    def get_categories_for_chart(self, group_id: int, brands):
        conditions = ["group_id = :group_id", "category IS NOT NULL", "height IS NOT NULL", "width IS NOT NULL", "depth IS NOT NULL"]
        params = {"group_id": group_id}
        
        if brands:
            placeholders = ",".join([f":brand{i}" for i in range(len(brands))])
            conditions.append(f"brand IN ({placeholders})")
            for i, brand in enumerate(brands):
                params[f"brand{i}"] = brand
        
        where_clause = " AND ".join(conditions)
        query = f"""
            SELECT category, COUNT(*) as count
            FROM product
            WHERE {where_clause}
            GROUP BY category
            ORDER BY category
        """
        result = self.fetch_all(query, params)
        return [{"label": f"{r[0]} ({r[1]})", "value": r[0]} for r in result] if result else []

    def get_types_for_chart(self, group_id: int, brands, category):
        conditions = ["group_id = :group_id", "product_type IS NOT NULL", "height IS NOT NULL", "width IS NOT NULL", "depth IS NOT NULL"]
        params = {"group_id": group_id}
        
        if brands:
            placeholders = ",".join([f":brand{i}" for i in range(len(brands))])
            conditions.append(f"brand IN ({placeholders})")
            for i, brand in enumerate(brands):
                params[f"brand{i}"] = brand
        
        if category:
            conditions.append("category = :category")
            params["category"] = category
        
        where_clause = " AND ".join(conditions)
        query = f"""
            SELECT product_type, COUNT(*) as count
            FROM product
            WHERE {where_clause}
            GROUP BY product_type
            ORDER BY product_type
        """
        result = self.fetch_all(query, params)
        return [{"label": f"{r[0]} ({r[1]})", "value": r[0]} for r in result] if result else []

    def update_skip_status(self, product_id: int, skip_status: int):
        """Update skip status for a product"""
        from datetime import datetime
        product = self.db.query(Product).filter(Product.product_id == product_id).first()
        if product:
            product.skip_status = skip_status
            product.skip_status_updated_date = datetime.now()

    def load_products_for_iteration(self, group_id: int, iteration: int, brands: list = None, 
                                   category: str = None, types: list = None, for_display=False):
        """Load products for specific iteration"""
        conditions = ["group_id = :group_id", 
                      "height IS NOT NULL", "width IS NOT NULL", "depth IS NOT NULL"]
        params = {'group_id': group_id}
        
        if iteration > 1:
            # Load products where: iteration = current OR (iteration = previous AND final_status = 1)
            conditions.append("(iteration_closed = :current_iteration OR (iteration_closed = :prev_iteration AND final_status = 1))")
            params['current_iteration'] = iteration
            params['prev_iteration'] = iteration - 1
        
        if brands and len(brands) > 0:
            placeholders = ','.join([f':brand{i}' for i in range(len(brands))])
            conditions.append(f"brand IN ({placeholders})")
            for i, brand in enumerate(brands):
                params[f'brand{i}'] = brand
        
        if category:
            conditions.append("category = :category")
            params['category'] = category
        
        if types and len(types) > 0:
            placeholders = ','.join([f':type{i}' for i in range(len(types))])
            conditions.append(f"product_type IN ({placeholders})")
            for i, ptype in enumerate(types):
                params[f'type{i}'] = ptype
        
        where_clause = " AND ".join(conditions)
        query = f"""
            SELECT 
                product_id,
                qb_code,
                brand,
                category,
                product_type,
                name,
                height,
                width,
                depth,
                weight,
                base_image_url,
                product_url
            FROM product
            WHERE {where_clause}
            ORDER BY product_id
        """
        
        result = self.fetch_all(query, params)
        
        if result:
            df = pd.DataFrame(result, columns=[
                'product_id', 'qb_code', 'brand', 'category', 'product_type', 
                'name', 'height', 'width', 'depth', 'weight', 'base_image_url', 'product_url'
            ])
            return df
        return pd.DataFrame()

    def update_iteration_results(self, product_updates: list):
        """Bulk update products with iteration results"""
        from datetime import datetime
        for update in product_updates:
            product = self.db.query(Product).filter(
                Product.product_id == update['product_id']
            ).first()
            if product:
                # Update iteration
                if update.get('iteration_closed') is not None:
                    product.iteration_closed = update.get('iteration_closed')
                
                # Update IQR fields (set to None if not in update or explicitly None)
                product.iqr_status = update.get('iqr_status')
                product.iqr_height_status = update.get('iqr_height_status')
                product.iqr_width_status = update.get('iqr_width_status')
                product.iqr_depth_status = update.get('iqr_depth_status')
                
                # Update DBSCAN field (set to None if not in update or explicitly None)
                product.dbs_status = update.get('dbs_status')
                
                # Update final status
                product.final_status = update.get('final_status')
                
                product.analyzed_date = datetime.now()
        self.db.commit()

    def get_iteration_history(self, group_id: int, brands: list, category: str, types: list):
        """Get iteration history for a category"""
        conditions = ["group_id = :group_id", "category = :category"]
        params = {'group_id': group_id, 'category': category}
        
        if brands and len(brands) > 0:
            placeholders = ','.join([f':brand{i}' for i in range(len(brands))])
            conditions.append(f"brand IN ({placeholders})")
            for i, brand in enumerate(brands):
                params[f'brand{i}'] = brand
        
        if types and len(types) > 0:
            placeholders = ','.join([f':type{i}' for i in range(len(types))])
            conditions.append(f"product_type IN ({placeholders})")
            for i, ptype in enumerate(types):
                params[f'type{i}'] = ptype
        
        where_clause = " AND ".join(conditions)
        
        # Get max iteration
        max_iter_query = f"""
            SELECT MAX(iteration_closed) as max_iter
            FROM product
            WHERE {where_clause} AND iteration_closed IS NOT NULL
        """
        max_result = self.fetch_all(max_iter_query, params)
        if not max_result or not max_result[0][0]:
            return []
        
        max_iteration = max_result[0][0]
        history = []
        
        # For each iteration, calculate totals based on the logic:
        # Total = products with iteration_closed >= current_iteration
        # Outlier = products with iteration_closed = current_iteration AND final_status = 0
        # Normal = Total - Outlier
        for iteration in range(1, max_iteration + 1):
            # Get total: all products with iteration >= current iteration
            total_query = f"""
                SELECT COUNT(*) as total
                FROM product
                WHERE {where_clause}
                AND iteration_closed >= :iteration
            """
            params['iteration'] = iteration
            total_result = self.fetch_all(total_query, params)
            total = total_result[0][0] if total_result and total_result[0][0] else 0
            
            # Get outlier: products with iteration = current AND final_status = 0
            outlier_query = f"""
                SELECT COUNT(*) as outlier
                FROM product
                WHERE {where_clause}
                AND iteration_closed = :iteration
                AND final_status = 0
            """
            outlier_result = self.fetch_all(outlier_query, params)
            outlier = outlier_result[0][0] if outlier_result and outlier_result[0][0] else 0
            
            # Normal = Total - Outlier
            normal = total - outlier
            
            history.append({
                'iteration': iteration,
                'total': total,
                'normal': normal,
                'outlier': outlier
            })
        
        return history

    def reset_iterations(self, group_id: int, brands: list, category: str, types: list):
        """Reset iterations for a category"""
        from sqlalchemy import text
        conditions = ["group_id = :group_id", "category = :category"]
        params = {'group_id': group_id, 'category': category}
        
        if brands and len(brands) > 0:
            placeholders = ','.join([f':brand{i}' for i in range(len(brands))])
            conditions.append(f"brand IN ({placeholders})")
            for i, brand in enumerate(brands):
                params[f'brand{i}'] = brand
        
        if types and len(types) > 0:
            placeholders = ','.join([f':type{i}' for i in range(len(types))])
            conditions.append(f"product_type IN ({placeholders})")
            for i, ptype in enumerate(types):
                params[f'type{i}'] = ptype
        
        where_clause = " AND ".join(conditions)
        query = f"""
            UPDATE product
            SET iteration_closed = NULL,
                iqr_status = NULL,
                iqr_height_status = NULL,
                iqr_width_status = NULL,
                iqr_depth_status = NULL,
                dbs_status = NULL,
                final_status = NULL,
                analyzed_date = NULL
            WHERE {where_clause}
        """
        
        self.db.execute(text(query), params)
        self.db.commit()

    def get_previous_outliers(self, group_id: int, brands: list, category: str, types: list, current_iteration: int):
        """Get all outliers from previous iterations with analysis data"""
        conditions = ["group_id = :group_id", "category = :category"]
        params = {'group_id': group_id, 'category': category}
        
        # Get outliers from iterations before current
        if current_iteration > 1:
            conditions.append("iteration_closed < :current_iteration")
            params['current_iteration'] = current_iteration
        else:
            # No previous iterations
            return pd.DataFrame()
        
        conditions.append("final_status = 0")
        
        if brands and len(brands) > 0:
            placeholders = ','.join([f':brand{i}' for i in range(len(brands))])
            conditions.append(f"brand IN ({placeholders})")
            for i, brand in enumerate(brands):
                params[f'brand{i}'] = brand
        
        if types and len(types) > 0:
            placeholders = ','.join([f':type{i}' for i in range(len(types))])
            conditions.append(f"product_type IN ({placeholders})")
            for i, ptype in enumerate(types):
                params[f'type{i}'] = ptype
        
        where_clause = " AND ".join(conditions)
        query = f"""
            SELECT 
                product_id,
                qb_code,
                brand,
                category,
                product_type,
                name,
                height,
                width,
                depth,
                weight,
                base_image_url,
                product_url,
                iqr_status,
                iqr_height_status,
                iqr_width_status,
                iqr_depth_status,
                dbs_status
            FROM product
            WHERE {where_clause}
            ORDER BY product_id
        """
        
        result = self.fetch_all(query, params)
        
        if result:
            df = pd.DataFrame(result, columns=[
                'product_id', 'qb_code', 'brand', 'category', 'product_type', 
                'name', 'height', 'width', 'depth', 'weight', 'base_image_url', 'product_url',
                'iqr_status', 'iqr_height_status', 'iqr_width_status', 'iqr_depth_status', 'dbs_status'
            ])
            return df
        return pd.DataFrame()
