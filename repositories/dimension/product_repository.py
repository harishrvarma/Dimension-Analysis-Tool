from models.base.base_repository import BaseRepository
from models.dimension.product import Product
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
        """Get brands and their product counts with analysis status for a specific product group"""
        query = """
            SELECT 
                brand, 
                COUNT(*) as product_count,
                SUM(CASE WHEN final_status IN (0, 1) THEN 1 ELSE 0 END) as analyzed_count
            FROM dimension_product
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
            df = pd.DataFrame(result, columns=['brand', 'product_count', 'analyzed_count'])
            return df
        return pd.DataFrame()

    def get_categories_for_group(self, group_id: int, brands: list = None):
        """Get categories for a group with analysis status, optionally filtered by brands"""
        if brands and len(brands) > 0:
            placeholders = ','.join([f':brand{i}' for i in range(len(brands))])
            query = f"""
                SELECT 
                    category, 
                    COUNT(*) as product_count,
                    SUM(CASE WHEN final_status IN (0, 1) THEN 1 ELSE 0 END) as analyzed_count
                FROM dimension_product
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
                SELECT 
                    category, 
                    COUNT(*) as product_count,
                    SUM(CASE WHEN final_status IN (0, 1) THEN 1 ELSE 0 END) as analyzed_count
                FROM dimension_product
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
            df = pd.DataFrame(result, columns=['category', 'product_count', 'analyzed_count'])
            return df
        return pd.DataFrame()

    def get_types_for_group(self, group_id: int, brands: list = None, category: str = None):
        """Get product types for a group with analysis status, optionally filtered by brands and category"""
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
            SELECT 
                product_type, 
                COUNT(*) as product_count,
                SUM(CASE WHEN final_status IN (0, 1) THEN 1 ELSE 0 END) as analyzed_count
            FROM dimension_product
            WHERE {where_clause}
            GROUP BY product_type
            ORDER BY product_count DESC, product_type
        """
        
        result = self.fetch_all(query, params)
        
        if result:
            df = pd.DataFrame(result, columns=['product_type', 'product_count', 'analyzed_count'])
            return df
        return pd.DataFrame()

    def load_products_filtered(self, group_id: int, brands: list = None, 
                               category: str = None, types: list = None):
        """Load product data from database with filters"""
        conditions = ["group_id = :group_id", 
                      "height IS NOT NULL", "width IS NOT NULL", "depth IS NOT NULL",
                      "(skip_status IS NULL OR skip_status != 1)"]
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
                product_url,
                outlier_mode,
                system_product_id
            FROM dimension_product
            WHERE {where_clause}
            ORDER BY product_id
        """
        
        result = self.fetch_all(query, params)
        
        if result:
            df = pd.DataFrame(result, columns=[
                'product_id', 'qb_code', 'brand', 'category', 'product_type', 
                'name', 'height', 'width', 'depth', 'weight', 'base_image_url', 'product_url', 'outlier_mode', 'system_product_id'
            ])
            return df
        return pd.DataFrame()

    def get_brands_for_chart(self, group_id: int):
        query = """
            SELECT brand, COUNT(*) as count
            FROM dimension_product
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
            FROM dimension_product
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
            FROM dimension_product
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
                      "height IS NOT NULL", "width IS NOT NULL", "depth IS NOT NULL",
                      "(skip_status IS NULL OR skip_status != 1)"]
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
                product_url,
                outlier_mode,
                system_product_id
            FROM dimension_product
            WHERE {where_clause}
            ORDER BY product_id
        """
        
        result = self.fetch_all(query, params)
        
        if result:
            df = pd.DataFrame(result, columns=[
                'product_id', 'qb_code', 'brand', 'category', 'product_type', 
                'name', 'height', 'width', 'depth', 'weight', 'base_image_url', 'product_url', 'outlier_mode', 'system_product_id'
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
                
                # Set outlier_mode: 0 for automatic outliers, preserve 1 for manual, None for normal
                if update.get('final_status') == 0 and product.outlier_mode != 1:
                    product.outlier_mode = 0
                elif update.get('final_status') == 1:
                    product.outlier_mode = None
                
                product.analyzed_date = datetime.now()
        self.db.commit()

    def get_iteration_history(self, group_id: int, brands: list, category: str, types: list):
        """Get iteration history for a category from product_iteration table"""
        from sqlalchemy import func, and_, case
        from models.dimension.product_iteration import ProductIteration
        
        # Build brand filter
        brand_str = ', '.join(brands) if brands and len(brands) > 0 else None
        
        # Query product_iteration table
        query = self.db.query(
            ProductIteration.iteration_number,
            func.count(func.distinct(ProductIteration.system_product_id)).label('total'),
            func.sum(case((ProductIteration.status == 1, 1), else_=0)).label('normal_count'),
            func.sum(case((and_(ProductIteration.status == 0, ProductIteration.outlier_mode == 0), 1), else_=0)).label('outlier_count'),
            func.sum(case((and_(ProductIteration.status == 0, ProductIteration.outlier_mode == 1), 1), else_=0)).label('manual_outlier_count')
        ).filter(
            ProductIteration.category == category
        )
        
        if brand_str:
            query = query.filter(ProductIteration.brand == brand_str)
        
        query = query.group_by(ProductIteration.iteration_number).order_by(ProductIteration.iteration_number)
        
        results = query.all()
        
        if not results:
            return []
        
        history = []
        for r in results:
            # Calculate normal as total - outlier - manual_outlier
            outlier = r.outlier_count or 0
            manual_outlier = r.manual_outlier_count or 0
            total = r.total or 0
            normal = total - outlier - manual_outlier
            
            history.append({
                'iteration': r.iteration_number,
                'total': total,
                'normal': normal,
                'outlier': outlier,
                'manual_outlier': manual_outlier
            })
        
        return history

    def reset_iterations(self, group_id: int, brands: list, category: str, types: list):
        """Reset iterations for a category in both product and product_iteration tables"""
        from sqlalchemy import text
        from repositories.dimension.product_iteration_repository import ProductIterationRepository
        
        # Delete from product_iteration table
        iteration_repo = ProductIterationRepository(self.db)
        iteration_repo.delete_iterations_by_brand_category(brands, category)
        
        # Reset product table
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
            UPDATE dimension_product
            SET iteration_closed = NULL,
                iqr_status = NULL,
                iqr_height_status = NULL,
                iqr_width_status = NULL,
                iqr_depth_status = NULL,
                dbs_status = NULL,
                final_status = NULL,
                outlier_mode = NULL,
                analyzed_date = NULL
            WHERE {where_clause}
        """
        
        self.db.execute(text(query), params)
        self.db.commit()

    def get_previous_outliers(self, group_id: int, brands: list, category: str, types: list, current_iteration: int):
        """Get all outliers from previous iterations with analysis data"""
        conditions = ["group_id = :group_id", "category = :category",
                      "(skip_status IS NULL OR skip_status != 1)"]
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
                dbs_status,
                outlier_mode,
                iteration_closed
            FROM dimension_product
            WHERE {where_clause}
            ORDER BY product_id
        """
        
        result = self.fetch_all(query, params)
        
        if result:
            df = pd.DataFrame(result, columns=[
                'product_id', 'qb_code', 'brand', 'category', 'product_type', 
                'name', 'height', 'width', 'depth', 'weight', 'base_image_url', 'product_url',
                'iqr_status', 'iqr_height_status', 'iqr_width_status', 'iqr_depth_status', 'dbs_status', 'outlier_mode', 'iteration_closed'
            ])
            return df
        return pd.DataFrame()

    def update_products_final_status(self, skus: list, final_status: int, iteration: int = None):
        """Update final_status, outlier_mode, and iteration_closed for multiple products by SKU"""
        from sqlalchemy import text
        from datetime import datetime
        
        if not skus:
            return
        
        placeholders = ','.join([f':sku{i}' for i in range(len(skus))])
        params = {'final_status': final_status, 'outlier_mode': 1 if final_status == 0 else None}
        for i, sku in enumerate(skus):
            params[f'sku{i}'] = sku
        
        if iteration is not None:
            params['iteration_closed'] = iteration
            query = f"""
                UPDATE dimension_product
                SET final_status = :final_status,
                    outlier_mode = :outlier_mode,
                    iteration_closed = :iteration_closed,
                    analyzed_date = NOW()
                WHERE qb_code IN ({placeholders})
            """
        else:
            query = f"""
                UPDATE dimension_product
                SET final_status = :final_status,
                    outlier_mode = :outlier_mode,
                    analyzed_date = NOW()
                WHERE qb_code IN ({placeholders})
            """
        
        self.db.execute(text(query), params)
        self.db.commit()

    def update_products_aggregated(self, product_updates: list):
        """Update products with aggregated results from all iterations including outlier_mode"""
        from sqlalchemy import text
        
        if not product_updates:
            return
        
        for update in product_updates:
            system_product_id = update.get('system_product_id')
            if not system_product_id:
                continue
            
            set_clauses = []
            params = {'system_product_id': system_product_id}
            
            if 'iqr_status' in update:
                set_clauses.append('iqr_status = :iqr_status')
                params['iqr_status'] = update['iqr_status']
            
            if 'dbs_status' in update:
                set_clauses.append('dbs_status = :dbs_status')
                params['dbs_status'] = update['dbs_status']
            
            if 'final_status' in update:
                set_clauses.append('final_status = :final_status')
                params['final_status'] = update['final_status']
            
            if 'outlier_mode' in update:
                set_clauses.append('outlier_mode = :outlier_mode')
                params['outlier_mode'] = update['outlier_mode']
            
            if set_clauses:
                set_clauses.append('analyzed_date = NOW()')
                query = f"""
                    UPDATE dimension_product
                    SET {', '.join(set_clauses)}
                    WHERE system_product_id = :system_product_id
                """
                self.db.execute(text(query), params)
        
        self.db.commit()

    def get_global_aggregate_data(self, group_id: int, brands: list, category: str, types: list, algorithms: list):
        """Get global aggregate data from product table for all saved iterations"""
        conditions = ["group_id = :group_id", "category = :category", 
                      "height IS NOT NULL", "width IS NOT NULL", "depth IS NOT NULL",
                      "(skip_status IS NULL OR skip_status != 1)"]
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
                dbs_status,
                final_status,
                outlier_mode,
                system_product_id
            FROM dimension_product
            WHERE {where_clause}
            ORDER BY product_id
        """
        
        result = self.fetch_all(query, params)
        
        if result:
            df = pd.DataFrame(result, columns=[
                'product_id', 'qb_code', 'brand', 'category', 'product_type', 
                'name', 'height', 'width', 'depth', 'weight', 'base_image_url', 'product_url',
                'iqr_status', 'dbs_status', 'final_status', 'outlier_mode', 'system_product_id'
            ])
            return df
        return pd.DataFrame()

    def get_basic_groups(self, group_id: int, brands: list = None, category: str = None, types: list = None):
        """Get all basic groups (Brand + Category + Product_Type combinations) with counts"""
        conditions = ["group_id = :group_id", 
                      "height IS NOT NULL", "width IS NOT NULL", "depth IS NOT NULL",
                      "(skip_status IS NULL OR skip_status != 1)"]
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
                brand,
                category,
                product_type,
                COUNT(*) as total_count
            FROM dimension_product
            WHERE {where_clause}
            GROUP BY brand, category, product_type
            ORDER BY brand, category, product_type
        """
        
        result = self.fetch_all(query, params)
        
        if result:
            return [{
                'brand': r[0],
                'category': r[1],
                'product_type': r[2],
                'total_count': r[3]
            } for r in result]
        return []

    def reset_analysis_fields(self, group_id: int, brands: list = None, category: str = None, types: list = None):
        """Reset all analysis-related fields in product table"""
        from sqlalchemy import text
        
        conditions = ["group_id = :group_id"]
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
            UPDATE dimension_product
            SET iqr_status = NULL,
                iqr_height_status = NULL,
                iqr_width_status = NULL,
                iqr_depth_status = NULL,
                dbs_status = NULL,
                final_status = NULL,
                outlier_mode = NULL,
                iteration_closed = NULL,
                analyzed_date = NULL
            WHERE {where_clause}
        """
        
        self.db.execute(text(query), params)
        self.db.commit()


    def load_products_by_ids(self, system_product_ids):
        """Load products by system_product_ids"""
        from models.dimension.product import Product
        
        try:
            products = self.db.query(Product).filter(
                Product.system_product_id.in_(system_product_ids)
            ).all()
            
            data = []
            for p in products:
                data.append({
                    'system_product_id': p.system_product_id,
                    'qb_code': p.qb_code,
                    'brand': p.brand,
                    'category': p.category,
                    'product_type': p.product_type,
                    'name': p.name,
                    'height': p.height,
                    'width': p.width,
                    'depth': p.depth,
                    'base_image_url': p.base_image_url,
                    'product_url': p.product_url
                })
            
            return pd.DataFrame(data)
        except Exception as e:
            print(f"Error loading products by IDs: {e}")
            return pd.DataFrame()

    def update_products_iqr_fields(self, iqr_updates: list):
        """Update products with IQR status fields only"""
        from sqlalchemy import text
        
        if not iqr_updates:
            return
        
        for update in iqr_updates:
            system_product_id = update.get('system_product_id')
            if not system_product_id:
                continue
            
            params = {'system_product_id': system_product_id}
            set_clauses = []
            
            for field in ['iqr_status', 'iqr_height_status', 'iqr_width_status', 'iqr_depth_status']:
                if field in update:
                    set_clauses.append(f'{field} = :{field}')
                    params[field] = update[field]
            
            if set_clauses:
                query = f"""
                    UPDATE dimension_product
                    SET {', '.join(set_clauses)}
                    WHERE system_product_id = :system_product_id
                """
                self.db.execute(text(query), params)
        
        self.db.commit()
