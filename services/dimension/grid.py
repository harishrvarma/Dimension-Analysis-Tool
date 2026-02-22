from models.base.base import SessionLocal
from repositories.product_repository import ProductRepository
from repositories.product_group_repository import ProductGroupRepository


def get_product_groups():
    """Get all product groups for dropdown"""
    db = SessionLocal()
    try:
        repo = ProductGroupRepository(db)
        df = repo.get_all_groups()
        if df.empty:
            return []
        return [
            {"label": f"{row['name']} ({row['product_count']})", "value": int(row['group_id'])}
            for _, row in df.iterrows()
        ]
    finally:
        db.close()


def get_brands_with_counts(group_id, final_status=None):
    """Get brands with product counts for a group"""
    db = SessionLocal()
    try:
        repo = ProductRepository(db)
        
        conditions = ["group_id = :group_id", "brand IS NOT NULL",
                      "height IS NOT NULL", "width IS NOT NULL", "depth IS NOT NULL"]
        params = {'group_id': group_id}
        
        if final_status and len(final_status) > 0:
            has_pending = 'Pending to Analyze' in final_status
            status_values = [1 if s == 'Normal' else 0 for s in final_status if s != 'Pending to Analyze']
            
            if has_pending and status_values:
                placeholders = ','.join([f':status{i}' for i in range(len(status_values))])
                conditions.append(f"(final_status IN ({placeholders}) OR final_status IS NULL)")
                for i, status in enumerate(status_values):
                    params[f'status{i}'] = status
            elif has_pending:
                conditions.append("final_status IS NULL")
            elif status_values:
                placeholders = ','.join([f':status{i}' for i in range(len(status_values))])
                conditions.append(f"final_status IN ({placeholders})")
                for i, status in enumerate(status_values):
                    params[f'status{i}'] = status
        
        where_clause = " AND ".join(conditions)
        query = f"""
            SELECT brand, COUNT(*) as product_count
            FROM product
            WHERE {where_clause}
            GROUP BY brand
            ORDER BY brand
        """
        result = repo.fetch_all(query, params)
        
        if result:
            return [
                {"label": f"{row[0]} ({int(row[1])})", "value": row[0]}
                for row in result
            ]
        return []
    finally:
        db.close()


def get_categories_with_counts(group_id, brands=None, final_status=None):
    """Get categories with product counts"""
    db = SessionLocal()
    try:
        repo = ProductRepository(db)
        
        conditions = ["group_id = :group_id", "category IS NOT NULL",
                      "height IS NOT NULL", "width IS NOT NULL", "depth IS NOT NULL"]
        params = {'group_id': group_id}
        
        if brands and len(brands) > 0:
            placeholders = ','.join([f':brand{i}' for i in range(len(brands))])
            conditions.append(f"brand IN ({placeholders})")
            for i, brand in enumerate(brands):
                params[f'brand{i}'] = brand
        
        if final_status and len(final_status) > 0:
            has_pending = 'Pending to Analyze' in final_status
            status_values = [1 if s == 'Normal' else 0 for s in final_status if s != 'Pending to Analyze']
            
            if has_pending and status_values:
                placeholders = ','.join([f':status{i}' for i in range(len(status_values))])
                conditions.append(f"(final_status IN ({placeholders}) OR final_status IS NULL)")
                for i, status in enumerate(status_values):
                    params[f'status{i}'] = status
            elif has_pending:
                conditions.append("final_status IS NULL")
            elif status_values:
                placeholders = ','.join([f':status{i}' for i in range(len(status_values))])
                conditions.append(f"final_status IN ({placeholders})")
                for i, status in enumerate(status_values):
                    params[f'status{i}'] = status
        
        where_clause = " AND ".join(conditions)
        query = f"""
            SELECT category, COUNT(*) as product_count
            FROM product
            WHERE {where_clause}
            GROUP BY category
            ORDER BY product_count DESC, category
        """
        
        result = repo.fetch_all(query, params)
        
        if result:
            return [
                {"label": f"{row[0]} ({int(row[1])})", "value": row[0]}
                for row in result
            ]
        return []
    finally:
        db.close()


def get_types_with_counts(group_id, brands=None, categories=None, final_status=None):
    """Get product types with product counts"""
    db = SessionLocal()
    try:
        repo = ProductRepository(db)
        
        conditions = ["group_id = :group_id", "product_type IS NOT NULL",
                      "height IS NOT NULL", "width IS NOT NULL", "depth IS NOT NULL"]
        params = {'group_id': group_id}
        
        if brands and len(brands) > 0:
            placeholders = ','.join([f':brand{i}' for i in range(len(brands))])
            conditions.append(f"brand IN ({placeholders})")
            for i, brand in enumerate(brands):
                params[f'brand{i}'] = brand
        
        if categories and len(categories) > 0:
            placeholders = ','.join([f':cat{i}' for i in range(len(categories))])
            conditions.append(f"category IN ({placeholders})")
            for i, cat in enumerate(categories):
                params[f'cat{i}'] = cat
        
        if final_status and len(final_status) > 0:
            has_pending = 'Pending to Analyze' in final_status
            status_values = [1 if s == 'Normal' else 0 for s in final_status if s != 'Pending to Analyze']
            
            if has_pending and status_values:
                placeholders = ','.join([f':status{i}' for i in range(len(status_values))])
                conditions.append(f"(final_status IN ({placeholders}) OR final_status IS NULL)")
                for i, status in enumerate(status_values):
                    params[f'status{i}'] = status
            elif has_pending:
                conditions.append("final_status IS NULL")
            elif status_values:
                placeholders = ','.join([f':status{i}' for i in range(len(status_values))])
                conditions.append(f"final_status IN ({placeholders})")
                for i, status in enumerate(status_values):
                    params[f'status{i}'] = status
        
        where_clause = " AND ".join(conditions)
        query = f"""
            SELECT product_type, COUNT(*) as product_count
            FROM product
            WHERE {where_clause}
            GROUP BY product_type
            ORDER BY product_count DESC, product_type
        """
        
        result = repo.fetch_all(query, params)
        
        if result:
            return [
                {"label": f"{row[0]} ({int(row[1])})", "value": row[0]}
                for row in result
            ]
        return []
    finally:
        db.close()


def load_grid_data(group_id, brands=None, categories=None, types=None, final_status=None, iteration=None, page=1, per_page=50, sort_column=None, sort_direction='asc'):
    """Load product data for grid display with pagination and sorting"""
    db = SessionLocal()
    try:
        repo = ProductRepository(db)
        
        conditions = ["group_id = :group_id",
                      "height IS NOT NULL", "width IS NOT NULL", "depth IS NOT NULL"]
        params = {'group_id': group_id}
        
        if brands and len(brands) > 0:
            placeholders = ','.join([f':brand{i}' for i in range(len(brands))])
            conditions.append(f"brand IN ({placeholders})")
            for i, brand in enumerate(brands):
                params[f'brand{i}'] = brand
        
        if categories and len(categories) > 0:
            placeholders = ','.join([f':cat{i}' for i in range(len(categories))])
            conditions.append(f"category IN ({placeholders})")
            for i, cat in enumerate(categories):
                params[f'cat{i}'] = cat
        
        if types and len(types) > 0:
            placeholders = ','.join([f':type{i}' for i in range(len(types))])
            conditions.append(f"product_type IN ({placeholders})")
            for i, ptype in enumerate(types):
                params[f'type{i}'] = ptype
        
        if iteration is not None:
            conditions.append("iteration_closed >= :iteration")
            params['iteration'] = iteration
        
        if final_status and len(final_status) > 0:
            has_pending = 'Pending to Analyze' in final_status
            status_values = [1 if s == 'Normal' else 0 for s in final_status if s != 'Pending to Analyze']
            
            if has_pending and status_values:
                placeholders = ','.join([f':status{i}' for i in range(len(status_values))])
                conditions.append(f"(final_status IN ({placeholders}) OR final_status IS NULL)")
                for i, status in enumerate(status_values):
                    params[f'status{i}'] = status
            elif has_pending:
                conditions.append("final_status IS NULL")
            elif status_values:
                placeholders = ','.join([f':status{i}' for i in range(len(status_values))])
                conditions.append(f"final_status IN ({placeholders})")
                for i, status in enumerate(status_values):
                    params[f'status{i}'] = status
        
        where_clause = " AND ".join(conditions)
        
        # Get total count
        count_query = f"SELECT COUNT(*) FROM product WHERE {where_clause}"
        total_count = repo.fetch_one(count_query, params)
        
        # Build order by clause
        column_map = {
            'sku': 'system_product_id',
            'qb_code': 'qb_code',
            'brand': 'brand',
            'category': 'category',
            'product_type': 'product_type',
            'name': 'name',
            'height': 'height',
            'width': 'width',
            'depth': 'depth',
            'iqr_height_status': 'iqr_height_status',
            'iqr_width_status': 'iqr_width_status',
            'iqr_depth_status': 'iqr_depth_status',
            'iqr_status': 'iqr_status',
            'dbs_status': 'dbs_status',
            'final_status': 'final_status',
            'iteration_closed': 'iteration_closed',
            'skip_status': 'skip_status'
        }
        
        order_by = 'system_product_id ASC'
        if sort_column and sort_column in column_map:
            db_column = column_map[sort_column]
            direction = 'DESC' if sort_direction == 'desc' else 'ASC'
            order_by = f"{db_column} {direction}"
        
        # Calculate offset
        offset = (page - 1) * per_page
        params['limit'] = per_page
        params['offset'] = offset
        
        query = f"""
            SELECT 
                system_product_id as sku,
                qb_code,
                brand,
                category,
                product_type,
                name,
                height,
                width,
                depth,
                iqr_height_status,
                iqr_width_status,
                iqr_depth_status,
                iqr_status,
                dbs_status,
                final_status,
                product_url,
                skip_status,
                product_id,
                base_image_url,
                iteration_closed
            FROM product
            WHERE {where_clause}
            ORDER BY {order_by}
            LIMIT :limit OFFSET :offset
        """
        
        result = repo.fetch_all(query, params)
        
        data = []
        if result:
            for row in result:
                data.append({
                    "sku": row[0] or "",
                    "qb_code": row[1] or "",
                    "brand": row[2] or "",
                    "category": row[3] or "",
                    "product_type": row[4] or "",
                    "name": row[5] or "",
                    "height": float(row[6]) if row[6] else 0,
                    "width": float(row[7]) if row[7] else 0,
                    "depth": float(row[8]) if row[8] else 0,
                    "iqr_height_status": "Normal" if row[9] == 1 else "Outlier" if row[9] == 0 else "-",
                    "iqr_width_status": "Normal" if row[10] == 1 else "Outlier" if row[10] == 0 else "-",
                    "iqr_depth_status": "Normal" if row[11] == 1 else "Outlier" if row[11] == 0 else "-",
                    "iqr_status": "Normal" if row[12] == 1 else "Outlier" if row[12] == 0 else "-",
                    "dbs_status": "Normal" if row[13] == 1 else "Outlier" if row[13] == 0 else "-",
                    "final_status": "Normal" if row[14] == 1 else "Outlier" if row[14] == 0 else "Pending to Analyze",
                    "product_url": row[15] or "",
                    "skip_status": row[16] or "",
                    "product_id": row[17],
                    "base_image_url" : row[18] or "",
                    "iteration_closed": row[19] or ""
                })
        
        return data, total_count
    finally:
        db.close()
