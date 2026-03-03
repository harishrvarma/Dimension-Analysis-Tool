from models.base.base import SessionLocal
from repositories.dimension.product_repository import ProductRepository
from repositories.dimension.product_group_repository import ProductGroupRepository
from sqlalchemy import text


def get_product_groups():
    """Get all product groups for dropdown with default_selected"""
    db = SessionLocal()
    try:
        repo = ProductGroupRepository(db)
        df = repo.get_all_groups()
        if df.empty:
            return [], None

        groups = [
            {
                "label": f"{row['name']} ({row['product_count']})",
                "value": int(row['group_id']),
                "default_selected": bool(row.get('default_selected', 0))
            }
            for _, row in df.iterrows()
        ]

        # Find default selected group
        default_group = next((g for g in groups if g['default_selected']), None)

        return groups, default_group['value'] if default_group else None
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
            FROM dimension_product
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
            FROM dimension_product
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
            FROM dimension_product
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


def get_analyzed_status(group_id, brands=None, categories=None):
    """Get analyzed status for brands, categories, and types"""
    db = SessionLocal()
    try:
        repo = ProductRepository(db)
        
        result = {
            'brands': {},
            'categories': {},
            'types': {}
        }
        
        # Get brand analyzed status
        brand_query = f"""
            SELECT brand,
                   COUNT(*) as total,
                   SUM(CASE WHEN final_status IS NOT NULL THEN 1 ELSE 0 END) as analyzed
            FROM dimension_product
            WHERE group_id = :group_id AND brand IS NOT NULL
                  AND height IS NOT NULL AND width IS NOT NULL AND depth IS NOT NULL
            GROUP BY brand
        """
        brand_result = repo.fetch_all(brand_query, {'group_id': group_id})
        for row in brand_result:
            brand, total, analyzed = row[0], int(row[1]), int(row[2])
            if analyzed == total:
                result['brands'][brand] = 'full'
            elif analyzed > 0:
                result['brands'][brand] = 'partial'
        
        # Get category analyzed status
        cat_conditions = ["group_id = :group_id", "category IS NOT NULL",
                         "height IS NOT NULL", "width IS NOT NULL", "depth IS NOT NULL"]
        cat_params = {'group_id': group_id}
        
        if brands and len(brands) > 0:
            placeholders = ','.join([f':brand{i}' for i in range(len(brands))])
            cat_conditions.append(f"brand IN ({placeholders})")
            for i, brand in enumerate(brands):
                cat_params[f'brand{i}'] = brand
        
        cat_where = " AND ".join(cat_conditions)
        cat_query = f"""
            SELECT category,
                   COUNT(*) as total,
                   SUM(CASE WHEN final_status IS NOT NULL THEN 1 ELSE 0 END) as analyzed
            FROM dimension_product
            WHERE {cat_where}
            GROUP BY category
        """
        cat_result = repo.fetch_all(cat_query, cat_params)
        for row in cat_result:
            category, total, analyzed = row[0], int(row[1]), int(row[2])
            if analyzed == total:
                result['categories'][category] = 'full'
            elif analyzed > 0:
                result['categories'][category] = 'partial'
        
        # Get type analyzed status
        type_conditions = ["group_id = :group_id", "product_type IS NOT NULL",
                          "height IS NOT NULL", "width IS NOT NULL", "depth IS NOT NULL"]
        type_params = {'group_id': group_id}
        
        if brands and len(brands) > 0:
            placeholders = ','.join([f':brand{i}' for i in range(len(brands))])
            type_conditions.append(f"brand IN ({placeholders})")
            for i, brand in enumerate(brands):
                type_params[f'brand{i}'] = brand
        
        if categories and len(categories) > 0:
            placeholders = ','.join([f':cat{i}' for i in range(len(categories))])
            type_conditions.append(f"category IN ({placeholders})")
            for i, cat in enumerate(categories):
                type_params[f'cat{i}'] = cat
        
        type_where = " AND ".join(type_conditions)
        type_query = f"""
            SELECT product_type,
                   COUNT(*) as total,
                   SUM(CASE WHEN final_status IS NOT NULL THEN 1 ELSE 0 END) as analyzed
            FROM dimension_product
            WHERE {type_where}
            GROUP BY product_type
        """
        type_result = repo.fetch_all(type_query, type_params)
        for row in type_result:
            ptype, total, analyzed = row[0], int(row[1]), int(row[2])
            if analyzed == total:
                result['types'][ptype] = 'full'
            elif analyzed > 0:
                result['types'][ptype] = 'partial'
        
        return result
    finally:
        db.close()


def get_configurations(group_id, brands=None, categories=None, types=None, skip_status=None, final_status=None):
    """Get unique eps+sample configurations from product_iteration"""
    db = SessionLocal()
    try:
        # If 'Pending to Analyze' is selected, return empty list
        if final_status and 'Pending to Analyze' in final_status and len(final_status) == 1:
            return []

        conditions = ["pi.product_group_id = :group_id"]
        params = {'group_id': group_id}

        if brands and len(brands) > 0:
            placeholders = ','.join([f':brand{i}' for i in range(len(brands))])
            conditions.append(f"pi.brand IN ({placeholders})")
            for i, brand in enumerate(brands):
                params[f'brand{i}'] = brand

        if categories and len(categories) > 0:
            placeholders = ','.join([f':cat{i}' for i in range(len(categories))])
            conditions.append(f"pi.category IN ({placeholders})")
            for i, cat in enumerate(categories):
                params[f'cat{i}'] = cat

        if types and len(types) > 0:
            type_conditions = []
            for i, ptype in enumerate(types):
                type_conditions.append(f"(pi.product_type LIKE :type{i} OR pi.product_type LIKE :type_start{i} OR pi.product_type LIKE :type_mid{i} OR pi.product_type LIKE :type_end{i})")
                params[f'type{i}'] = ptype
                params[f'type_start{i}'] = f"{ptype}|%"
                params[f'type_mid{i}'] = f"%|{ptype}|%"
                params[f'type_end{i}'] = f"%|{ptype}"
            conditions.append(f"({' OR '.join(type_conditions)})")

        if skip_status and len(skip_status) > 0:
            skip_values = [1 if s == 'Yes' else 0 for s in skip_status]
            if len(skip_values) == 1:
                skip_subquery = "EXISTS (SELECT 1 FROM dimension_product p WHERE p.system_product_id IN (SELECT system_product_id FROM dimension_product_iteration_item WHERE iteration_id = pi.iteration_id)"
                if skip_values[0] == 1:
                    skip_subquery += " AND p.skip_status = 1)"
                else:
                    skip_subquery += " AND (p.skip_status = 0 OR p.skip_status IS NULL))"
                conditions.append(skip_subquery)

        where_clause = " AND ".join(conditions)
        query = f"""
            SELECT DISTINCT pi.eps, pi.sample
            FROM dimension_product_iteration pi
            WHERE {where_clause} AND pi.eps IS NOT NULL AND pi.sample IS NOT NULL
            ORDER BY pi.eps, pi.sample
        """

        result = db.execute(text(query), params).fetchall()

        if result:
            return [
                {"label": f"eps {float(row[0])}, sample {int(row[1])}", "value": f"{float(row[0])}_{int(row[1])}"}
                for row in result
            ]
        return []
    finally:
        db.close()


def load_grid_data(group_id, brands=None, categories=None, types=None, final_status=None, skip_status=None, iteration=None, configuration=None, page=1, per_page=50, sort_column=None, sort_direction='asc', skip_count=False, include_iteration_count=False):
    """Load aggregated product data for grid display"""
    db = SessionLocal()
    try:
        params = {'group_id': group_id}
        
        # Build iteration filter if configuration is selected
        iter_filter = ""
        join_type = "LEFT JOIN"
        if configuration:
            configs = configuration if isinstance(configuration, list) else [configuration]
            iter_conditions = []
            for idx, config in enumerate(configs):
                eps, sample = config.split('_')
                params[f'eps{idx}'] = float(eps)
                params[f'sample{idx}'] = int(sample)
                iter_conditions.append(f"(pi.eps = :eps{idx} AND pi.sample = :sample{idx})")
            iter_filter = f"AND ({' OR '.join(iter_conditions)})"
            join_type = "INNER JOIN"

        # Build product filter conditions
        prod_conditions = []
        
        if brands and len(brands) > 0:
            placeholders = ','.join([f':brand{i}' for i in range(len(brands))])
            prod_conditions.append(f"p.brand IN ({placeholders})")
            for i, brand in enumerate(brands):
                params[f'brand{i}'] = brand
        
        if categories and len(categories) > 0:
            placeholders = ','.join([f':cat{i}' for i in range(len(categories))])
            prod_conditions.append(f"p.category IN ({placeholders})")
            for i, cat in enumerate(categories):
                params[f'cat{i}'] = cat
        
        if types and len(types) > 0:
            placeholders = ','.join([f':type{i}' for i in range(len(types))])
            prod_conditions.append(f"p.product_type IN ({placeholders})")
            for i, ptype in enumerate(types):
                params[f'type{i}'] = ptype
        
        if skip_status and len(skip_status) > 0:
            skip_values = [1 if s == 'Yes' else 0 for s in skip_status]
            if len(skip_values) == 1:
                if skip_values[0] == 1:
                    prod_conditions.append("p.skip_status = 1")
                else:
                    prod_conditions.append("(p.skip_status = 0 OR p.skip_status IS NULL)")

        prod_where = " AND " + " AND ".join(prod_conditions) if prod_conditions else ""

        # Main aggregation query
        agg_query = f"""
            SELECT
                p.system_product_id,
                p.qb_code,
                p.brand,
                p.category,
                p.product_type,
                p.name,
                p.height,
                p.width,
                p.depth,
                p.weight,
                p.product_url,
                p.skip_status,
                p.product_id,
                p.base_image_url,
                p.iqr_height_status,
                p.iqr_width_status,
                p.iqr_depth_status,
                COALESCE(agg.iteration_count, 0) as iteration_count,
                COALESCE(agg.outlier_count, 0) as outlier_count,
                COALESCE(agg.total_items, 0) as total_items,
                COALESCE(agg.outlier_percentage, 0) as outlier_percentage,
                COALESCE(agg.final_status, 0) as final_status,
                COALESCE(agg.config_info_with_manual, '') as config_info,
                COALESCE(agg.eps, '') as eps,
                COALESCE(agg.sample, '') as sample,
                COALESCE(agg.manual_outlier_count, 0) as manual_outlier_count,
                COALESCE(agg.iteration_product_type, '') as iteration_product_type,
                COALESCE(agg.iteration_product_count, 0) as iteration_product_count
            FROM dimension_product p
            {join_type} (
                SELECT
                    dpii.system_product_id,
                    COUNT(DISTINCT dpii.id) as iteration_count,
                    SUM(CASE WHEN dpii.status = 0 THEN 1 ELSE 0 END) as outlier_count,
                    COUNT(dpii.id) as total_items,
                    CASE
                        WHEN COUNT(dpii.id) = 0 THEN 0
                        ELSE ROUND((SUM(CASE WHEN dpii.status = 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(dpii.id)), 2)
                    END as outlier_percentage,
                    CASE
                        WHEN SUM(CASE WHEN dpii.status = 1 THEN 1 ELSE 0 END) > SUM(CASE WHEN dpii.status = 0 THEN 1 ELSE 0 END) THEN 1
                        ELSE 0
                    END as final_status,
                    CASE WHEN COUNT(DISTINCT pi.iteration_id) = 1 THEN MAX(pi.eps) ELSE NULL END as eps,
                    CASE WHEN COUNT(DISTINCT pi.iteration_id) = 1 THEN MAX(pi.sample) ELSE NULL END as sample,
                    CASE WHEN COUNT(DISTINCT pi.iteration_id) = 1 AND MAX(pi.product_type) IS NOT NULL AND MAX(pi.product_type) != '' THEN MAX(pi.product_type) ELSE NULL END as iteration_product_type,
                    {'CASE WHEN COUNT(DISTINCT pi.iteration_id) = 1 THEN MAX(iter_counts.product_count) ELSE NULL END' if include_iteration_count else 'NULL'} as iteration_product_count,
                    SUM(CASE WHEN dpii.outlier_mode = 1 THEN 1 ELSE 0 END) as manual_outlier_count,
                    GROUP_CONCAT(
                        CONCAT('EPS: ', pi.eps, ', Sample: ', pi.sample, ' (',
                               CASE
                                   WHEN dpii.outlier_mode = 1 THEN 'Manual Outlier'
                                   WHEN dpii.status = 0 THEN 'Outlier'
                                   WHEN dpii.status = 1 THEN 'Normal'
                                   ELSE 'Unknown'
                               END, ')')
                        ORDER BY pi.eps, pi.sample
                        SEPARATOR '
'
                    ) as config_info_with_manual
                FROM dimension_product_iteration_item dpii
                INNER JOIN dimension_product_iteration pi ON dpii.iteration_id = pi.iteration_id
                {'LEFT JOIN (SELECT iteration_id, COUNT(DISTINCT system_product_id) as product_count FROM dimension_product_iteration_item GROUP BY iteration_id) iter_counts ON pi.iteration_id = iter_counts.iteration_id' if include_iteration_count else ''}
                WHERE pi.product_group_id = :group_id {iter_filter}
                GROUP BY dpii.system_product_id
            ) agg ON p.system_product_id = agg.system_product_id
            WHERE p.group_id = :group_id{prod_where}
        """

        # Wrap query to apply final_status filter on computed values
        is_wrapped = False
        if final_status and len(final_status) > 0:
            final_status_conditions = []
            if 'Pending to Analyze' in final_status:
                final_status_conditions.append("(iteration_count = 0)")
            if 'Normal' in final_status:
                final_status_conditions.append("(final_status = 1 AND iteration_count > 0)")
            if 'Outlier' in final_status:
                final_status_conditions.append("(final_status = 0 AND iteration_count > 0)")

            if final_status_conditions:
                agg_query = f"SELECT * FROM ({agg_query}) as filtered WHERE {' OR '.join(final_status_conditions)}"
                is_wrapped = True

        # Get total count
        if skip_count:
            total_count = 0
        else:
            count_query = f"SELECT COUNT(*) FROM ({agg_query}) as agg"
            total_count = db.execute(text(count_query), params).fetchone()[0]

        # Build order by
        if is_wrapped:
            column_map = {
                'system_product_id': 'CAST(system_product_id AS UNSIGNED)',
                'qb_code': 'qb_code',
                'brand': 'brand',
                'category': 'category',
                'product_type': 'product_type',
                'name': 'name',
                'height': 'height',
                'width': 'width',
                'depth': 'depth',
                'final_status': 'final_status',
                'manual_outlier': 'manual_outlier_count',
                'skip_status': 'skip_status',
                'iterations': 'iteration_count',
                'outlier_percentage': 'outlier_percentage'
            }
            order_by = 'CAST(system_product_id AS UNSIGNED) ASC'
        else:
            column_map = {
                'system_product_id': 'CAST(p.system_product_id AS UNSIGNED)',
                'qb_code': 'p.qb_code',
                'brand': 'p.brand',
                'category': 'p.category',
                'product_type': 'p.product_type',
                'name': 'p.name',
                'height': 'p.height',
                'width': 'p.width',
                'depth': 'p.depth',
                'final_status': 'final_status',
                'manual_outlier': 'manual_outlier_count',
                'skip_status': 'p.skip_status',
                'iterations': 'iteration_count',
                'outlier_percentage': 'outlier_percentage'
            }
            order_by = 'CAST(p.system_product_id AS UNSIGNED) ASC'
        
        if sort_column and sort_column in column_map:
            direction = 'DESC' if sort_direction == 'desc' else 'ASC'
            order_by = f"{column_map[sort_column]} {direction}"
        
        # Paginate
        offset = (page - 1) * per_page
        params['limit'] = per_page
        params['offset'] = offset
        
        final_query = f"{agg_query} ORDER BY {order_by} LIMIT :limit OFFSET :offset"
        result = db.execute(text(final_query), params).fetchall()
        
        data = []
        if result:
            for row in result:
                data.append({
                    "system_product_id": row[0] or "",
                    "qb_code": row[1] or "",
                    "brand": row[2] or "",
                    "category": row[3] or "",
                    "product_type": row[4] or "",
                    "name": row[5] or "",
                    "height": float(row[6]) if row[6] else 0,
                    "width": float(row[7]) if row[7] else 0,
                    "depth": float(row[8]) if row[8] else 0,
                    "weight": float(row[9]) if row[9] else 0,
                    "product_url": row[10] or "",
                    "skip_status": row[11] or "",
                    "product_id": row[12],
                    "base_image_url": row[13] or "",
                    "iqr_height_status": int(row[14]) if row[14] is not None else 1,
                    "iqr_width_status": int(row[15]) if row[15] is not None else 1,
                    "iqr_depth_status": int(row[16]) if row[16] is not None else 1,
                    "iterations": int(row[17]) if row[17] else 0,
                    "outlier_count": int(row[18]) if row[18] else 0,
                    "total_items": int(row[19]) if row[19] else 0,
                    "outlier_percentage": float(row[20]) if row[20] else 0,
                    "final_status": "Normal" if row[21] == 1 else "Outlier" if row[21] == 0 and row[17] > 0 else "Pending to Analyze",
                    "config_info": row[22] or "",
                    "eps": float(row[23]) if row[23] else "-",
                    "sample": int(row[24]) if row[24] else "-",
                    "manual_outlier": "Yes" if row[25] and row[25] > 0 else ("No" if row[18] and row[18] > 0 else "-"),
                    "manual_outlier_count": int(row[25]) if row[25] else 0,
                    "iteration_product_type": row[26] if row[26] else "-",
                    "iteration_product_count": int(row[27]) if row[27] else "-",
                    "product_type_count": (len(row[26].split('|')) if row[26] and row[26] != '-' else "-") if row[26] else "-"
                })

        return data, total_count
    finally:
        db.close()
