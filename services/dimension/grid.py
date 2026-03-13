from models.base.base import SessionLocal
from repositories.dimension.product_repository import ProductRepository
from repositories.dimension.product_group_repository import ProductGroupRepository
from sqlalchemy import text


def build_filter_conditions(group_id, brands=None, categories=None, types=None, final_status=None, skip_status=None, clusters=None, iteration_id=None, table_alias='p'):
    """Build WHERE clause and params for product filters"""
    params = {'group_id': group_id}
    conditions = [f"{table_alias}.group_id = :group_id"]

    if brands:
        placeholders = ','.join([f':brand{i}' for i in range(len(brands))])
        conditions.append(f"{table_alias}.brand IN ({placeholders})")
        for i, brand in enumerate(brands):
            params[f'brand{i}'] = brand

    if categories:
        placeholders = ','.join([f':cat{i}' for i in range(len(categories))])
        conditions.append(f"{table_alias}.category IN ({placeholders})")
        for i, cat in enumerate(categories):
            params[f'cat{i}'] = cat

    if types:
        placeholders = ','.join([f':type{i}' for i in range(len(types))])
        conditions.append(f"{table_alias}.product_type IN ({placeholders})")
        for i, ptype in enumerate(types):
            params[f'type{i}'] = ptype

    if clusters:
        # Ensure clusters is a list
        if not isinstance(clusters, list):
            clusters = [clusters]
        placeholders = ','.join([f':cluster{i}' for i in range(len(clusters))])
        conditions.append(f"{table_alias}.dbscan_cluster IN ({placeholders})")
        for i, cluster in enumerate(clusters):
            params[f'cluster{i}'] = int(cluster)

    if skip_status:
        skip_values = [1 if s == 'Yes' else 0 for s in skip_status]
        if len(skip_values) == 1:
            if skip_values[0] == 1:
                conditions.append(f"{table_alias}.skip_status = 1")
            else:
                conditions.append(f"({table_alias}.skip_status = 0 OR {table_alias}.skip_status IS NULL)")

    if final_status:
        status_conditions = []
        if 'Pending to Analyze' in final_status:
            status_conditions.append(f"{table_alias}.final_status IS NULL")
        if 'Normal' in final_status:
            status_conditions.append(f"{table_alias}.final_status = 1")
        if 'Outlier' in final_status:
            status_conditions.append(f"{table_alias}.final_status = 0")
        if status_conditions:
            conditions.append(f"({' OR '.join(status_conditions)})")

    return " AND ".join(conditions), params


def get_iteration_history(db, where_clause, params):
    """Get iteration history for products matching filters"""
    history_query = f"""
        SELECT
            dpii.system_product_id,
            pi.eps,
            pi.sample,
            dpii.final_status,
            dpii.analyzed_date
        FROM dimension_product_iteration_item dpii
        INNER JOIN dimension_product_iteration pi ON dpii.iteration_id = pi.iteration_id
        WHERE dpii.system_product_id IN (
            SELECT p.system_product_id FROM dimension_product p WHERE {where_clause}
        )
        AND dpii.analyzed_date IS NOT NULL
        ORDER BY dpii.system_product_id, dpii.analyzed_date DESC
    """
    history_result = db.execute(text(history_query), params).fetchall()

    history_map = {}
    for h_row in history_result:
        pid = h_row[0]
        if pid not in history_map:
            history_map[pid] = []
        history_map[pid].append({
            'eps': float(h_row[1]) if h_row[1] else None,
            'sample': int(h_row[2]) if h_row[2] else None,
            'status': 'Normal' if h_row[3] == 1 else 'Outlier' if h_row[3] == 0 else 'Unknown',
            'date': h_row[4].strftime('%Y-%m-%d %H:%M:%S') if h_row[4] else None
        })

    return history_map


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


def get_iteration_filters(iteration_id):
    """Get brands and categories from iteration items"""
    db = SessionLocal()
    try:
        query = """
            SELECT DISTINCT dpii.brand, dpii.category, pi.product_group_id
            FROM dimension_product_iteration_item dpii
            INNER JOIN dimension_product_iteration pi ON dpii.iteration_id = pi.iteration_id
            WHERE dpii.iteration_id = :iteration_id
        """
        result = db.execute(text(query), {'iteration_id': iteration_id}).fetchall()
        
        if not result:
            return None
        
        brands = list(set([row[0] for row in result if row[0]]))
        categories = list(set([row[1] for row in result if row[1]]))
        group_id = result[0][2] if result else None
        
        return {
            'brands': brands,
            'categories': categories,
            'group_id': group_id
        }
    finally:
        db.close()





def load_grid_data(group_id, brands=None, categories=None, types=None, final_status=None, skip_status=None, clusters=None, iteration_id=None, page=1, per_page=50, sort_column=None, sort_direction='asc', skip_count=False):
    """Load product data from main dimension_product table or iteration tables"""
    db = SessionLocal()
    try:
        # Ensure page and per_page are integers
        page = int(page) if page is not None else 1
        per_page = int(per_page) if per_page is not None else 50
        if iteration_id:
            # Load from iteration tables
            where_conditions = ["pi.iteration_id = :iteration_id"]
            params = {'iteration_id': iteration_id}
            
            if brands:
                placeholders = ','.join([f':brand{i}' for i in range(len(brands))])
                where_conditions.append(f"p.brand IN ({placeholders})")
                for i, brand in enumerate(brands):
                    params[f'brand{i}'] = brand
            
            if categories:
                placeholders = ','.join([f':cat{i}' for i in range(len(categories))])
                where_conditions.append(f"p.category IN ({placeholders})")
                for i, cat in enumerate(categories):
                    params[f'cat{i}'] = cat
            
            if types:
                placeholders = ','.join([f':type{i}' for i in range(len(types))])
                where_conditions.append(f"p.product_type IN ({placeholders})")
                for i, ptype in enumerate(types):
                    params[f'type{i}'] = ptype
            
            if clusters:
                # Ensure clusters is a list
                if not isinstance(clusters, list):
                    clusters = [clusters]
                cluster_conditions = []
                for i, cluster in enumerate(clusters):
                    cluster_name = 'Noise/Outlier' if int(cluster) == -1 else f'Cluster {cluster}'
                    cluster_conditions.append(f":cluster{i}")
                    params[f'cluster{i}'] = cluster_name
                placeholders = ','.join(cluster_conditions)
                where_conditions.append(f"dpii.cluster IN ({placeholders})")
            
            if skip_status:
                skip_values = [1 if s == 'Yes' else 0 for s in skip_status]
                if len(skip_values) == 1:
                    if skip_values[0] == 1:
                        where_conditions.append("p.skip_status = 1")
                    else:
                        where_conditions.append("(p.skip_status = 0 OR p.skip_status IS NULL)")
            
            if final_status:
                status_conditions = []
                if 'Pending to Analyze' in final_status:
                    status_conditions.append("dpii.final_status IS NULL")
                if 'Normal' in final_status:
                    status_conditions.append("dpii.final_status = 1")
                if 'Outlier' in final_status:
                    status_conditions.append("dpii.final_status = 0")
                if status_conditions:
                    where_conditions.append(f"({' OR '.join(status_conditions)})")
            
            where_clause = " AND ".join(where_conditions)
            
            query = f"""
                SELECT
                    p.product_id, p.system_product_id, p.qb_code, p.brand, p.category, p.product_type, p.name,
                    p.height, p.width, p.depth, pi.eps, pi.sample, dpii.final_status, dpii.status, p.skip_status,
                    p.base_image_url, p.product_url, pi.total_items, pi.analyzed_items, pi.pending_items, pi.outlier_items,
                    dpii.cluster_items, dpii.cluster_items_per, dpii.cluster
                FROM dimension_product p
                INNER JOIN dimension_product_iteration_item dpii ON p.system_product_id = dpii.system_product_id
                INNER JOIN dimension_product_iteration pi ON dpii.iteration_id = pi.iteration_id
                WHERE {where_clause}
            """
            
            if not skip_count:
                count_query = f"SELECT COUNT(*) FROM dimension_product p INNER JOIN dimension_product_iteration_item dpii ON p.system_product_id = dpii.system_product_id INNER JOIN dimension_product_iteration pi ON dpii.iteration_id = pi.iteration_id WHERE {where_clause}"
                total_count = db.execute(text(count_query), params).fetchone()[0]
            else:
                total_count = 0
            
            column_map = {
                'system_product_id': 'CAST(p.system_product_id AS UNSIGNED)', 'qb_code': 'p.qb_code',
                'brand': 'p.brand', 'category': 'p.category', 'product_type': 'p.product_type', 'name': 'p.name',
                'height': 'p.height', 'width': 'p.width', 'depth': 'p.depth', 'eps': 'pi.eps', 'sample': 'pi.sample',
                'final_status': 'dpii.final_status', 'skip_status': 'p.skip_status',
                'total_items': 'pi.total_items', 'analyzed_items': 'pi.analyzed_items',
                'pending_items': 'pi.pending_items', 'outlier_items': 'pi.outlier_items',
                'cluster_items': 'dpii.cluster_items', 'cluster_items_per': 'dpii.cluster_items_per',
                'cluster': 'dpii.cluster'
            }
            order_by = 'p.category ASC, dpii.cluster_items ASC, dpii.cluster ASC'
            if sort_column and sort_column in column_map:
                direction = 'DESC' if sort_direction == 'desc' else 'ASC'
                order_by = f"{column_map[sort_column]} {direction}"
            
            offset = (page - 1) * per_page
            params['limit'] = per_page
            params['offset'] = offset
            
            final_query = f"{query} ORDER BY {order_by} LIMIT :limit OFFSET :offset"
            result = db.execute(text(final_query), params).fetchall()
            
            history_map = {}
        else:
            # Load from main product table
            where_clause, params = build_filter_conditions(group_id, brands, categories, types, final_status, skip_status, clusters, iteration_id)

            query = f"""
                SELECT
                    p.product_id, p.system_product_id, p.qb_code, p.brand, p.category, p.product_type, p.name,
                    p.height, p.width, p.depth, latest_iter.eps, latest_iter.sample, p.final_status, p.skip_status,
                    p.iqr_height_status, p.iqr_width_status, p.iqr_depth_status, p.base_image_url, p.product_url,
                    latest_iter.total_items, latest_iter.analyzed_items, latest_iter.pending_items, latest_iter.outlier_items,
                    latest_item.cluster_items, latest_item.cluster_items_per, latest_item.cluster, latest_iter.iteration_id
                FROM dimension_product p
                LEFT JOIN (
                    SELECT dpii.system_product_id, dpii.iteration_id, dpii.cluster_items, dpii.cluster_items_per, dpii.cluster,
                           ROW_NUMBER() OVER (PARTITION BY dpii.system_product_id ORDER BY dpii.iteration_id DESC) as rn
                    FROM dimension_product_iteration_item dpii
                ) latest_item ON p.system_product_id = latest_item.system_product_id AND latest_item.rn = 1
                LEFT JOIN dimension_product_iteration latest_iter ON latest_item.iteration_id = latest_iter.iteration_id
                WHERE {where_clause}
            """

            if not skip_count:
                count_query = f"SELECT COUNT(*) FROM dimension_product p WHERE {where_clause}"
                total_count = db.execute(text(count_query), params).fetchone()[0]
            else:
                total_count = 0

            column_map = {
                'system_product_id': 'CAST(p.system_product_id AS UNSIGNED)', 'qb_code': 'p.qb_code',
                'brand': 'p.brand', 'category': 'p.category', 'product_type': 'p.product_type', 'name': 'p.name',
                'height': 'p.height', 'width': 'p.width', 'depth': 'p.depth', 'eps': 'latest_iter.eps', 'sample': 'latest_iter.sample',
                'final_status': 'p.final_status', 'skip_status': 'p.skip_status',
                'total_items': 'latest_iter.total_items', 'analyzed_items': 'latest_iter.analyzed_items',
                'pending_items': 'latest_iter.pending_items', 'outlier_items': 'latest_iter.outlier_items',
                'cluster_items': 'latest_item.cluster_items', 'cluster_items_per': 'latest_item.cluster_items_per',
                'cluster': 'latest_item.cluster'
            }
            order_by = 'p.category ASC, latest_item.cluster_items ASC, latest_item.cluster ASC'
            if sort_column and sort_column in column_map:
                direction = 'DESC' if sort_direction == 'desc' else 'ASC'
                order_by = f"{column_map[sort_column]} {direction}"

            offset = (page - 1) * per_page
            params['limit'] = per_page
            params['offset'] = offset

            final_query = f"{query} ORDER BY {order_by} LIMIT :limit OFFSET :offset"
            result = db.execute(text(final_query), params).fetchall()

            if result:
                product_ids = [row[1] for row in result]
                placeholders = ','.join([f':pid{i}' for i in range(len(product_ids))])
                history_params = {f'pid{i}': pid for i, pid in enumerate(product_ids)}

                history_query = f"""
                    SELECT dpii.system_product_id, pi.eps, pi.sample, dpii.final_status, dpii.analyzed_date
                    FROM dimension_product_iteration_item dpii
                    INNER JOIN dimension_product_iteration pi ON dpii.iteration_id = pi.iteration_id
                    WHERE dpii.system_product_id IN ({placeholders}) AND dpii.analyzed_date IS NOT NULL
                    ORDER BY dpii.system_product_id, dpii.analyzed_date DESC
                """
                history_result = db.execute(text(history_query), history_params).fetchall()

                history_map = {}
                for h_row in history_result:
                    pid = h_row[0]
                    if pid not in history_map:
                        history_map[pid] = []
                    history_map[pid].append({
                        'eps': float(h_row[1]) if h_row[1] else None,
                        'sample': int(h_row[2]) if h_row[2] else None,
                        'status': 'Normal' if h_row[3] == 1 else 'Outlier' if h_row[3] == 0 else 'Unknown',
                        'date': h_row[4].strftime('%Y-%m-%d %H:%M:%S') if h_row[4] else None
                    })
            else:
                history_map = {}

        data = []
        for row in result:
            system_product_id = row[1] or ""
            if iteration_id:
                final_status_val = row[12]
                # Determine final_status based on final_status column from dimension_product_iteration_item
                if final_status_val == 1:
                    final_status_display = "Normal"
                elif final_status_val == 0:
                    final_status_display = "Outlier"
                else:
                    final_status_display = "Pending to Analyze"
                
                data.append({
                    "product_id": row[0], "system_product_id": system_product_id, "qb_code": row[2] or "",
                    "brand": row[3] or "", "category": row[4] or "", "product_type": row[5] or "", "name": row[6] or "",
                    "height": float(row[7]) if row[7] else 0, "width": float(row[8]) if row[8] else 0, "depth": float(row[9]) if row[9] else 0,
                    "eps": float(row[10]) if row[10] else "-", "sample": int(row[11]) if row[11] else "-",
                    "final_status": final_status_display,
                    "skip_status": row[14], "iqr_height_status": 1,
                    "iqr_width_status": 1, "iqr_depth_status": 1,
                    "base_image_url": row[15] or "", "product_url": row[16] or "",
                    "total_items": int(row[17]) if row[17] else 0,
                    "analyzed_items": int(row[18]) if row[18] else 0,
                    "pending_items": int(row[19]) if row[19] else 0,
                    "outlier_items": int(row[20]) if row[20] else 0,
                    "cluster_items": int(row[21]) if row[21] else 0,
                    "cluster_items_per": float(row[22]) if row[22] else 0.0,
                    "cluster": row[23] or "",
                    "iteration_id": iteration_id,
                    "iteration_history": []
                })
            else:
                data.append({
                    "product_id": row[0], "system_product_id": system_product_id, "qb_code": row[2] or "",
                    "brand": row[3] or "", "category": row[4] or "", "product_type": row[5] or "", "name": row[6] or "",
                    "height": float(row[7]) if row[7] else 0, "width": float(row[8]) if row[8] else 0, "depth": float(row[9]) if row[9] else 0,
                    "eps": float(row[10]) if row[10] else "-", "sample": int(row[11]) if row[11] else "-",
                    "final_status": "Normal" if row[12] == 1 else "Outlier" if row[12] == 0 else "Pending to Analyze",
                    "skip_status": row[13], "iqr_height_status": row[14] if len(row) > 14 else 1,
                    "iqr_width_status": row[15] if len(row) > 15 else 1, "iqr_depth_status": row[16] if len(row) > 16 else 1,
                    "base_image_url": row[17] or "", "product_url": row[18] or "",
                    "total_items": int(row[19]) if len(row) > 19 and row[19] else 0,
                    "analyzed_items": int(row[20]) if len(row) > 20 and row[20] else 0,
                    "pending_items": int(row[21]) if len(row) > 21 and row[21] else 0,
                    "outlier_items": int(row[22]) if len(row) > 22 and row[22] else 0,
                    "cluster_items": int(row[23]) if len(row) > 23 and row[23] else 0,
                    "cluster_items_per": float(row[24]) if len(row) > 24 and row[24] else 0.0,
                    "cluster": row[25] if len(row) > 25 else "",
                    "iteration_id": row[26] if len(row) > 26 else None,
                    "iteration_history": history_map.get(system_product_id, [])
                })

        return data, total_count
    finally:
        db.close()
