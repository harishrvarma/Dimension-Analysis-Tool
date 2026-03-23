from models.base.base import SessionLocal
from repositories.pricing.product_repository import ProductRepository
from repositories.pricing.product_group_repository import ProductGroupRepository
from repositories.pricing.product_iteration_repository import ProductIterationRepository
from models.pricing.product import Product
from constants import ALGO_DBSCAN
import pandas as pd
import numpy as np
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler
 

def get_product_groups():
    """Get all product groups for dropdown with default_selected"""
    db = SessionLocal()
    try:
        repo = ProductGroupRepository(db)
        df = repo.get_all_groups()
        if df.empty:
            return []

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


def get_brands_for_group(group_id):
    """Get brands with product counts and analysis status for a specific product group"""
    db = SessionLocal()
    try:
        repo = ProductRepository(db)
        df = repo.get_brands_for_group(group_id)
        if df.empty:
            return []
        return [
            {
                "label": f"{row['brand']} ({row['product_count']})",
                "value": row['brand'],
                "brand_id": int(row['brand_id']),
                "analyzed_count": int(row['analyzed_count']),
                "total_count": int(row['product_count'])
            }
            for _, row in df.iterrows()
        ]
    finally:
        db.close()


def get_brands_for_group_filtered(group_id, categories=None, types=None):
    """Get brands filtered by categories and/or types (for interconnected filters)"""
    db = SessionLocal()
    try:
        repo = ProductRepository(db)
        df = repo.get_brands_for_group_filtered(group_id, categories=categories, types=types)
        if df.empty:
            return []
        return [
            {
                "label": f"{row['brand']} ({row['product_count']})",
                "value": row['brand'],
                "brand_id": int(row['brand_id']),
                "analyzed_count": int(row['analyzed_count']),
                "total_count": int(row['product_count'])
            }
            for _, row in df.iterrows()
        ]
    finally:
        db.close()


def get_categories_for_group(group_id, brands=None, types=None):
    """Get categories for a group with analysis status, optionally filtered by brands and types"""
    db = SessionLocal()
    try:
        repo = ProductRepository(db)
        df = repo.get_categories_for_group(group_id, brands, types=types)
        if df.empty:
            return []
        return [
            {
                "label": f"{row['category']} ({row['product_count']})",
                "value": row['category'],
                "category_id": int(row['category_id']),
                "analyzed_count": int(row['analyzed_count']),
                "total_count": int(row['product_count'])
            }
            for _, row in df.iterrows()
        ]
    finally:
        db.close()


def get_types_for_group(group_id, brands=None, categories=None):
    """Get product types for a group with analysis status"""
    db = SessionLocal()
    try:
        repo = ProductRepository(db)
        # Normalize categories to a single string for the repo (supports comma-separated)
        if isinstance(categories, list):
            category_str = ','.join(categories) if categories else None
        else:
            category_str = categories
        df = repo.get_types_for_group(group_id, brands, category_str)
        if df.empty:
            return []
        return [
            {
                "label": f"{row['product_type']} ({row['product_count']})",
                "value": row['product_type'],
                "analyzed_count": int(row['analyzed_count']),
                "total_count": int(row['product_count'])
            }
            for _, row in df.iterrows()
        ]
    finally:
        db.close()


def load_products_filtered(group_id, brands=None, category=None, types=None, iteration=1, for_save=False, for_display=False, axis_cols=None):
    """Load product data from database with filters for specific iteration"""
    db = SessionLocal()
    try:
        repo = ProductRepository(db)
        if iteration > 1 and not for_save:
            df = repo.load_products_for_iteration(group_id, iteration, brands, category, types, for_display=for_display, axis_cols=axis_cols)
        else:
            df = repo.load_products_filtered(group_id, brands, category, types, axis_cols=axis_cols)
        
        if not df.empty:
            # Rename columns
            df = df.rename(columns={
                'qb_code': 'SKU',
                'brand': 'Brand',
                'category': 'Category',
                'product_type': 'Type',
                'name': 'Name',
                'base_image_url': 'imageUrl',
                'product_url': 'url_key'
            })
            
            # Ensure numeric columns for all known pricing/dimension columns
            numeric_cols = ['mfr_cost', 'shipping_cost', 'price', 'profit_margin', 'weight']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Remove rows with missing axis data (only drop cols that exist)
            if axis_cols and len(axis_cols) == 3:
                drop_cols = [c for c in axis_cols if c in df.columns]
            else:
                drop_cols = [c for c in ['mfr_cost', 'shipping_cost', 'profit_margin'] if c in df.columns]
            if drop_cols:
                df = df.dropna(subset=drop_cols)
        
        return df
    finally:
        db.close()




# def detect_outliers_dbscan(filtered_df, eps=1.0, min_samples=4):
#     """Detect outliers using DBSCAN"""
#     df_dbscan = filtered_df.copy()
    
#     X = df_dbscan[['mfr_cost', 'shipping_cost', 'profit_margin']].values
    
#     scaler = StandardScaler()
#     X_scaled = scaler.fit_transform(X)
    
#     dbscan = DBSCAN(eps=eps, min_samples=min_samples, metric='euclidean')
#     clusters = dbscan.fit_predict(X_scaled)
    
#     is_outlier_dbscan = pd.Series((clusters == -1), index=df_dbscan.index)
    
#     df_dbscan['dbscan_cluster'] = clusters
#     df_dbscan['dbscan_is_outlier'] = is_outlier_dbscan
    
#     return is_outlier_dbscan, df_dbscan

# def detect_outliers_dbscan(filtered_df, eps=1.0, min_samples=4):
#     """Detect outliers using DBSCAN"""

#     df_dbscan = filtered_df.copy()

#     # Ensure required columns exist
#     required_cols = ['mfr_cost', 'shipping_cost', 'profit_margin']
#     missing_cols = [c for c in required_cols if c not in df_dbscan.columns]
#     if missing_cols:
#         raise ValueError(f"Missing required columns: {missing_cols}")

#     # Remove rows with NaN values
#     df_dbscan = df_dbscan.dropna(subset=required_cols)

#     # Create ratio features (better shape detection)
#     df_dbscan['H_W'] = df_dbscan['H'] / df_dbscan['W']
#     df_dbscan['H_D'] = df_dbscan['H'] / df_dbscan['D']
#     df_dbscan['W_D'] = df_dbscan['W'] / df_dbscan['D']

#     # Final feature set
#     #features = ['mfr_cost', 'shipping_cost', 'price', 'H_W', 'H_D', 'W_D']
#     features = ['H_W', 'H_D', 'W_D']
#     X = df_dbscan[features].values

#     # Scale features
#     scaler = StandardScaler()
#     X_scaled = scaler.fit_transform(X)

#     # Run DBSCAN
#     dbscan = DBSCAN(eps=eps, min_samples=min_samples, metric='euclidean')
#     clusters = dbscan.fit_predict(X_scaled)

#     # Identify outliers
#     is_outlier_dbscan = pd.Series((clusters == -1), index=df_dbscan.index)

#     # Store results
#     df_dbscan['dbscan_cluster'] = clusters
#     df_dbscan['dbscan_is_outlier'] = is_outlier_dbscan

#     return is_outlier_dbscan, df_dbscan

def detect_outliers_dbscan(filtered_df, eps=1.0, min_samples=4, algorithm_settings=None, axis_cols=None):
    print(f"Running DBSCAN with eps={eps}, min_samples={min_samples}, settings={algorithm_settings}")
    """Detect outliers using DBSCAN.

    axis_cols: list of 3 column codes [x_code, y_code, z_code] to use as features.
    algorithm_settings controls which feature groups are used:
    - shape: ratios between axis columns
    - size: raw axis values
    - volume: product of axis values
    """

    df_dbscan = filtered_df.copy()

    # Determine the 3 axis columns to use
    if axis_cols and len(axis_cols) == 3:
        col_x, col_y, col_z = axis_cols
    else:
        raise ValueError("axis_cols with 3 column codes is required for DBSCAN")

    required_cols = [col_x, col_y, col_z]
    missing_cols = [c for c in required_cols if c not in df_dbscan.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    df_dbscan = df_dbscan.dropna(subset=required_cols)
    for _c in required_cols:
        df_dbscan[_c] = pd.to_numeric(df_dbscan[_c], errors='coerce').astype(float)
    df_dbscan = df_dbscan.dropna(subset=required_cols)
    df_dbscan = df_dbscan[(df_dbscan[required_cols] != 0).all(axis=1)]

    valid_settings = {'shape', 'size', 'volume'}
    settings = {
        str(s).strip().lower()
        for s in (algorithm_settings or [])
        if str(s).strip()
    }
    settings = settings & valid_settings
    if not settings:
        settings = set(valid_settings)

    eps_val = 1e-6
    features = []

    if 'size' in settings:
        features.extend(required_cols)

    if 'shape' in settings:
        df_dbscan['_ax_ratio_xy'] = df_dbscan[col_x] / (df_dbscan[col_y] + eps_val)
        df_dbscan['_ax_ratio_yz'] = df_dbscan[col_y] / (df_dbscan[col_z] + eps_val)
        df_dbscan['_ax_ratio_xz'] = df_dbscan[col_x] / (df_dbscan[col_z] + eps_val)
        features.extend(['_ax_ratio_xy', '_ax_ratio_yz', '_ax_ratio_xz'])

    if 'volume' in settings:
        df_dbscan['_ax_volume'] = df_dbscan[col_x] * df_dbscan[col_y] * df_dbscan[col_z]
        features.append('_ax_volume')

    seen = set()
    features = [f for f in features if not (f in seen or seen.add(f))]
    print(f"DBSCAN using features: {features}")
    X = df_dbscan[features].values

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    dbscan = DBSCAN(eps=eps, min_samples=min_samples, metric='euclidean')
    clusters = dbscan.fit_predict(X_scaled)

    is_outlier_dbscan = pd.Series((clusters == -1), index=df_dbscan.index)
    df_dbscan['dbscan_cluster'] = clusters
    df_dbscan['dbscan_is_outlier'] = is_outlier_dbscan

    return is_outlier_dbscan, df_dbscan

def fetch_com_col_values(db, system_product_ids, col_codes):
    """Fetch com column values from pricing_product for given system_product_ids.
    col_codes: list of 3 column code strings [x_com_code, y_com_code, z_com_code]
    Returns dict: {system_product_id: {col_code: value}}
    """
    if not system_product_ids or not col_codes or not any(col_codes):
        return {}
    valid_codes = [c for c in col_codes if c]
    if not valid_codes:
        return {}
    from sqlalchemy import text
    cols_sql = ', '.join(valid_codes)
    placeholders = ', '.join([f':spid{i}' for i in range(len(system_product_ids))])
    params = {f'spid{i}': spid for i, spid in enumerate(system_product_ids)}
    query = text(f"SELECT system_product_id, {cols_sql} FROM pricing_product WHERE system_product_id IN ({placeholders})")
    rows = db.execute(query, params).fetchall()
    result = {}
    for row in rows:
        spid = row[0]
        result[spid] = {code: row[idx + 1] for idx, code in enumerate(valid_codes)}
    return result


def get_iteration_history(group_id, categories, category_ids=None, brand_ids=None):
    """Get iteration history from database - by group_id and one or more category IDs"""
    from repositories.pricing.product_iteration_repository import ProductIterationRepository

    db = SessionLocal()
    try:
        iter_repo = ProductIterationRepository(db)
        all_history = []
        seen_ids = set()

        has_brands = bool(brand_ids)
        has_categories = bool(category_ids or categories)

        if has_brands and has_categories:
            # Both selected: OR match
            cat_lookup = [str(cid) for cid in category_ids] if category_ids else categories
            for item in iter_repo.get_iteration_summary_by_brand_or_category(group_id, brand_ids, cat_lookup):
                if item['iteration_id'] not in seen_ids:
                    seen_ids.add(item['iteration_id'])
                    all_history.append(item)
        elif has_categories:
            # Category only: match by category
            lookup_values = [str(cid) for cid in category_ids] if category_ids else categories
            for val in lookup_values:
                for item in iter_repo.get_iteration_summary_by_group_category(group_id, val):
                    if item['iteration_id'] not in seen_ids:
                        seen_ids.add(item['iteration_id'])
                        all_history.append(item)
        elif has_brands:
            # Brand only: match by brand
            for item in iter_repo.get_iteration_summary_by_group_brand(group_id, brand_ids):
                if item['iteration_id'] not in seen_ids:
                    seen_ids.add(item['iteration_id'])
                    all_history.append(item)

        return sorted(all_history, key=lambda x: x['iteration_id'])
    finally:
        db.close()


def reset_iterations(group_id, category=None):
    """Reset all iterations for a product group, optionally filtered by category.
    If category is None, resets all iterations for the entire group.
    """
    from repositories.pricing.product_iteration_repository import ProductIterationRepository
    from models.pricing.product_iteration import ProductIteration
    from models.pricing.product_iteration_item import PricingProductIterationItem
    from models.pricing.product import Product
    
    db = SessionLocal()
    try:
        # Materialize iteration IDs once upfront
        iter_q = db.query(ProductIteration.iteration_id).filter(
            ProductIteration.product_group_id == group_id
        )
        if category:
            iter_q = iter_q.filter(ProductIteration.category == category)
        iteration_ids = [row[0] for row in iter_q.all()]
        
        if not iteration_ids:
            return True  # nothing to delete
        
        # Get all system_product_ids from those iterations
        system_product_ids = [
            row[0] for row in
            db.query(PricingProductIterationItem.system_product_id)
            .filter(PricingProductIterationItem.iteration_id.in_(iteration_ids))
            .distinct().all()
        ]
        
        # Delete iteration items first
        db.query(PricingProductIterationItem).filter(
            PricingProductIterationItem.iteration_id.in_(iteration_ids)
        ).delete(synchronize_session=False)
        
        # Delete iterations
        db.query(ProductIteration).filter(
            ProductIteration.iteration_id.in_(iteration_ids)
        ).delete(synchronize_session=False)
        
        # Reset pricing_product fields for affected products
        if system_product_ids:
            db.query(Product).filter(
                Product.system_product_id.in_(system_product_ids)
            ).update({
                'final_status': None,
                'dbs_status': None,
                'analyzed_date': None,
                'eps': None,
                'sample': None
            }, synchronize_session=False)
        
        db.commit()
        return True
    except Exception as e:
        db.rollback()
        print(f"Error resetting iterations: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        db.close()


def set_cluster_as_normal(skus, iteration_id, brands, category, eps, sample, group_id):
    """Mark cluster products as normal in pricing tables and update product table"""
    from repositories.pricing.product_iteration_repository import ProductIterationRepository
    from repositories.pricing.product_iteration_item_repository import PricingProductIterationItemRepository
    from models.pricing.product_iteration import ProductIteration
    from models.pricing.product import Product
    
    db = SessionLocal()
    try:
        repo = ProductRepository(db)
        iter_repo = ProductIterationRepository(db)
        item_repo = PricingProductIterationItemRepository(db)
        
        iteration = db.query(ProductIteration).filter(
            ProductIteration.iteration_id == iteration_id
        ).first()
        
        if not iteration:
            return False, "Iteration not found."
        
        # Bulk fetch system_product_ids
        system_product_ids = db.query(Product.system_product_id).filter(
            Product.qb_code.in_(skus)
        ).all()
        system_product_ids = [row[0] for row in system_product_ids]
        
        if not system_product_ids:
            return False, "No products found"
        
        # Update final_status and analyzed_date in pricing_product_iteration_item table
        item_repo.update_items_final_status(iteration_id, system_product_ids, final_status=1)
        
        # Update product table for selected group
        product_updates = []
        for sys_id in system_product_ids:
            product_updates.append({
                'system_product_id': sys_id,
                'final_status': 1,
                'eps': eps,
                'sample': sample
            })
        
        if product_updates:
            repo.update_products_with_eps_sample(product_updates, group_id)
        
        db.commit()
        return True, None
    except Exception as e:
        db.rollback()
        print(f"Error setting cluster as normal: {e}")
        return False, str(e)
    finally:
        db.close()


def set_cluster_as_outlier(skus, iteration_id, brands, category, eps, sample, group_id):
    """Mark cluster products as outliers in pricing tables"""
    from repositories.pricing.product_iteration_repository import ProductIterationRepository
    from repositories.pricing.product_iteration_item_repository import PricingProductIterationItemRepository
    from models.pricing.product_iteration import ProductIteration
    from models.pricing.product import Product
    
    db = SessionLocal()
    try:
        repo = ProductRepository(db)
        iter_repo = ProductIterationRepository(db)
        item_repo = PricingProductIterationItemRepository(db)
        
        # Check if iteration exists
        iteration = db.query(ProductIteration).filter(
            ProductIteration.iteration_id == iteration_id
        ).first()
        
        if not iteration:
            return False, "Iteration not found. Please save the iteration first."
        
        # Bulk fetch system_product_ids
        system_product_ids = db.query(Product.system_product_id).filter(
            Product.qb_code.in_(skus)
        ).all()
        system_product_ids = [row[0] for row in system_product_ids]
        
        if not system_product_ids:
            return False, "No products found"
        
        # Update final_status and analyzed_date in pricing_product_iteration_item table
        item_repo.update_items_final_status(iteration_id, system_product_ids, final_status=0)
        
        # Update product table for selected group
        product_updates = []
        for sys_id in system_product_ids:
            product_updates.append({
                'system_product_id': sys_id,
                'final_status': 0,
                'eps': eps,
                'sample': sample
            })
        
        if product_updates:
            repo.update_products_with_eps_sample(product_updates, group_id)
        
        db.commit()
        return True, None
    except Exception as e:
        db.rollback()
        print(f"Error setting cluster as outlier: {e}")
        return False, str(e)
    finally:
        db.close()


def remove_cluster_outlier(skus, iteration_id, brands, category, group_id):
    """Remove outlier status from cluster products in pricing tables"""
    from repositories.pricing.product_iteration_repository import ProductIterationRepository
    from repositories.pricing.product_iteration_item_repository import PricingProductIterationItemRepository
    from models.pricing.product_iteration import ProductIteration
    from models.pricing.product import Product
    
    db = SessionLocal()
    try:
        repo = ProductRepository(db)
        iter_repo = ProductIterationRepository(db)
        item_repo = PricingProductIterationItemRepository(db)
        
        iteration = db.query(ProductIteration).filter(
            ProductIteration.iteration_id == iteration_id
        ).first()
        
        if not iteration:
            return False, "Iteration not found."
        
        # Bulk fetch system_product_ids
        system_product_ids = db.query(Product.system_product_id).filter(
            Product.qb_code.in_(skus)
        ).all()
        system_product_ids = [row[0] for row in system_product_ids]
        
        if not system_product_ids:
            return False, "No products found"
        
        # Update final_status to NULL in iteration_item table
        item_repo.update_items_final_status(iteration_id, system_product_ids, final_status=None)
        
        # Update product table for selected group
        product_updates = []
        for sys_id in system_product_ids:
            product_updates.append({
                'system_product_id': sys_id,
                'final_status': None,
                'eps': iteration.eps,
                'sample': iteration.sample
            })
        
        if product_updates:
            repo.update_products_with_eps_sample(product_updates, group_id)
        
        db.commit()
        return True, None
    except Exception as e:
        db.rollback()
        print(f"Error removing cluster outlier: {e}")
        return False, str(e)
    finally:
        db.close()


def update_item_status(sku, final_status, iteration_id, group_id, category, eps, sample):
    """Update final_status for a specific iteration item"""
    from repositories.pricing.product_iteration_item_repository import PricingProductIterationItemRepository
    from models.pricing.product import Product
    from datetime import datetime
    
    db = SessionLocal()
    try:
        repo = ProductRepository(db)
        item_repo = PricingProductIterationItemRepository(db)
        
        # Get system_product_id from qb_code
        product = db.query(Product).filter(Product.qb_code == sku).first()
        if not product:
            return False, "Product not found"
        
        system_product_id = product.system_product_id
        
        # Update pricing_product_iteration_item table
        item_repo.update_items_final_status(
            iteration_id, 
            [system_product_id], 
            final_status=final_status
        )
        
        # Update pricing_product table
        if final_status is None:
            # Reset to null
            product_update = {
                'system_product_id': system_product_id,
                'final_status': None,
                'dbs_status': None,
                'analyzed_date': None,
                'eps': None,
                'sample': None
            }
        else:
            # Set to specific status
            product_update = {
                'system_product_id': system_product_id,
                'final_status': final_status,
                'dbs_status': final_status,
                'analyzed_date': datetime.now(),
                'eps': eps,
                'sample': sample
            }
        
        repo.update_products_with_eps_sample([product_update], group_id)
        
        db.commit()
        return True, None
    except Exception as e:
        db.rollback()
        print(f"Error updating item status: {e}")
        return False, str(e)
    finally:
        db.close()


def load_saved_iteration(iteration_id):
    """Load saved iteration filters and complete data for display - optimized"""
    from models.pricing.product_iteration import ProductIteration
    from models.pricing.product_iteration_item import PricingProductIterationItem
    from models.pricing.product import Product
    from sqlalchemy import text
    
    db = SessionLocal()
    try:
        # Single query to get iteration with basic data
        iteration = db.query(ProductIteration).filter(
            ProductIteration.iteration_id == iteration_id
        ).first()
        
        if not iteration:
            return {"ok": False, "message": "Iteration not found"}
        
        product_types = iteration.product_type.split('|') if iteration.product_type else []

        # Resolve stored category IDs back to category strings for frontend
        stored_category = iteration.category or ''
        stored_parts = [p.strip() for p in stored_category.split(',') if p.strip()]
        if stored_parts and all(p.isdigit() for p in stored_parts):
            try:
                from repositories.pricing.product_repository import ProductRepository
                prod_repo = ProductRepository(db)
                cat_df = prod_repo.get_categories_for_group(iteration.product_group_id)
                id_to_cat = {str(row['category_id']): row['category'] for _, row in cat_df.iterrows()} if not cat_df.empty else {}
                resolved_categories = [id_to_cat[p] for p in stored_parts if p in id_to_cat]
            except Exception:
                resolved_categories = []
        else:
            resolved_categories = stored_parts

        # Resolve stored brand IDs back to brand strings for frontend
        stored_brand = iteration.brand or ''
        stored_brand_parts = [p.strip() for p in stored_brand.split(',') if p.strip()]
        if stored_brand_parts and all(p.isdigit() for p in stored_brand_parts):
            try:
                from repositories.pricing.product_repository import ProductRepository
                prod_repo = ProductRepository(db)
                brand_df = prod_repo.get_brands_for_group(iteration.product_group_id)
                id_to_brand = {str(row['brand_id']): row['brand'] for _, row in brand_df.iterrows()} if not brand_df.empty else {}
                resolved_brands = [id_to_brand[p] for p in stored_brand_parts if p in id_to_brand]
                resolved_brand_ids = [int(p) for p in stored_brand_parts]
            except Exception:
                resolved_brands = []
                resolved_brand_ids = []
        else:
            resolved_brands = stored_brand_parts
            resolved_brand_ids = []
        
        # Resolve axis column codes from stored IDs
        from repositories.pricing.product_column_repository import ProductColumnRepository
        col_repo = ProductColumnRepository(db)
        all_cols = col_repo.get_all()
        col_id_map = {c.column_id: c for c in all_cols}

        def _col_code(col_id):
            c = col_id_map.get(col_id)
            return c.code if c else None

        axis_x_code = _col_code(iteration.x_axis)
        axis_y_code = _col_code(iteration.y_axis)
        axis_z_code = _col_code(iteration.z_axis)
        com_x_code = _col_code(iteration.x_axis_com)
        com_y_code = _col_code(iteration.y_axis_com)
        com_z_code = _col_code(iteration.z_axis_com)

        # Build dynamic SELECT for axis + com columns (deduplicated)
        extra_cols = list(dict.fromkeys(c for c in [
            axis_x_code, axis_y_code, axis_z_code,
            com_x_code, com_y_code, com_z_code
        ] if c))
        extra_select = (', ' + ', '.join(f'p.{c}' for c in extra_cols)) if extra_cols else ''

        # Optimized single query to get all required data
        query = text(f"""
            SELECT 
                ppii.system_product_id,
                ppii.status,
                ppii.final_status,
                ppii.outlier_mode,
                ppii.cluster,
                ppii.analyzed_date,
                p.qb_code,
                p.brand,
                p.category,
                p.product_type,
                p.name,
                p.price,
                p.base_image_url,
                p.product_url{extra_select}
            FROM pricing_product_iteration_item ppii
            INNER JOIN pricing_product p ON ppii.system_product_id = p.system_product_id
            WHERE ppii.iteration_id = :iteration_id
        """)
        
        items = db.execute(query, {'iteration_id': iteration_id}).fetchall()

        # Cluster totals across entire iteration
        cluster_totals_query = text("""
            SELECT cluster, COUNT(*) as cnt
            FROM pricing_product_iteration_item
            WHERE iteration_id = :iteration_id AND cluster IS NOT NULL
            GROUP BY cluster
        """)
        cluster_totals_rows = db.execute(cluster_totals_query, {'iteration_id': iteration_id}).fetchall()
        cluster_totals = {}
        for row in cluster_totals_rows:
            cluster_totals[str(row.cluster)] = int(row.cnt)

        data = []
        normals = 0
        outliers = 0
        
        # Column index offset: first 14 fixed cols (0-13), then extra_cols start at 14
        extra_col_offset = 14

        for item in items:
            if item.cluster is None:
                cluster_num = None
            elif str(item.cluster) == 'Noise/Outlier':
                cluster_num = -1
            else:
                try:
                    cluster_num = int(str(item.cluster).replace('Cluster ', ''))
                except:
                    cluster_num = None
            
            is_outlier = (item.final_status == 0) or (item.final_status is None and item.status == 0)
            
            if item.final_status == 0:
                outliers += 1
            elif item.final_status == 1:
                normals += 1
            
            row = {
                'SKU': item[6],
                'Brand': item[7],
                'Category': item[8],
                'Type': item[9],
                'Name': item[10],
                'price': float(item[11]) if item[11] else None,
                'imageUrl': item[12],
                'url_key': item[13],
                'system_product_id': item[0],
                'is_outlier_combined': is_outlier,
                'outlier_mode': item[3],
                'final_status': item[2],
                'dbscan_cluster': cluster_num,
                'analyzed_date': item[5].isoformat() if item[5] else None
            }
            # Add dynamic axis + com column values
            for i, col in enumerate(extra_cols):
                raw = item[extra_col_offset + i]
                row[col] = float(raw) if raw is not None else None
            data.append(row)

        total = len(data)
        return {
            "ok": True,
            "iteration_data": data,
            "total": total,
            "normals": normals,
            "outliers": outliers,
            "cluster_totals": cluster_totals,
            "filters": {
                "group_id": iteration.product_group_id,
                "brand": iteration.brand,
                "brands": resolved_brands,
                "brand_ids": resolved_brand_ids,
                "category": iteration.category,
                "categories": resolved_categories,
                "product_types": product_types,
                "eps": float(iteration.eps) if iteration.eps else None,
                "sample": int(iteration.sample) if iteration.sample else None,
                "algorithm": iteration.algorithm,
                "x_axis": iteration.x_axis,
                "y_axis": iteration.y_axis,
                "z_axis": iteration.z_axis,
                "x_axis_com": iteration.x_axis_com,
                "y_axis_com": iteration.y_axis_com,
                "z_axis_com": iteration.z_axis_com
            }
        }
    except Exception as e:
        print(f"Error loading iteration: {e}")
        return {"ok": False, "message": str(e)}
    finally:
        db.close()


def delete_iteration(iteration_id):
    """Delete iteration and its items"""
    from repositories.pricing.product_iteration_repository import ProductIterationRepository
    from models.pricing.product_iteration import ProductIteration
    from models.pricing.product_iteration_item import PricingProductIterationItem
    
    db = SessionLocal()
    try:
        # Delete iteration items
        db.query(PricingProductIterationItem).filter(
            PricingProductIterationItem.iteration_id == iteration_id
        ).delete(synchronize_session=False)
        
        # Delete iteration
        db.query(ProductIteration).filter(
            ProductIteration.iteration_id == iteration_id
        ).delete(synchronize_session=False)
        
        db.commit()
        return True, "Iteration deleted successfully"
    except Exception as e:
        db.rollback()
        print(f"Error deleting iteration: {e}")
        return False, str(e)
    finally:
        db.close()


def get_all_previous_outliers(group_id, brands, category, types, current_iteration, algorithms):
    """Get all outliers from previous iterations with analysis data for selected algorithms only"""
    db = SessionLocal()
    try:
        repo = ProductRepository(db)
        df = repo.get_previous_outliers(group_id, brands, category, types, current_iteration)
        if df.empty:
            return []
        
        # Rename display columns only (keep mfr_cost/shipping_cost/profit_margin as-is)
        df = df.rename(columns={
            'qb_code': 'SKU',
            'brand': 'Brand',
            'category': 'Category',
            'product_type': 'Type',
            'name': 'Name',
            'base_image_url': 'imageUrl',
            'product_url': 'url_key'
        })
        
        # Process only selected algorithms
        for idx, row in df.iterrows():
            # DBSCAN status
            if 'DBSCAN' in algorithms:
                if pd.notna(row.get('dbs_status')):
                    df.at[idx, 'dbscan_is_outlier'] = (row['dbs_status'] == 0)
                    df.at[idx, 'dbscan_cluster'] = -1 if row['dbs_status'] == 0 else 0
                else:
                    df.at[idx, 'dbscan_is_outlier'] = True
                    df.at[idx, 'dbscan_cluster'] = -1
        
        # Replace NaN with None for JSON serialization
        df = df.replace({pd.NA: None, np.nan: None})
        return df.to_dict('records')
    finally:
        db.close()


def analyze_products(group_id, brands, category, types, algorithms, h_mult, w_mult, d_mult, dbscan_eps, dbscan_min_samples, algorithm_settings=None, iteration=1, analysis_mode='all', axis_cols=None):
    """Main analysis function with iteration support"""
    db = SessionLocal()
    try:
        col_x, col_y, col_z = (axis_cols or ['mfr_cost', 'shipping_cost', 'profit_margin'])
        # Determine which products to load based on iteration
        if iteration == 1:
            df = load_products_filtered(group_id, brands, category, types, iteration=1, for_save=False, for_display=False, axis_cols=[col_x, col_y, col_z])
        else:
            # Subsequent iterations: load from product_iteration table
            iteration_repo = ProductIterationRepository(db)
            products = iteration_repo.get_products_for_iteration(brands, category, iteration, analysis_mode)
            
            if not products:
                return None, "No products found for this iteration"
            
            # Convert to DataFrame
            product_data = []
            for p in products:
                row = {
                    'product_id': p.product_id,
                    'SKU': p.qb_code,
                    'Brand': p.brand,
                    'Category': p.category,
                    'Type': p.product_type,
                    'Name': p.name,
                    'price': float(p.price) if p.price else None,
                    'imageUrl': p.base_image_url,
                    'url_key': p.product_url,
                    'system_product_id': p.system_product_id,
                    'outlier_mode': p.outlier_mode or 0,
                    'final_status': p.final_status
                }
                # Add all axis columns dynamically
                for col in [col_x, col_y, col_z]:
                    row[col] = float(getattr(p, col)) if getattr(p, col, None) is not None else None
                product_data.append(row)
            
            df = pd.DataFrame(product_data)
            drop_cols = [c for c in [col_x, col_y, col_z] if c in df.columns]
            if drop_cols:
                df = df.dropna(subset=drop_cols)
        
        if df.empty or len(df) < 4:
            return None, "Seems less than 4 products available."
        
        df_combined = df.copy()
        df_combined['is_outlier_combined'] = False
        
        # DBSCAN Analysis
        if 'DBSCAN' in algorithms:
            is_outlier_dbscan, df_dbscan = detect_outliers_dbscan(df_combined.copy(), eps=dbscan_eps, min_samples=dbscan_min_samples, algorithm_settings=algorithm_settings, axis_cols=[col_x, col_y, col_z])
            df_combined['is_outlier_combined'] = is_outlier_dbscan
            df_combined['dbscan_cluster'] = df_dbscan['dbscan_cluster']
            df_combined['dbscan_is_outlier'] = df_dbscan['dbscan_is_outlier']

        # Calculate statistics
        total = len(df_combined)
        outliers = df_combined['is_outlier_combined'].sum()
        normals = total - outliers
        
        df_combined = df_combined.replace({pd.NA: None, np.nan: None})
        records = df_combined.to_dict('records')
        for record in records:
            if 'is_outlier_combined' in record:
                record['is_outlier_combined'] = bool(record['is_outlier_combined'])
        
        return {
            'data': records,
            'total': total,
            'outliers': int(outliers),
            'normals': int(normals),
            'outlier_pct': round((outliers / total * 100), 2) if total > 0 else 0,
            'iteration': iteration
        }, None
    finally:
        db.close()


def update_products_final_status(skus, final_status, iteration=None, brands=None, category=None):
    """Update final_status for multiple products by SKU in product_iteration table"""
    db = SessionLocal()
    try:
        iteration_repo = ProductIterationRepository(db)
        product_repo = ProductRepository(db)
        
        # Check if iteration is saved
        if iteration and brands is not None and category:
            is_saved = iteration_repo.is_iteration_saved(brands, category, iteration)
            if not is_saved:
                return False, "Please save current iteration first."
        
        # Get system_product_ids for the SKUs
        system_product_ids = []
        for sku in skus:
            product = product_repo.get_by_qb_code(sku)
            if product:
                system_product_ids.append(product.system_product_id)
        
        if not system_product_ids:
            return False, "No products found"
        
        # Update in product_iteration table
        if iteration and brands is not None and category:
            iteration_repo.update_cluster_outliers_in_iteration(
                system_product_ids, iteration, brands, category
            )
            
            # Update product table with aggregated results
            update_product_table_aggregated(brands, category)
        
        return True, None
    except Exception as e:
        print(f"Error updating products: {e}")
        return False, str(e)
    finally:
        db.close()


def save_iteration_to_db(analysis_result, algorithms, dbscan_eps, dbscan_min_samples, iteration_number, brands, category):
    """Save analysis results to product_iteration table and update product table with aggregated results"""
    db = SessionLocal()
    try:
        print(f"Saving iteration {iteration_number} results to DB...")
        iteration_repo = ProductIterationRepository(db)
        
        iteration_data_list = []
        
        # Convert brands list to comma-separated string
        brand_str = ', '.join(brands) if brands and len(brands) > 0 else None
        
        for product in analysis_result['data']:
            system_product_id = product.get('system_product_id')
            if not system_product_id:
                continue
            
            is_outlier = product.get('is_outlier_combined', False)
            status = 0 if is_outlier else 1
            outlier_mode = product.get('outlier_mode', 0)
            
            # Get cluster info
            cluster = None
            if 'dbscan_cluster' in product and product['dbscan_cluster'] is not None:
                cluster_num = product['dbscan_cluster']
                if cluster_num == -1:
                    cluster = "Noise/Outlier"
                else:
                    cluster = f"Cluster {cluster_num}"
            
            # Save for each algorithm
            for algo in algorithms:
                iteration_data = {
                    'system_product_id': system_product_id,
                    'iteration_number': iteration_number,
                    'algo_id': algo,
                    'brand': brand_str,
                    'category': category,
                    'eps': dbscan_eps if algo == ALGO_DBSCAN else None,
                    'sample': dbscan_min_samples if algo == ALGO_DBSCAN else None,
                    'cluster': cluster if algo == ALGO_DBSCAN else None,
                    'outlier_mode': outlier_mode,
                    'status': status
                }
                iteration_data_list.append(iteration_data)
        
        # Save to product_iteration table
        iteration_repo.save_iteration_results(iteration_data_list)
        
        # Update product table with aggregated results
        update_product_table_aggregated(brands, category)
        
        return True
    except Exception as e:
        print(f"Error saving iteration to DB: {e}")
        return False
    finally:
        db.close()


def update_product_table_aggregated(brands, category):
    """Update product table with aggregated results from all iterations including outlier_mode"""
    db = SessionLocal()
    try:
        print(f"Updating product table with aggregated results for category: {category}")
        iteration_repo = ProductIterationRepository(db)
        product_repo = ProductRepository(db)
        
        # Fetch all iteration data for the given brand and category
        iteration_data = iteration_repo.get_iterations_by_brand_category(brands, category)
        
        if not iteration_data:
            print("No iteration data found")
            return
        
        # Group by system_product_id and algorithm
        product_aggregates = {}
        
        for record in iteration_data:
            system_product_id = record['system_product_id']
            algo_id = record['algo_id']
            status = record['status']  # 0=Outlier, 1=Normal
            outlier_mode = record.get('outlier_mode', 0)
            
            key = (system_product_id, algo_id)
            
            if key not in product_aggregates:
                product_aggregates[key] = {
                    'system_product_id': system_product_id,
                    'algo_id': algo_id,
                    'outlier_count': 0,
                    'normal_count': 0,
                    'manual_outlier_count': 0,
                    'total_count': 0
                }
            
            product_aggregates[key]['total_count'] += 1
            if status == 0:
                product_aggregates[key]['outlier_count'] += 1
                # Track manual outliers (outlier_mode = 1)
                if outlier_mode == 1:
                    product_aggregates[key]['manual_outlier_count'] += 1
            else:
                product_aggregates[key]['normal_count'] += 1
        
        # Prepare updates for product table
        product_updates = {}
        
        for key, agg in product_aggregates.items():
            system_product_id, algo_id = key
            
            if system_product_id not in product_updates:
                product_updates[system_product_id] = {
                    'system_product_id': system_product_id
                }
            
            # Determine final status: if 50-50 or more outliers, mark as outlier
            outlier_pct = agg['outlier_count'] / agg['total_count']
            final_status = 0 if outlier_pct >= 0.5 else 1
            
            # Determine outlier_mode: if product is outlier and has any manual marking, set to 1
            if final_status == 0 and agg['manual_outlier_count'] > 0:
                product_updates[system_product_id]['outlier_mode'] = 1
            elif final_status == 1:
                product_updates[system_product_id]['outlier_mode'] = None
            else:
                product_updates[system_product_id]['outlier_mode'] = 0
            
            # Update algorithm-specific status
            if algo_id == ALGO_DBSCAN:
                product_updates[system_product_id]['dbs_status'] = final_status
        
        # Calculate final_status based on DBSCAN
        for system_product_id, update_data in product_updates.items():
            dbs_status = update_data.get('dbs_status')
            if dbs_status is not None:
                update_data['final_status'] = dbs_status
            
            # Reset outlier_mode if final status is normal
            if update_data.get('final_status') == 1:
                update_data['outlier_mode'] = None
        
        # Update product table
        product_repo.update_products_aggregated(list(product_updates.values()))
        
        print(f"Updated {len(product_updates)} products with aggregated results")
        
    except Exception as e:
        print(f"Error updating product table with aggregated results: {e}")
    finally:
        db.close()


def get_global_aggregate_data(group_id, brands, category, types, algorithms):
    """Get global aggregate data from product table"""
    db = SessionLocal()
    try:
        repo = ProductRepository(db)
        df = repo.get_global_aggregate_data(group_id, brands, category, types, algorithms)
        
        if df.empty:
            return []
        
        # Rename display columns only (keep mfr_cost/shipping_cost/profit_margin as-is)
        df = df.rename(columns={
            'qb_code': 'SKU',
            'brand': 'Brand',
            'category': 'Category',
            'product_type': 'Type',
            'name': 'Name',
            'base_image_url': 'imageUrl',
            'product_url': 'url_key'
        })
        df['is_outlier_combined'] = df['final_status'] == 0
        
        # Add algorithm-specific outlier flags
        if 'DBSCAN' in algorithms:
            df['dbscan_is_outlier'] = df['dbs_status'] == 0
            # Set cluster to -1 for outliers, 0 for normal (simplified for global view)
            df['dbscan_cluster'] = df['dbs_status'].apply(lambda x: -1 if x == 0 else 0)
        
        # Replace NaN with None for JSON serialization
        df = df.replace({pd.NA: None, np.nan: None})
        return df.to_dict('records')
    finally:
        db.close()


def analyze_multiple_combinations(group_id, brands, category, types, algorithms, h_mult, w_mult, d_mult, dbscan_eps, dbscan_min_samples, save_to_db=False):
    """Generate combinations and prepare for processing"""
    db = SessionLocal()
    try:
        repo = ProductRepository(db)
        
        # Get all basic groups based on filter hierarchy
        basic_groups = repo.get_basic_groups(group_id, brands, category, types)
        
        if not basic_groups:
            return {"ok": False, "message": "No products found"}
        
        # Apply grouping logic
        UPPER_LIMIT = 50
        LOWER_LIMIT = 10
        
        final_groups = []
        small_groups = []
        
        # Separate groups by upper limit
        for group in basic_groups:
            if group['total_count'] >= UPPER_LIMIT:
                final_groups.append({
                    'combination_key': f"{group['brand']}_{group['category']}_{group['product_type']}",
                    'brand': group['brand'],
                    'category': group['category'],
                    'product_type': group['product_type'],
                    'total_count': group['total_count'],
                    'normal_count': 0,
                    'outlier_count': 0,
                    'normal_count_percent': 0,
                    'outlier_count_percent': 0,
                    'is_valid': True,
                    'is_processed': 0
                })
            else:
                small_groups.append(group)
        
        # Combine small groups by Brand + Category
        combined_groups = {}
        for group in small_groups:
            key = f"{group['brand']}_{group['category']}"
            if key not in combined_groups:
                combined_groups[key] = {
                    'brand': group['brand'],
                    'category': group['category'],
                    'product_types': [],
                    'total_count': 0
                }
            combined_groups[key]['product_types'].append(group['product_type'])
            combined_groups[key]['total_count'] += group['total_count']
        
        # Add combined groups to final groups
        for key, group in combined_groups.items():
            is_valid = group['total_count'] >= LOWER_LIMIT
            final_groups.append({
                'combination_key': key,
                'brand': group['brand'],
                'category': group['category'],
                'product_type': '|'.join(group['product_types']) if len(group['product_types']) > 1 else group['product_types'][0],
                'total_count': group['total_count'],
                'normal_count': 0,
                'outlier_count': 0,
                'normal_count_percent': 0,
                'outlier_count_percent': 0,
                'is_valid': is_valid,
                'is_processed': 0
            })
        
        valid_groups = [g for g in final_groups if g['is_valid']]
        
        return {
            'ok': True,
            'final_groups': final_groups,
            'total_valid_groups': len(valid_groups)
        }
    finally:
        db.close()


def process_single_combination(group_id, combination, algorithms, h_mult, w_mult, d_mult, dbscan_eps, dbscan_min_samples, save_to_db=False, axis_cols=None):
    """Process a single combination"""
    db = SessionLocal()
    try:
        repo = ProductRepository(db)
        iteration_repo = ProductIterationRepository(db)
        col_x, col_y, col_z = (axis_cols or ['mfr_cost', 'shipping_cost', 'profit_margin'])
        
        # Parse product types
        product_types = combination['product_type'].split('|') if '|' in combination['product_type'] else [combination['product_type']]
        
        # Fetch products
        df = repo.load_products_filtered(group_id, [combination['brand']], combination['category'], product_types, axis_cols=[col_x, col_y, col_z])
        
        if df.empty or len(df) < 4:
            return {'ok': False, 'message': 'Seems less than 4 products available'}
        
        df = df.rename(columns={
            'qb_code': 'SKU', 'brand': 'Brand', 'category': 'Category',
            'product_type': 'Type', 'name': 'Name',
            'base_image_url': 'imageUrl', 'product_url': 'url_key'
        })
        
        # Ensure numeric axis columns
        for col in [col_x, col_y, col_z]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        drop_cols = [c for c in [col_x, col_y, col_z] if c in df.columns]
        if drop_cols:
            df = df.dropna(subset=drop_cols)
        
        if df.empty or len(df) < 4:
            return {'ok': False, 'message': 'Insufficient data after cleaning'}
        
        df_combined = df.copy()
        df_combined['is_outlier_combined'] = False
        
        if ALGO_DBSCAN in algorithms:
            is_outlier_dbscan, df_dbscan = detect_outliers_dbscan(df_combined.copy(), eps=dbscan_eps, min_samples=dbscan_min_samples, axis_cols=[col_x, col_y, col_z])
            df_combined['is_outlier_combined'] = is_outlier_dbscan
        
        total = len(df_combined)
        outliers = df_combined['is_outlier_combined'].sum()
        normals = total - outliers
        
        if save_to_db:
            iteration_data_list = []
            product_updates = []
            
            for _, row in df_combined.iterrows():
                is_outlier = row['is_outlier_combined']
                status = 0 if is_outlier else 1
                
                update = {
                    'system_product_id': row['system_product_id'],
                    'dbs_status': status if ALGO_DBSCAN in algorithms else None,
                    'final_status': status,
                    'outlier_mode': 0 if is_outlier else None
                }
                product_updates.append(update)
                
                for algo in algorithms:
                    iteration_data = {
                        'system_product_id': row['system_product_id'],
                        'iteration_number': 1,
                        'algo_id': algo,
                        'brand': combination['brand'],
                        'category': combination['category'],
                        'eps': dbscan_eps if algo == ALGO_DBSCAN else None,
                        'sample': dbscan_min_samples if algo == ALGO_DBSCAN else None,
                        'cluster': None,
                        'outlier_mode': 0,
                        'status': status
                    }
                    iteration_data_list.append(iteration_data)
            
            iteration_repo.save_iteration_results(iteration_data_list)
            repo.update_products_aggregated(product_updates)
        
        return {
            'ok': True,
            'normal_count': int(normals),
            'outlier_count': int(outliers),
            'normal_count_percent': round((normals / total * 100), 2) if total > 0 else 0,
            'outlier_count_percent': round((outliers / total * 100), 2) if total > 0 else 0
        }
    finally:
        db.close()


def process_single_combination_v2(group_id, combination, algorithms, h_mult, w_mult, d_mult, dbscan_eps, dbscan_min_samples, save_to_db=False, axis_cols=None, algorithm_settings=None):
    """Process a single combination with new dimension tables flow"""
    from repositories.pricing.product_iteration_repository import ProductIterationRepository
    from repositories.pricing.product_iteration_item_repository import PricingProductIterationItemRepository
    from models.pricing.product_iteration import ProductIteration
    
    db = SessionLocal()
    try:
        repo = ProductRepository(db)
        dim_iter_repo = ProductIterationRepository(db)
        dim_item_repo = PricingProductIterationItemRepository(db)
        col_x, col_y, col_z = (axis_cols or ['mfr_cost', 'shipping_cost', 'profit_margin'])
        
        # Parse product types
        product_types = combination['product_type'].split('|') if '|' in combination['product_type'] else [combination['product_type']]
        
        # Fetch and analyze products
        df = repo.load_products_filtered(group_id, [combination['brand']], combination['category'], product_types, axis_cols=[col_x, col_y, col_z])
        
        if df.empty or len(df) < 4:
            return {'ok': False, 'message': 'Insufficient data'}
        
        df = df.rename(columns={
            'qb_code': 'SKU', 'brand': 'Brand', 'category': 'Category',
            'product_type': 'Type', 'name': 'Name',
            'base_image_url': 'imageUrl', 'product_url': 'url_key'
        })
        
        for col in [col_x, col_y, col_z]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        drop_cols = [c for c in [col_x, col_y, col_z] if c in df.columns]
        if drop_cols:
            df = df.dropna(subset=drop_cols)
        
        if df.empty or len(df) < 4:
            return {'ok': False, 'message': 'Insufficient data after cleaning'}
        
        # Run analysis
        df_combined = df.copy()
        df_combined['is_outlier_combined'] = False
        df_combined['cluster'] = None
        
        # DBSCAN Analysis
        if ALGO_DBSCAN in algorithms:
            is_outlier_dbscan, df_dbscan = detect_outliers_dbscan(
                df_combined.copy(),
                eps=dbscan_eps,
                min_samples=dbscan_min_samples,
                algorithm_settings=algorithm_settings,
                axis_cols=[col_x, col_y, col_z]
            )
            df_combined['is_outlier_combined'] = is_outlier_dbscan
            df_combined['cluster'] = df_dbscan['dbscan_cluster'].apply(
                lambda x: f"Cluster {x}" if x != -1 else "Noise/Outlier"
            )
        
        # Calculate statistics
        total = len(df_combined)
        outliers = df_combined['is_outlier_combined'].sum()
        normals = total - outliers
        
        # Save to DB if requested
        
        if save_to_db:
            # Find and delete existing iteration with exact match
            existing_iter = dim_iter_repo.find_existing_iteration(
                combination['brand'], combination['category'], product_types,
                dbscan_eps, dbscan_min_samples, ALGO_DBSCAN, group_id
            )
            
            if existing_iter:
                dim_iter_repo.delete_iteration_with_items(existing_iter.iteration_id)
            
            # Also delete iterations with NULL product_type but matching other criteria
            null_type_iter = db.query(ProductIteration).filter(
                ProductIteration.brand == combination['brand'],
                ProductIteration.category == combination['category'],
                ProductIteration.product_type.is_(None),
                ProductIteration.eps == dbscan_eps,
                ProductIteration.sample == dbscan_min_samples,
                ProductIteration.algorithm == ALGO_DBSCAN,
                ProductIteration.product_group_id == group_id
            ).order_by(ProductIteration.timestamp.desc()).first()
            
            if null_type_iter:
                dim_iter_repo.delete_iteration_with_items(null_type_iter.iteration_id)
            
            # Save new iteration
            iteration_id = dim_iter_repo.save_iteration(
                combination['brand'], combination['category'], product_types,
                group_id, ALGO_DBSCAN, dbscan_eps, dbscan_min_samples
            )
            print(f"Saved iteration with ID: {iteration_id}")
            
            if iteration_id:
                # Prepare items data
                items_data = []
                for _, row in df_combined.iterrows():
                    is_outlier = row['is_outlier_combined']
                    items_data.append({
                        'iteration_id': iteration_id,
                        'system_product_id': row['system_product_id'],
                        'brand': row['Brand'],
                        'category': row['Category'],
                        'product_type': row['Type'],
                        'cluster': row['cluster'],
                        'outlier_mode': 0 if is_outlier else None,
                        'status': 0 if is_outlier else 1
                    })
                
                # Save items
                dim_item_repo.save_items(items_data)
                
                # Get aggregated status and update product table
                system_product_ids = df_combined['system_product_id'].tolist()
                aggregated = dim_item_repo.get_aggregated_status_by_product(system_product_ids)
                
                product_updates = []
                for sys_id, agg_data in aggregated.items():
                    product_updates.append({
                        'system_product_id': sys_id,
                        'dbs_status': agg_data['dbs_status'],
                        'final_status': agg_data['final_status'],
                        'outlier_mode': agg_data['outlier_mode']
                    })
                
                if product_updates:
                    repo.update_products_aggregated(product_updates)
                
                db.commit()
        
        return {
            'ok': True,
            'normal_count': int(normals),
            'outlier_count': int(outliers),
            'normal_count_percent': round((normals / total * 100), 2) if total > 0 else 0,
            'outlier_count_percent': round((outliers / total * 100), 2) if total > 0 else 0
        }
    except Exception as e:
        db.rollback()
        print(f"Error processing combination: {e}")
        return {'ok': False, 'message': str(e)}
    finally:
        db.close()


def analyze_and_save(group_id, brands, categories, types, algorithms, h_mult, w_mult, d_mult, dbscan_eps, dbscan_min_samples, analysis_mode, save_to_db, selected_iteration_id=None, algorithm_settings=None, axis_cols=None, axis_col_ids=None, axis_col_com_ids=None, category_ids=None, brand_ids=None):
    """Analyze products and save to dimension tables"""
    # Normalize categories to list
    if isinstance(categories, str):
        categories = [categories] if categories else []
    categories = [c for c in (categories or []) if c]
    # Store category_ids (numeric) in the iteration table; fall back to category strings
    if category_ids:
        category_str = ','.join(str(cid) for cid in sorted(category_ids))
    else:
        category_str = ','.join(sorted(categories)) if categories else None
    # Store brand_ids (numeric) in the iteration table; fall back to brand strings
    if brand_ids:
        brand_str = ','.join(str(bid) for bid in sorted(brand_ids))
    else:
        brand_str = ','.join(sorted(brands)) if brands else None
    # For single-category queries keep backward compat
    category = categories[0] if len(categories) == 1 else None
    from repositories.pricing.product_iteration_repository import ProductIterationRepository
    from repositories.pricing.product_iteration_item_repository import PricingProductIterationItemRepository
    from models.pricing.product_iteration_item import PricingProductIterationItem
    from models.pricing.product import Product
    from models.pricing.product_iteration import ProductIteration
    import time
    import uuid
    
    db = SessionLocal()
    try:
        repo = ProductRepository(db)
        iter_repo = ProductIterationRepository(db)
        item_repo = PricingProductIterationItemRepository(db)
        
        brand = brands[0] if brands and len(brands) > 0 else None
        product_types = types if types and len(types) > 0 else None
        algorithm = algorithms[0] if algorithms and len(algorithms) > 0 else ALGO_DBSCAN
        
        # Load products based on analysis mode
        # Determine axis columns for DBSCAN (default fallback)
        col_x, col_y, col_z = (axis_cols or ['mfr_cost', 'shipping_cost', 'profit_margin'])

        # Resolve com column codes from axis_col_com_ids
        com_col_codes = [None, None, None]
        if axis_col_com_ids and len(axis_col_com_ids) == 3:
            from repositories.pricing.product_column_repository import ProductColumnRepository
            col_repo = ProductColumnRepository(db)
            all_cols = col_repo.get_all()
            col_id_map = {c.column_id: c.code for c in all_cols}
            com_col_codes = [col_id_map.get(cid) for cid in axis_col_com_ids]

        if analysis_mode == 'all':
            # Always use category strings for DB product queries
            category_filter = ','.join(categories) if len(categories) > 1 else (categories[0] if categories else None)
            df = repo.load_products_filtered(group_id, brands, category_filter, types, axis_cols=[col_x, col_y, col_z])
        elif analysis_mode == 'pending' and selected_iteration_id:
            # Get products from selected iteration where final_status is null
            system_product_ids = item_repo.get_system_product_ids_by_final_status(selected_iteration_id, None)
            
            if not system_product_ids:
                return {'ok': False, 'message': 'No pending products found in selected iteration'}
            
            df = repo.load_products_by_ids(system_product_ids, axis_cols=[col_x, col_y, col_z])
        else:
            return {'ok': False, 'message': 'Invalid analysis mode or missing iteration'}
        
        if df.empty or len(df) < 4:
            return {'ok': False, 'message': 'Insufficient data'}
        
        # Rename display columns only (keep mfr_cost/shipping_cost/profit_margin for DBSCAN)
        df = df.rename(columns={
            'qb_code': 'SKU', 'brand': 'Brand', 'category': 'Category',
            'product_type': 'Type', 'name': 'Name',
            'base_image_url': 'imageUrl', 'product_url': 'url_key'
        })
        
        df = df.dropna(subset=[col_x, col_y, col_z])
        
        if df.empty or len(df) < 4:
            return {'ok': False, 'message': 'Insufficient data after cleaning'}
        
        # Fetch com column values and merge into df
        if any(com_col_codes):
            spids = df['system_product_id'].tolist()
            com_vals = fetch_com_col_values(db, spids, com_col_codes)
            for code in com_col_codes:
                if code:
                    df[code] = df['system_product_id'].map(lambda spid, c=code: com_vals.get(spid, {}).get(c))
                    df[code] = pd.to_numeric(df[code], errors='coerce')
        
        # Run analysis
        df_combined = df.copy()
        df_combined['is_outlier_combined'] = False
        df_combined['cluster'] = None

        if ALGO_DBSCAN in algorithms:
            is_outlier_dbscan, df_dbscan = detect_outliers_dbscan(
                df_combined.copy(),
                eps=dbscan_eps,
                min_samples=dbscan_min_samples,
                algorithm_settings=algorithm_settings,
                axis_cols=[col_x, col_y, col_z]
            )
            df_combined['is_outlier_combined'] = is_outlier_dbscan
            df_combined['dbscan_cluster'] = df_dbscan['dbscan_cluster']
            df_combined['cluster'] = df_dbscan['dbscan_cluster'].apply(
                lambda x: f"Cluster {x}" if x != -1 else "Noise/Outlier"
            )
        # Calculate statistics
        total = len(df_combined)
        outliers = df_combined['is_outlier_combined'].sum()
        normals = total - outliers
        
        # Save to DB based on analysis mode
        if save_to_db:
            # Get counts from pricing_product table using dynamic axis columns
            def _axis_filter(q):
                from sqlalchemy import text as _text
                for c in [col_x, col_y, col_z]:
                    q = q.filter(_text(f"{c} IS NOT NULL"))
                return q

            if analysis_mode == 'all':
                # For 'all' mode: Create NEW iteration with all products
                unique_number = f"{int(time.time() * 1000)}{uuid.uuid4().hex[:8]}"

                total_items_query = _axis_filter(db.query(Product).filter(Product.group_id == group_id))
                if brands and len(brands) > 0:
                    total_items_query = total_items_query.filter(Product.brand.in_(brands))
                if category:
                    total_items_query = total_items_query.filter(Product.category == category)
                if types and len(types) > 0:
                    total_items_query = total_items_query.filter(Product.product_type.in_(types))
                total_items_count = total_items_query.count()
                
                analyzed_items_query = _axis_filter(db.query(Product).filter(
                    Product.group_id == group_id, Product.final_status.isnot(None)))
                if brands and len(brands) > 0:
                    analyzed_items_query = analyzed_items_query.filter(Product.brand.in_(brands))
                if category:
                    analyzed_items_query = analyzed_items_query.filter(Product.category == category)
                if types and len(types) > 0:
                    analyzed_items_query = analyzed_items_query.filter(Product.product_type.in_(types))
                analyzed_items_count = analyzed_items_query.count()
                
                pending_items_query = _axis_filter(db.query(Product).filter(
                    Product.group_id == group_id, Product.final_status.is_(None)))
                if brands and len(brands) > 0:
                    pending_items_query = pending_items_query.filter(Product.brand.in_(brands))
                if category:
                    pending_items_query = pending_items_query.filter(Product.category == category)
                if types and len(types) > 0:
                    pending_items_query = pending_items_query.filter(Product.product_type.in_(types))
                pending_items_count = pending_items_query.count()
                
                # Outlier items: from current analysis
                outlier_items = int(df_combined['is_outlier_combined'].sum())
                
                # Save new iteration
                iteration_id = iter_repo.save_iteration(
                     brand_str, category_str, None,
                    group_id, algorithm, dbscan_eps, dbscan_min_samples,
                    unique_number=unique_number,
                    total_items=total_items_count,
                    analyzed_items=analyzed_items_count,
                    pending_items=pending_items_count,
                    outlier_items=outlier_items,
                    x_axis=axis_col_ids[0] if axis_col_ids and len(axis_col_ids) == 3 else None,
                    y_axis=axis_col_ids[1] if axis_col_ids and len(axis_col_ids) == 3 else None,
                    z_axis=axis_col_ids[2] if axis_col_ids and len(axis_col_ids) == 3 else None,
                    x_axis_com=axis_col_com_ids[0] if axis_col_com_ids and len(axis_col_com_ids) == 3 else None,
                    y_axis_com=axis_col_com_ids[1] if axis_col_com_ids and len(axis_col_com_ids) == 3 else None,
                    z_axis_com=axis_col_com_ids[2] if axis_col_com_ids and len(axis_col_com_ids) == 3 else None
                )
                
                if iteration_id:
                    # Calculate cluster statistics
                    cluster_stats = {}
                    total_items_in_iteration = len(df_combined)
                    for _, row in df_combined.iterrows():
                        cluster = row['cluster']
                        if cluster not in cluster_stats:
                            cluster_stats[cluster] = 0
                        cluster_stats[cluster] += 1
                    
                    # Prepare items data
                    items_data = []
                    for _, row in df_combined.iterrows():
                        is_outlier = row['is_outlier_combined']
                        cluster = row['cluster']
                        cluster_items = cluster_stats.get(cluster, 0)
                        cluster_items_per = (cluster_items / total_items_in_iteration * 100) if total_items_in_iteration > 0 else 0
                        
                        items_data.append({
                            'iteration_id': iteration_id,
                            'system_product_id': row['system_product_id'],
                            'brand': row['Brand'],
                            'category': row['Category'],
                            'product_type': row['Type'],
                            'cluster': cluster,
                            'cluster_items': cluster_items,
                            'cluster_items_per': cluster_items_per,
                            'outlier_mode': 0 if is_outlier else None,
                            'status': 0 if is_outlier else 1
                        })
                    
                    item_repo.save_items(items_data)
                    db.commit()
                    
                    return_data = {
                        'iteration_id': iteration_id,
                        'unique_number': unique_number
                    }
            elif analysis_mode == 'pending':
                # For 'pending' mode: Create NEW iteration with only pending products
                unique_number = f"{int(time.time() * 1000)}{uuid.uuid4().hex[:8]}"
                
                total_items_query = _axis_filter(db.query(Product).filter(Product.group_id == group_id))
                if brands and len(brands) > 0:
                    total_items_query = total_items_query.filter(Product.brand.in_(brands))
                if category:
                    total_items_query = total_items_query.filter(Product.category == category)
                if types and len(types) > 0:
                    total_items_query = total_items_query.filter(Product.product_type.in_(types))
                total_items_count = total_items_query.count()
                
                analyzed_items_query = _axis_filter(db.query(Product).filter(
                    Product.group_id == group_id, Product.final_status.isnot(None)))
                if brands and len(brands) > 0:
                    analyzed_items_query = analyzed_items_query.filter(Product.brand.in_(brands))
                if category:
                    analyzed_items_query = analyzed_items_query.filter(Product.category == category)
                if types and len(types) > 0:
                    analyzed_items_query = analyzed_items_query.filter(Product.product_type.in_(types))
                analyzed_items_count = analyzed_items_query.count()
                
                pending_items_query = _axis_filter(db.query(Product).filter(
                    Product.group_id == group_id, Product.final_status.is_(None)))
                if brands and len(brands) > 0:
                    pending_items_query = pending_items_query.filter(Product.brand.in_(brands))
                if category:
                    pending_items_query = pending_items_query.filter(Product.category == category)
                if types and len(types) > 0:
                    pending_items_query = pending_items_query.filter(Product.product_type.in_(types))
                pending_items_count = pending_items_query.count()
                
                # Outlier items: from current analysis
                outlier_items = int(df_combined['is_outlier_combined'].sum())
                
                # Save new iteration
                iteration_id = iter_repo.save_iteration(
                     brand_str, category_str, None,
                    group_id, algorithm, dbscan_eps, dbscan_min_samples,
                    unique_number=unique_number,
                    total_items=total_items_count,
                    analyzed_items=analyzed_items_count,
                    pending_items=pending_items_count,
                    outlier_items=outlier_items,
                    x_axis=axis_col_ids[0] if axis_col_ids and len(axis_col_ids) == 3 else None,
                    y_axis=axis_col_ids[1] if axis_col_ids and len(axis_col_ids) == 3 else None,
                    z_axis=axis_col_ids[2] if axis_col_ids and len(axis_col_ids) == 3 else None,
                    x_axis_com=axis_col_com_ids[0] if axis_col_com_ids and len(axis_col_com_ids) == 3 else None,
                    y_axis_com=axis_col_com_ids[1] if axis_col_com_ids and len(axis_col_com_ids) == 3 else None,
                    z_axis_com=axis_col_com_ids[2] if axis_col_com_ids and len(axis_col_com_ids) == 3 else None
                )
                
                if iteration_id:
                    # Calculate cluster statistics
                    cluster_stats = {}
                    total_items_in_iteration = len(df_combined)
                    for _, row in df_combined.iterrows():
                        cluster = row['cluster']
                        if cluster not in cluster_stats:
                            cluster_stats[cluster] = 0
                        cluster_stats[cluster] += 1
                    
                    # Prepare items data
                    items_data = []
                    for _, row in df_combined.iterrows():
                        is_outlier = row['is_outlier_combined']
                        cluster = row['cluster']
                        cluster_items = cluster_stats.get(cluster, 0)
                        cluster_items_per = (cluster_items / total_items_in_iteration * 100) if total_items_in_iteration > 0 else 0
                        
                        items_data.append({
                            'iteration_id': iteration_id,
                            'system_product_id': row['system_product_id'],
                            'brand': row['Brand'],
                            'category': row['Category'],
                            'product_type': row['Type'],
                            'cluster': cluster,
                            'cluster_items': cluster_items,
                            'cluster_items_per': cluster_items_per,
                            'outlier_mode': 0 if is_outlier else None,
                            'status': 0 if is_outlier else 1
                        })
                    
                    item_repo.save_items(items_data)
                    db.commit()
                    
                    return_data = {
                        'iteration_id': iteration_id,
                        'unique_number': unique_number
                    }
        else:
            return_data = {}
        
        return_data.update({
            'ok': True,
            'data': df_combined.replace({pd.NA: None, np.nan: None}).to_dict('records'),
            'total': int(total),
            'total_count': int(total),
            'normals': int(normals),
            'normal_count': int(normals),
            'outliers': int(outliers),
            'outlier_count': int(outliers),
            'manual_outlier_count': 0,
            'eps': dbscan_eps,
            'sample': dbscan_min_samples
        })
        
        return return_data
    except Exception as e:
        db.rollback()
        print(f"Error in analyze_and_save: {e}")
        return {'ok': False, 'message': str(e)}
    finally:
        db.close()



