from models.base.base import SessionLocal
from repositories.product_repository import ProductRepository
from repositories.product_group_repository import ProductGroupRepository
from repositories.dimension.product_iteration_repository import ProductIterationRepository
from constants import ALGO_IQR, ALGO_DBSCAN
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
                "analyzed_count": int(row['analyzed_count']),
                "total_count": int(row['product_count'])
            }
            for _, row in df.iterrows()
        ]
    finally:
        db.close()


def get_categories_for_group(group_id, brands=None):
    """Get categories for a group with analysis status, optionally filtered by brands"""
    db = SessionLocal()
    try:
        repo = ProductRepository(db)
        df = repo.get_categories_for_group(group_id, brands)
        if df.empty:
            return []
        return [
            {
                "label": f"{row['category']} ({row['product_count']})", 
                "value": row['category'],
                "analyzed_count": int(row['analyzed_count']),
                "total_count": int(row['product_count'])
            }
            for _, row in df.iterrows()
        ]
    finally:
        db.close()


def get_types_for_group(group_id, brands=None, category=None):
    """Get product types for a group with analysis status"""
    db = SessionLocal()
    try:
        repo = ProductRepository(db)
        df = repo.get_types_for_group(group_id, brands, category)
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


def load_products_filtered(group_id, brands=None, category=None, types=None, iteration=1, for_save=False, for_display=False):
    """Load product data from database with filters for specific iteration"""
    db = SessionLocal()
    try:
        repo = ProductRepository(db)
        if iteration > 1 and not for_save:
            df = repo.load_products_for_iteration(group_id, iteration, brands, category, types, for_display=for_display)
        else:
            df = repo.load_products_filtered(group_id, brands, category, types)
        
        if not df.empty:
            # Rename columns
            df = df.rename(columns={
                'qb_code': 'SKU',
                'brand': 'Brand',
                'category': 'Category',
                'product_type': 'Type',
                'name': 'Name',
                'height': 'H',
                'width': 'W',
                'depth': 'D',
                'base_image_url': 'imageUrl',
                'product_url': 'url_key'
            })
            
            # Keep system_product_id and product_id as is (don't rename)
            
            # Ensure numeric columns
            for col in ['H', 'W', 'D']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            if 'weight' in df.columns:
                df['weight'] = pd.to_numeric(df['weight'], errors='coerce')
            
            # Remove rows with missing dimension data
            df = df.dropna(subset=['H', 'W', 'D'])
        
        return df
    finally:
        db.close()


def calculate_iqr_bounds(data_subset, dimensions=['H', 'W', 'D'], multipliers={'H': 1.5, 'W': 1.5, 'D': 1.5}):
    """Calculate IQR bounds with configurable multipliers"""
    iqr_stats = {}
    
    for dim in dimensions:
        Q1 = data_subset[dim].quantile(0.25)
        Q3 = data_subset[dim].quantile(0.75)
        IQR = Q3 - Q1
        
        multiplier = multipliers.get(dim, 1.5)
        
        lower_bound = Q1 - (multiplier * IQR)
        upper_bound = Q3 + (multiplier * IQR)

        # Clip bounds to actual min/max of data
        min_val = data_subset[dim].min()
        max_val = data_subset[dim].max()
        
        if lower_bound < min_val:
            lower_bound = min_val
        if upper_bound > max_val:
            upper_bound = max_val
        
        iqr_stats[dim] = {
            'Q1': Q1,
            'Q3': Q3,
            'IQR': IQR,
            'lower_bound': lower_bound,
            'upper_bound': upper_bound,
            'multiplier': multiplier
        }
    
    return iqr_stats


def detect_outliers_iqr(df_subset, iqr_stats):
    """Detect outliers based on IQR bounds"""
    is_outlier = pd.Series([False] * len(df_subset), index=df_subset.index)
    
    for dim in ['H', 'W', 'D']:
        lower = iqr_stats[dim]['lower_bound']
        upper = iqr_stats[dim]['upper_bound']
        is_outlier |= (df_subset[dim] < lower) | (df_subset[dim] > upper)
    
    return is_outlier


def calculate_dynamic_iqr(filtered_df, multipliers={'H': 1.5, 'W': 1.5, 'D': 1.5}):
    """Calculate IQR dynamically based on filtered data"""
    df_enriched = filtered_df.copy()
    
    # Initialize IQR columns
    for dim in ['H', 'W', 'D']:
        df_enriched[f'{dim}_IQR'] = 0.0
        df_enriched[f'{dim}_Q1'] = 0.0
        df_enriched[f'{dim}_Q3'] = 0.0
        df_enriched[f'{dim}_lower_bound'] = 0.0
        df_enriched[f'{dim}_upper_bound'] = 0.0
        df_enriched[f'{dim}_multiplier'] = multipliers[dim]
    
    unique_types = df_enriched['Type'].unique()
    
    if len(unique_types) > 1:
        for product_type in unique_types:
            type_mask = df_enriched['Type'] == product_type
            type_data = df_enriched[type_mask]
            
            if len(type_data) >= 4:
                iqr_stats = calculate_iqr_bounds(type_data, multipliers=multipliers)
                
                for dim in ['H', 'W', 'D']:
                    df_enriched.loc[type_mask, f'{dim}_IQR'] = iqr_stats[dim]['IQR']
                    df_enriched.loc[type_mask, f'{dim}_Q1'] = iqr_stats[dim]['Q1']
                    df_enriched.loc[type_mask, f'{dim}_Q3'] = iqr_stats[dim]['Q3']
                    df_enriched.loc[type_mask, f'{dim}_lower_bound'] = iqr_stats[dim]['lower_bound']
                    df_enriched.loc[type_mask, f'{dim}_upper_bound'] = iqr_stats[dim]['upper_bound']
    else:
        if len(df_enriched) >= 4:
            iqr_stats = calculate_iqr_bounds(df_enriched, multipliers=multipliers)
            
            for dim in ['H', 'W', 'D']:
                df_enriched[f'{dim}_IQR'] = iqr_stats[dim]['IQR']
                df_enriched[f'{dim}_Q1'] = iqr_stats[dim]['Q1']
                df_enriched[f'{dim}_Q3'] = iqr_stats[dim]['Q3']
                df_enriched[f'{dim}_lower_bound'] = iqr_stats[dim]['lower_bound']
                df_enriched[f'{dim}_upper_bound'] = iqr_stats[dim]['upper_bound']
    
    return df_enriched


def detect_outliers_dbscan(filtered_df, eps=1.0, min_samples=4):
    """Detect outliers using DBSCAN"""
    df_dbscan = filtered_df.copy()
    
    X = df_dbscan[['H', 'W', 'D']].values
    
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    dbscan = DBSCAN(eps=eps, min_samples=min_samples, metric='euclidean')
    clusters = dbscan.fit_predict(X_scaled)
    
    is_outlier_dbscan = pd.Series((clusters == -1), index=df_dbscan.index)
    
    df_dbscan['dbscan_cluster'] = clusters
    df_dbscan['dbscan_is_outlier'] = is_outlier_dbscan
    
    return is_outlier_dbscan, df_dbscan



def get_iteration_history(group_id, brands, category, types):
    """Get iteration history from database"""
    from repositories.dimension.product_iteration_item_repository import DimensionProductIterationItemRepository
    
    db = SessionLocal()
    try:
        if brands and len(brands) == 1:
            item_repo = DimensionProductIterationItemRepository(db)
            return item_repo.get_iteration_summary(brands[0], category)
        return []
    finally:
        db.close()


def reset_iterations(group_id, brands, category, types):
    """Reset all iterations for a category"""
    from repositories.dimension.product_iteration_repository import ProductIterationRepository
    from repositories.product_repository import ProductRepository
    from repositories.dimension.product_iteration_item_repository import DimensionProductIterationItemRepository
    
    db = SessionLocal()
    try:
        iter_repo = ProductIterationRepository(db)
        product_repo = ProductRepository(db)
        item_repo = DimensionProductIterationItemRepository(db)
        
        # Get filter values
        brand = brands[0] if brands and len(brands) > 0 else None
        product_types = types if types and len(types) > 0 else None
        
        # Delete from dimension tables
        iter_repo.delete_by_filters(
            product_group_id=group_id,
            brand=brand,
            category=category,
            product_types=product_types,
            eps=None,
            sample=None,
            algorithm=None
        )
        
        # Get system_product_ids for selected filters
        df = product_repo.load_products_filtered(group_id, brands, category, types)
        if not df.empty:
            system_product_ids = df['system_product_id'].tolist()
            
            # Recalculate aggregated data for these products
            aggregated = item_repo.get_aggregated_status_by_product(system_product_ids)
            
            product_updates = []
            for sys_id in system_product_ids:
                if sys_id in aggregated:
                    agg_data = aggregated[sys_id]
                    product_updates.append({
                        'system_product_id': sys_id,
                        'dbs_status': agg_data['dbs_status'],
                        'final_status': agg_data['final_status'],
                        'outlier_mode': agg_data['outlier_mode']
                    })
                else:
                    # No iteration data, reset to None
                    product_updates.append({
                        'system_product_id': sys_id,
                        'dbs_status': None,
                        'iqr_status': None,
                        'final_status': None,
                        'outlier_mode': None
                    })
            
            if product_updates:
                product_repo.update_products_aggregated(product_updates)
        
        db.commit()
        return True
    except Exception as e:
        db.rollback()
        print(f"Error resetting iterations: {e}")
        return False
    finally:
        db.close()


def set_cluster_as_outlier(skus, iteration_id, brands, category):
    """Mark cluster products as outliers in dimension tables"""
    from repositories.dimension.product_iteration_repository import ProductIterationRepository
    from repositories.dimension.product_iteration_item_repository import DimensionProductIterationItemRepository
    from models.dimension.product_iteration import ProductIteration
    
    db = SessionLocal()
    try:
        repo = ProductRepository(db)
        iter_repo = ProductIterationRepository(db)
        item_repo = DimensionProductIterationItemRepository(db)
        
        # Check if iteration exists
        iteration = db.query(ProductIteration).filter(
            ProductIteration.iteration_id == iteration_id
        ).first()
        
        if not iteration:
            return False, "Iteration not found. Please save the iteration first."
        
        # Get system_product_ids from SKUs
        system_product_ids = []
        for sku in skus:
            product = repo.get_by_qb_code(sku)
            if product:
                system_product_ids.append(product.system_product_id)
        
        if not system_product_ids:
            return False, "No products found"
        
        # Update items in dimension_product_iteration_item table
        item_repo.update_items_status(iteration_id, system_product_ids, status=0, outlier_mode=1)
        
        # Recalculate aggregated status for these products
        aggregated = item_repo.get_aggregated_status_by_product(system_product_ids)
        
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
        return True, None
    except Exception as e:
        db.rollback()
        print(f"Error setting cluster as outlier: {e}")
        return False, str(e)
    finally:
        db.close()


def remove_cluster_outlier(skus, iteration_id, brands, category):
    """Remove outlier status from cluster products in dimension tables"""
    from repositories.dimension.product_iteration_repository import ProductIterationRepository
    from repositories.dimension.product_iteration_item_repository import DimensionProductIterationItemRepository
    from models.dimension.product_iteration import ProductIteration
    
    db = SessionLocal()
    try:
        repo = ProductRepository(db)
        iter_repo = ProductIterationRepository(db)
        item_repo = DimensionProductIterationItemRepository(db)
        
        iteration = db.query(ProductIteration).filter(
            ProductIteration.iteration_id == iteration_id
        ).first()
        
        if not iteration:
            return False, "Iteration not found."
        
        system_product_ids = []
        for sku in skus:
            product = repo.get_by_qb_code(sku)
            if product:
                system_product_ids.append(product.system_product_id)
        
        if not system_product_ids:
            return False, "No products found"
        
        item_repo.update_items_status(iteration_id, system_product_ids, status=1, outlier_mode=None)
        
        aggregated = item_repo.get_aggregated_status_by_product(system_product_ids)
        
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
        return True, None
    except Exception as e:
        db.rollback()
        print(f"Error removing cluster outlier: {e}")
        return False, str(e)
    finally:
        db.close()


def load_saved_iteration(iteration_id):
    """Load saved iteration filters and data with outlier_mode"""
    from models.dimension.product_iteration import ProductIteration
    from models.dimension.product_iteration_item import DimensionProductIterationItem
    from models.product import Product
    
    db = SessionLocal()
    try:
        iteration = db.query(ProductIteration).filter(
            ProductIteration.iteration_id == iteration_id
        ).first()
        
        if not iteration:
            return {"ok": False, "message": "Iteration not found"}
        
        product_types = iteration.product_type.split('|') if iteration.product_type else []
        
        # Load minimal data needed for cluster summary
        items = db.query(
            DimensionProductIterationItem.system_product_id,
            DimensionProductIterationItem.status,
            DimensionProductIterationItem.outlier_mode,
            DimensionProductIterationItem.cluster,
            Product.qb_code
        ).join(
            Product,
            DimensionProductIterationItem.system_product_id == Product.system_product_id
        ).filter(
            DimensionProductIterationItem.iteration_id == iteration_id
        ).all()
        
        data = []
        for item in items:
            cluster_num = -1
            if item.cluster:
                if 'Cluster' in item.cluster:
                    cluster_num = int(item.cluster.replace('Cluster ', ''))
            
            data.append({
                'SKU': item.qb_code,
                'is_outlier_combined': item.status == 0,
                'outlier_mode': item.outlier_mode,
                'dbscan_cluster': cluster_num
            })
        
        return {
            "ok": True,
            "iteration_data": data,
            "filters": {
                "brand": iteration.brand,
                "category": iteration.category,
                "product_types": product_types,
                "eps": float(iteration.eps) if iteration.eps else None,
                "sample": int(iteration.sample) if iteration.sample else None,
                "algorithm": iteration.algorithm
            }
        }
    except Exception as e:
        print(f"Error loading iteration: {e}")
        return {"ok": False, "message": str(e)}
    finally:
        db.close()


def delete_iteration(iteration_id):
    """Delete iteration and recalculate aggregate data"""
    from repositories.dimension.product_iteration_repository import ProductIterationRepository
    from repositories.dimension.product_iteration_item_repository import DimensionProductIterationItemRepository
    from models.dimension.product_iteration import ProductIteration
    from models.dimension.product_iteration_item import DimensionProductIterationItem
    
    db = SessionLocal()
    try:
        product_repo = ProductRepository(db)
        item_repo = DimensionProductIterationItemRepository(db)
        
        # Get system_product_ids for this iteration
        items = db.query(DimensionProductIterationItem).filter(
            DimensionProductIterationItem.iteration_id == iteration_id
        ).all()
        
        if not items:
            return False, "No items found for this iteration"
        
        system_product_ids = [item.system_product_id for item in items]
        
        # Delete iteration items
        db.query(DimensionProductIterationItem).filter(
            DimensionProductIterationItem.iteration_id == iteration_id
        ).delete(synchronize_session=False)
        
        # Delete iteration
        db.query(ProductIteration).filter(
            ProductIteration.iteration_id == iteration_id
        ).delete(synchronize_session=False)
        
        # Recalculate aggregated data
        aggregated = item_repo.get_aggregated_status_by_product(system_product_ids)
        
        product_updates = []
        for sys_id in system_product_ids:
            if sys_id in aggregated:
                agg_data = aggregated[sys_id]
                product_updates.append({
                    'system_product_id': sys_id,
                    'dbs_status': agg_data['dbs_status'],
                    'final_status': agg_data['final_status'],
                    'outlier_mode': agg_data['outlier_mode']
                })
            else:
                # No iteration data, reset to None
                product_updates.append({
                    'system_product_id': sys_id,
                    'dbs_status': None,
                    'iqr_status': None,
                    'final_status': None,
                    'outlier_mode': None
                })
        
        if product_updates:
            product_repo.update_products_aggregated(product_updates)
        
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
        
        # Rename columns to match frontend format
        df = df.rename(columns={
            'qb_code': 'SKU',
            'brand': 'Brand',
            'category': 'Category',
            'product_type': 'Type',
            'name': 'Name',
            'height': 'H',
            'width': 'W',
            'depth': 'D',
            'base_image_url': 'imageUrl',
            'product_url': 'url_key'
        })
        
        # Add is_outlier_combined flag
        df['is_outlier_combined'] = True
        
        # Process only selected algorithms
        for idx, row in df.iterrows():
            # IQR status - only if IQR is selected
            if 'IQR' in algorithms and pd.notna(row.get('iqr_status')):
                df.at[idx, 'iqr_is_outlier'] = (row['iqr_status'] == 0)
            
            # DBSCAN status - only if DBSCAN is selected
            if 'DBSCAN' in algorithms:
                if pd.notna(row.get('dbs_status')):
                    df.at[idx, 'dbscan_is_outlier'] = (row['dbs_status'] == 0)
                    df.at[idx, 'dbscan_cluster'] = -1 if row['dbs_status'] == 0 else 0
                else:
                    # If no dbs_status (manually marked), treat as outlier
                    df.at[idx, 'dbscan_is_outlier'] = True
                    df.at[idx, 'dbscan_cluster'] = -1
        
        # Calculate IQR bounds only if IQR is selected
        if 'IQR' in algorithms:
            all_products_df = load_products_filtered(group_id, brands, category, types, iteration=1, for_save=False, for_display=False)
            
            if not all_products_df.empty and len(all_products_df) >= 4:
                multipliers = {'H': 1.5, 'W': 1.5, 'D': 1.5}
                unique_types = df['Type'].unique()
                
                if len(unique_types) > 1:
                    for product_type in unique_types:
                        type_mask = df['Type'] == product_type
                        type_data = all_products_df[all_products_df['Type'] == product_type]
                        
                        if len(type_data) >= 4:
                            iqr_stats = calculate_iqr_bounds(type_data, multipliers=multipliers)
                            
                            for idx in df[type_mask].index:
                                if pd.notna(df.at[idx, 'iqr_status']):
                                    for dim in ['H', 'W', 'D']:
                                        df.at[idx, f'{dim}_lower_bound'] = iqr_stats[dim]['lower_bound']
                                        df.at[idx, f'{dim}_upper_bound'] = iqr_stats[dim]['upper_bound']
                                        df.at[idx, f'{dim}_IQR'] = iqr_stats[dim]['IQR']
                                        df.at[idx, f'{dim}_Q1'] = iqr_stats[dim]['Q1']
                                        df.at[idx, f'{dim}_Q3'] = iqr_stats[dim]['Q3']
                else:
                    if len(all_products_df) >= 4:
                        iqr_stats = calculate_iqr_bounds(all_products_df, multipliers=multipliers)
                        
                        for idx in df.index:
                            if pd.notna(df.at[idx, 'iqr_status']):
                                for dim in ['H', 'W', 'D']:
                                    df.at[idx, f'{dim}_lower_bound'] = iqr_stats[dim]['lower_bound']
                                    df.at[idx, f'{dim}_upper_bound'] = iqr_stats[dim]['upper_bound']
                                    df.at[idx, f'{dim}_IQR'] = iqr_stats[dim]['IQR']
                                    df.at[idx, f'{dim}_Q1'] = iqr_stats[dim]['Q1']
                                    df.at[idx, f'{dim}_Q3'] = iqr_stats[dim]['Q3']
        
        # Replace NaN with None for JSON serialization
        df = df.replace({pd.NA: None, np.nan: None})
        return df.to_dict('records')
    finally:
        db.close()


def analyze_products(group_id, brands, category, types, algorithms, h_mult, w_mult, d_mult, dbscan_eps, dbscan_min_samples, iteration=1, analysis_mode='all'):
    """Main analysis function with iteration support"""
    db = SessionLocal()
    try:
        # Determine which products to load based on iteration
        if iteration == 1:
            # First iteration: load all products from product table
            df = load_products_filtered(group_id, brands, category, types, iteration=1, for_save=False, for_display=False)
        else:
            # Subsequent iterations: load from product_iteration table
            iteration_repo = ProductIterationRepository(db)
            products = iteration_repo.get_products_for_iteration(brands, category, iteration, analysis_mode)
            
            if not products:
                return None, "No products found for this iteration"
            
            # Convert to DataFrame
            product_data = []
            for p in products:
                product_data.append({
                    'product_id': p.product_id,
                    'SKU': p.qb_code,
                    'Brand': p.brand,
                    'Category': p.category,
                    'Type': p.product_type,
                    'Name': p.name,
                    'H': float(p.height) if p.height else None,
                    'W': float(p.width) if p.width else None,
                    'D': float(p.depth) if p.depth else None,
                    'imageUrl': p.base_image_url,
                    'url_key': p.product_url,
                    'system_product_id': p.system_product_id,
                    'outlier_mode': p.outlier_mode or 0,
                    'final_status': p.final_status
                })
            
            df = pd.DataFrame(product_data)
            # Remove rows with missing dimension data
            df = df.dropna(subset=['H', 'W', 'D'])
        
        if df.empty or len(df) < 4:
            return None, "Insufficient data"
        
        multipliers = {'H': h_mult, 'W': w_mult, 'D': d_mult}
        df_combined = df.copy()
        df_combined['is_outlier_combined'] = False
        
        # IQR Analysis
        if 'IQR' in algorithms:
            df_iqr = calculate_dynamic_iqr(df_combined.copy(), multipliers=multipliers)
            df_iqr['iqr_is_outlier'] = False
            unique_types = df_iqr['Type'].unique()
            
            if len(unique_types) > 1:
                for product_type in unique_types:
                    type_mask = df_iqr['Type'] == product_type
                    type_data = df_iqr[type_mask]
                    
                    if len(type_data) >= 4:
                        iqr_stats = {}
                        for dim in ['H', 'W', 'D']:
                            iqr_stats[dim] = {
                                'Q1': type_data[f'{dim}_Q1'].iloc[0],
                                'Q3': type_data[f'{dim}_Q3'].iloc[0],
                                'IQR': type_data[f'{dim}_IQR'].iloc[0],
                                'lower_bound': type_data[f'{dim}_lower_bound'].iloc[0],
                                'upper_bound': type_data[f'{dim}_upper_bound'].iloc[0]
                            }
                        
                        outlier_mask = detect_outliers_iqr(type_data, iqr_stats)
                        df_iqr.loc[type_mask, 'iqr_is_outlier'] = outlier_mask
            else:
                if len(df_iqr) >= 4:
                    iqr_stats = {}
                    for dim in ['H', 'W', 'D']:
                        iqr_stats[dim] = {
                            'Q1': df_iqr[f'{dim}_Q1'].iloc[0],
                            'Q3': df_iqr[f'{dim}_Q3'].iloc[0],
                            'IQR': df_iqr[f'{dim}_IQR'].iloc[0],
                            'lower_bound': df_iqr[f'{dim}_lower_bound'].iloc[0],
                            'upper_bound': df_iqr[f'{dim}_upper_bound'].iloc[0]
                        }
                    
                    outlier_mask = detect_outliers_iqr(df_iqr, iqr_stats)
                    df_iqr['iqr_is_outlier'] = outlier_mask
            
            df_combined['is_outlier_combined'] = df_iqr['iqr_is_outlier'].values
            df_combined['iqr_is_outlier'] = df_iqr['iqr_is_outlier'].values
            
            iqr_cols = [col for col in df_iqr.columns if col.startswith('iqr_') or col.endswith('_IQR') or 
                        'Q1' in col or 'Q3' in col or 'bound' in col]
            for col in iqr_cols:
                df_combined[col] = df_iqr[col]
        
        # DBSCAN Analysis
        if 'DBSCAN' in algorithms:
            is_outlier_dbscan, df_dbscan = detect_outliers_dbscan(df_combined.copy(), eps=dbscan_eps, min_samples=dbscan_min_samples)
            
            if 'IQR' in algorithms:
                df_combined['is_outlier_combined'] = df_combined['is_outlier_combined'] & is_outlier_dbscan
            else:
                df_combined['is_outlier_combined'] = is_outlier_dbscan
            
            # Set dbscan_cluster from the newly calculated results
            df_combined['dbscan_cluster'] = df_dbscan['dbscan_cluster']
            df_combined['dbscan_is_outlier'] = df_dbscan['dbscan_is_outlier']

        # Calculate statistics
        total = len(df_combined)
        outliers = df_combined['is_outlier_combined'].sum()
        normals = total - outliers
        
        # Convert to dict and ensure boolean values are proper Python bools
        # Replace NaN with None for JSON serialization
        df_combined = df_combined.replace({pd.NA: None, np.nan: None})
        records = df_combined.to_dict('records')
        for record in records:
            # Ensure is_outlier_combined is a proper boolean
            if 'is_outlier_combined' in record:
                record['is_outlier_combined'] = bool(record['is_outlier_combined'])
            if 'iqr_is_outlier' in record:
                record['iqr_is_outlier'] = bool(record['iqr_is_outlier'])
        
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
            if algo_id == ALGO_IQR:
                product_updates[system_product_id]['iqr_status'] = final_status
            elif algo_id == ALGO_DBSCAN:
                product_updates[system_product_id]['dbs_status'] = final_status
        
        # Calculate final_status based on all algorithms
        for system_product_id, update_data in product_updates.items():
            iqr_status = update_data.get('iqr_status')
            dbs_status = update_data.get('dbs_status')
            
            # If both algorithms present, use OR logic (if either is outlier, final is outlier)
            if iqr_status is not None and dbs_status is not None:
                update_data['final_status'] = 0 if (iqr_status == 0 or dbs_status == 0) else 1
            elif iqr_status is not None:
                update_data['final_status'] = iqr_status
            elif dbs_status is not None:
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
        
        # Rename columns to match frontend format
        df = df.rename(columns={
            'qb_code': 'SKU',
            'brand': 'Brand',
            'category': 'Category',
            'product_type': 'Type',
            'name': 'Name',
            'height': 'H',
            'width': 'W',
            'depth': 'D',
            'base_image_url': 'imageUrl',
            'product_url': 'url_key'
        })
        
        # Determine is_outlier_combined based on final_status
        df['is_outlier_combined'] = df['final_status'] == 0
        
        # Add algorithm-specific outlier flags
        if 'IQR' in algorithms:
            df['iqr_is_outlier'] = df['iqr_status'] == 0
        
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


def process_single_combination(group_id, combination, algorithms, h_mult, w_mult, d_mult, dbscan_eps, dbscan_min_samples, save_to_db=False):
    """Process a single combination"""
    db = SessionLocal()
    try:
        repo = ProductRepository(db)
        iteration_repo = ProductIterationRepository(db)
        
        # Parse product types
        product_types = combination['product_type'].split('|') if '|' in combination['product_type'] else [combination['product_type']]
        
        # Fetch products
        df = repo.load_products_filtered(group_id, [combination['brand']], combination['category'], product_types)
        
        if df.empty or len(df) < 4:
            return {'ok': False, 'message': 'Insufficient data'}
        
        # Rename columns to match analysis function expectations
        df = df.rename(columns={
            'qb_code': 'SKU',
            'brand': 'Brand',
            'category': 'Category',
            'product_type': 'Type',
            'name': 'Name',
            'height': 'H',
            'width': 'W',
            'depth': 'D',
            'base_image_url': 'imageUrl',
            'product_url': 'url_key'
        })
        
        # Ensure numeric columns
        for col in ['H', 'W', 'D']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Remove rows with missing dimension data
        df = df.dropna(subset=['H', 'W', 'D'])
        
        if df.empty or len(df) < 4:
            return {'ok': False, 'message': 'Insufficient data after cleaning'}
        
        # Run analysis
        multipliers = {'H': h_mult, 'W': w_mult, 'D': d_mult}
        df_combined = df.copy()
        df_combined['is_outlier_combined'] = False
        
        # IQR Analysis
        if ALGO_IQR in algorithms:
            df_iqr = calculate_dynamic_iqr(df_combined.copy(), multipliers=multipliers)
            df_iqr['iqr_is_outlier'] = False
            unique_types = df_iqr['Type'].unique()
            
            if len(unique_types) > 1:
                for product_type in unique_types:
                    type_mask = df_iqr['Type'] == product_type
                    type_data = df_iqr[type_mask]
                    
                    if len(type_data) >= 4:
                        iqr_stats = {}
                        for dim in ['H', 'W', 'D']:
                            iqr_stats[dim] = {
                                'Q1': type_data[f'{dim}_Q1'].iloc[0],
                                'Q3': type_data[f'{dim}_Q3'].iloc[0],
                                'IQR': type_data[f'{dim}_IQR'].iloc[0],
                                'lower_bound': type_data[f'{dim}_lower_bound'].iloc[0],
                                'upper_bound': type_data[f'{dim}_upper_bound'].iloc[0]
                            }
                        outlier_mask = detect_outliers_iqr(type_data, iqr_stats)
                        df_iqr.loc[type_mask, 'iqr_is_outlier'] = outlier_mask
            else:
                if len(df_iqr) >= 4:
                    iqr_stats = {}
                    for dim in ['H', 'W', 'D']:
                        iqr_stats[dim] = {
                            'Q1': df_iqr[f'{dim}_Q1'].iloc[0],
                            'Q3': df_iqr[f'{dim}_Q3'].iloc[0],
                            'IQR': df_iqr[f'{dim}_IQR'].iloc[0],
                            'lower_bound': df_iqr[f'{dim}_lower_bound'].iloc[0],
                            'upper_bound': df_iqr[f'{dim}_upper_bound'].iloc[0]
                        }
                    outlier_mask = detect_outliers_iqr(df_iqr, iqr_stats)
                    df_iqr['iqr_is_outlier'] = outlier_mask
            
            df_combined['is_outlier_combined'] = df_iqr['iqr_is_outlier'].values
        
        # DBSCAN Analysis
        if ALGO_DBSCAN in algorithms:
            is_outlier_dbscan, df_dbscan = detect_outliers_dbscan(df_combined.copy(), eps=dbscan_eps, min_samples=dbscan_min_samples)
            
            if ALGO_IQR in algorithms:
                df_combined['is_outlier_combined'] = df_combined['is_outlier_combined'] & is_outlier_dbscan
            else:
                df_combined['is_outlier_combined'] = is_outlier_dbscan
        
        # Calculate statistics
        total = len(df_combined)
        outliers = df_combined['is_outlier_combined'].sum()
        normals = total - outliers
        
        # Only save to DB if save_to_db is True
        if save_to_db:
            # Prepare iteration data
            iteration_data_list = []
            product_updates = []
            
            for _, row in df_combined.iterrows():
                is_outlier = row['is_outlier_combined']
                status = 0 if is_outlier else 1
                
                # Prepare product update
                update = {
                    'system_product_id': row['system_product_id'],
                    'iqr_status': status if ALGO_IQR in algorithms else None,
                    'dbs_status': status if ALGO_DBSCAN in algorithms else None,
                    'final_status': status,
                    'outlier_mode': 0 if is_outlier else None
                }
                product_updates.append(update)
                
                # Prepare iteration data for each algorithm
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
            
            # Save to product_iteration table
            iteration_repo.save_iteration_results(iteration_data_list)
            
            # Update product table
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


def process_single_combination_v2(group_id, combination, algorithms, h_mult, w_mult, d_mult, dbscan_eps, dbscan_min_samples, save_to_db=False):
    """Process a single combination with new dimension tables flow"""
    from repositories.dimension.product_iteration_repository import ProductIterationRepository
    from repositories.dimension.product_iteration_item_repository import DimensionProductIterationItemRepository
    from models.dimension.product_iteration import ProductIteration
    
    db = SessionLocal()
    try:
        repo = ProductRepository(db)
        dim_iter_repo = ProductIterationRepository(db)
        dim_item_repo = DimensionProductIterationItemRepository(db)
        
        # Parse product types
        product_types = combination['product_type'].split('|') if '|' in combination['product_type'] else [combination['product_type']]
        
        # Fetch and analyze products
        df = repo.load_products_filtered(group_id, [combination['brand']], combination['category'], product_types)
        
        if df.empty or len(df) < 4:
            return {'ok': False, 'message': 'Insufficient data'}
        
        # Rename columns
        df = df.rename(columns={
            'qb_code': 'SKU', 'brand': 'Brand', 'category': 'Category',
            'product_type': 'Type', 'name': 'Name', 'height': 'H',
            'width': 'W', 'depth': 'D', 'base_image_url': 'imageUrl',
            'product_url': 'url_key'
        })
        
        for col in ['H', 'W', 'D']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        df = df.dropna(subset=['H', 'W', 'D'])
        
        if df.empty or len(df) < 4:
            return {'ok': False, 'message': 'Insufficient data after cleaning'}
        
        # Run analysis
        multipliers = {'H': h_mult, 'W': w_mult, 'D': d_mult}
        df_combined = df.copy()
        df_combined['is_outlier_combined'] = False
        df_combined['cluster'] = None
        
        # Calculate and save IQR status only if save_to_db is True
        if save_to_db:
            df_iqr = calculate_dynamic_iqr(df_combined.copy(), multipliers=multipliers)
            df_iqr['iqr_is_outlier'] = False
            df_iqr['H_is_outlier'] = False
            df_iqr['W_is_outlier'] = False
            df_iqr['D_is_outlier'] = False
            
            unique_types = df_iqr['Type'].unique()
            
            if len(unique_types) > 1:
                for product_type in unique_types:
                    type_mask = df_iqr['Type'] == product_type
                    type_data = df_iqr[type_mask]
                    
                    if len(type_data) >= 4:
                        iqr_stats = {}
                        for dim in ['H', 'W', 'D']:
                            iqr_stats[dim] = {
                                'Q1': type_data[f'{dim}_Q1'].iloc[0],
                                'Q3': type_data[f'{dim}_Q3'].iloc[0],
                                'IQR': type_data[f'{dim}_IQR'].iloc[0],
                                'lower_bound': type_data[f'{dim}_lower_bound'].iloc[0],
                                'upper_bound': type_data[f'{dim}_upper_bound'].iloc[0]
                            }
                        
                        outlier_mask = detect_outliers_iqr(type_data, iqr_stats)
                        df_iqr.loc[type_mask, 'iqr_is_outlier'] = outlier_mask
                        
                        # Per-dimension outliers
                        for dim in ['H', 'W', 'D']:
                            dim_outlier = (type_data[dim] < iqr_stats[dim]['lower_bound']) | (type_data[dim] > iqr_stats[dim]['upper_bound'])
                            df_iqr.loc[type_mask, f'{dim}_is_outlier'] = dim_outlier
            else:
                if len(df_iqr) >= 4:
                    iqr_stats = {}
                    for dim in ['H', 'W', 'D']:
                        iqr_stats[dim] = {
                            'Q1': df_iqr[f'{dim}_Q1'].iloc[0],
                            'Q3': df_iqr[f'{dim}_Q3'].iloc[0],
                            'IQR': df_iqr[f'{dim}_IQR'].iloc[0],
                            'lower_bound': df_iqr[f'{dim}_lower_bound'].iloc[0],
                            'upper_bound': df_iqr[f'{dim}_upper_bound'].iloc[0]
                        }
                    
                    outlier_mask = detect_outliers_iqr(df_iqr, iqr_stats)
                    df_iqr['iqr_is_outlier'] = outlier_mask
                    
                    # Per-dimension outliers
                    for dim in ['H', 'W', 'D']:
                        dim_outlier = (df_iqr[dim] < iqr_stats[dim]['lower_bound']) | (df_iqr[dim] > iqr_stats[dim]['upper_bound'])
                        df_iqr[f'{dim}_is_outlier'] = dim_outlier
            
            iqr_updates = []
            for _, row in df_iqr.iterrows():
                iqr_updates.append({
                    'system_product_id': row['system_product_id'],
                    'iqr_status': 0 if row.get('iqr_is_outlier', False) else 1,
                    'iqr_height_status': 0 if row.get('H_is_outlier', False) else 1,
                    'iqr_width_status': 0 if row.get('W_is_outlier', False) else 1,
                    'iqr_depth_status': 0 if row.get('D_is_outlier', False) else 1
                })
            
            if iqr_updates:
                repo.update_products_iqr_fields(iqr_updates)
        
        # DBSCAN Analysis (only DBSCAN for now as per requirements)
        if ALGO_DBSCAN in algorithms:
            is_outlier_dbscan, df_dbscan = detect_outliers_dbscan(df_combined.copy(), eps=dbscan_eps, min_samples=dbscan_min_samples)
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


def analyze_and_save(group_id, brands, category, types, algorithms, h_mult, w_mult, d_mult, dbscan_eps, dbscan_min_samples, analysis_mode, save_to_db):
    """Analyze products and save to dimension tables"""
    from repositories.dimension.product_iteration_repository import ProductIterationRepository
    from repositories.dimension.product_iteration_item_repository import DimensionProductIterationItemRepository
    from models.dimension.product_iteration_item import DimensionProductIterationItem
    
    db = SessionLocal()
    try:
        repo = ProductRepository(db)
        iter_repo = ProductIterationRepository(db)
        item_repo = DimensionProductIterationItemRepository(db)
        
        brand = brands[0] if brands and len(brands) > 0 else None
        product_types = types if types and len(types) > 0 else None
        algorithm = algorithms[0] if algorithms and len(algorithms) > 0 else ALGO_DBSCAN
        
        # Fetch most recent iteration based on whether product_types is selected
        if product_types:
            # Product type selected: match with product_type
            existing_iter = iter_repo.find_existing_iteration(
                brand, category, product_types,
                dbscan_eps, dbscan_min_samples, algorithm, group_id
            )
        else:
            # Product type NOT selected: match without product_type (NULL)
            from models.dimension.product_iteration import ProductIteration
            existing_iter = db.query(ProductIteration).filter(
                ProductIteration.brand == brand,
                ProductIteration.category == category,
                ProductIteration.eps == dbscan_eps,
                ProductIteration.sample == dbscan_min_samples,
                ProductIteration.algorithm == algorithm,
                ProductIteration.product_group_id == group_id
            ).order_by(ProductIteration.timestamp.desc()).first()
        
        # Delete existing iteration if found and save_to_db is True and analysis_mode is 'all'
        if analysis_mode == 'all' and existing_iter and save_to_db:
            iter_repo.delete_iteration_with_items(existing_iter.iteration_id)
        
        # Load products based on analysis mode
        if analysis_mode == 'all':
            # Load all products
            df = repo.load_products_filtered(group_id, brands, category, types)
        else:
            # Load only normal products from existing iteration
            if not existing_iter:
                return {'ok': False, 'message': 'No previous iteration found for normal analysis'}
            
            # Get normal products from existing iteration
            normal_ids = db.query(DimensionProductIterationItem.system_product_id).filter(
                DimensionProductIterationItem.iteration_id == existing_iter.iteration_id,
                DimensionProductIterationItem.status == 1
            ).all()
            
            if not normal_ids:
                return {'ok': False, 'message': 'No normal products found'}
            
            system_product_ids = [r[0] for r in normal_ids]
            df = repo.load_products_by_ids(system_product_ids)
        
        if df.empty or len(df) < 4:
            return {'ok': False, 'message': 'Insufficient data'}
        
        # Rename columns
        df = df.rename(columns={
            'qb_code': 'SKU', 'brand': 'Brand', 'category': 'Category',
            'product_type': 'Type', 'name': 'Name', 'height': 'H',
            'width': 'W', 'depth': 'D', 'base_image_url': 'imageUrl',
            'product_url': 'url_key'
        })
        
        for col in ['H', 'W', 'D']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        df = df.dropna(subset=['H', 'W', 'D'])
        
        if df.empty or len(df) < 4:
            return {'ok': False, 'message': 'Insufficient data after cleaning'}
        
        # Run analysis
        df_combined = df.copy()
        df_combined['is_outlier_combined'] = False
        df_combined['cluster'] = None
        
        # Calculate and save IQR status only if save_to_db is True
        if save_to_db:
            multipliers = {'H': h_mult, 'W': w_mult, 'D': d_mult}
            df_iqr = calculate_dynamic_iqr(df_combined.copy(), multipliers=multipliers)
            df_iqr['iqr_is_outlier'] = False
            df_iqr['H_is_outlier'] = False
            df_iqr['W_is_outlier'] = False
            df_iqr['D_is_outlier'] = False
            
            unique_types = df_iqr['Type'].unique()
            
            if len(unique_types) > 1:
                for product_type in unique_types:
                    type_mask = df_iqr['Type'] == product_type
                    type_data = df_iqr[type_mask]
                    
                    if len(type_data) >= 4:
                        iqr_stats = {}
                        for dim in ['H', 'W', 'D']:
                            iqr_stats[dim] = {
                                'Q1': type_data[f'{dim}_Q1'].iloc[0],
                                'Q3': type_data[f'{dim}_Q3'].iloc[0],
                                'IQR': type_data[f'{dim}_IQR'].iloc[0],
                                'lower_bound': type_data[f'{dim}_lower_bound'].iloc[0],
                                'upper_bound': type_data[f'{dim}_upper_bound'].iloc[0]
                            }
                        
                        outlier_mask = detect_outliers_iqr(type_data, iqr_stats)
                        df_iqr.loc[type_mask, 'iqr_is_outlier'] = outlier_mask
                        
                        # Per-dimension outliers
                        for dim in ['H', 'W', 'D']:
                            dim_outlier = (type_data[dim] < iqr_stats[dim]['lower_bound']) | (type_data[dim] > iqr_stats[dim]['upper_bound'])
                            df_iqr.loc[type_mask, f'{dim}_is_outlier'] = dim_outlier
            else:
                if len(df_iqr) >= 4:
                    iqr_stats = {}
                    for dim in ['H', 'W', 'D']:
                        iqr_stats[dim] = {
                            'Q1': df_iqr[f'{dim}_Q1'].iloc[0],
                            'Q3': df_iqr[f'{dim}_Q3'].iloc[0],
                            'IQR': df_iqr[f'{dim}_IQR'].iloc[0],
                            'lower_bound': df_iqr[f'{dim}_lower_bound'].iloc[0],
                            'upper_bound': df_iqr[f'{dim}_upper_bound'].iloc[0]
                        }
                    
                    outlier_mask = detect_outliers_iqr(df_iqr, iqr_stats)
                    df_iqr['iqr_is_outlier'] = outlier_mask
                    
                    # Per-dimension outliers
                    for dim in ['H', 'W', 'D']:
                        dim_outlier = (df_iqr[dim] < iqr_stats[dim]['lower_bound']) | (df_iqr[dim] > iqr_stats[dim]['upper_bound'])
                        df_iqr[f'{dim}_is_outlier'] = dim_outlier
            
            iqr_updates = []
            for _, row in df_iqr.iterrows():
                iqr_updates.append({
                    'system_product_id': row['system_product_id'],
                    'iqr_status': 0 if row.get('iqr_is_outlier', False) else 1,
                    'iqr_height_status': 0 if row.get('H_is_outlier', False) else 1,
                    'iqr_width_status': 0 if row.get('W_is_outlier', False) else 1,
                    'iqr_depth_status': 0 if row.get('D_is_outlier', False) else 1
                })
            
            if iqr_updates:
                repo.update_products_iqr_fields(iqr_updates)
        
        if ALGO_DBSCAN in algorithms:
            is_outlier_dbscan, df_dbscan = detect_outliers_dbscan(df_combined.copy(), eps=dbscan_eps, min_samples=dbscan_min_samples)
            df_combined['is_outlier_combined'] = is_outlier_dbscan
            df_combined['dbscan_cluster'] = df_dbscan['dbscan_cluster']
            df_combined['cluster'] = df_dbscan['dbscan_cluster'].apply(
                lambda x: f"Cluster {x}" if x != -1 else "Noise/Outlier"
            )
        
        # Calculate statistics
        total = len(df_combined)
        outliers = df_combined['is_outlier_combined'].sum()
        normals = total - outliers
        
        # Save to DB if requested
        if save_to_db:
            # Save new iteration
            iteration_id = iter_repo.save_iteration(
                brand, category, product_types,
                group_id, algorithm, dbscan_eps, dbscan_min_samples
            )
            
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
                item_repo.save_items(items_data)
                
                # Get aggregated status and update product table
                system_product_ids = df_combined['system_product_id'].tolist()
                aggregated = item_repo.get_aggregated_status_by_product(system_product_ids)
                
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
        }
    except Exception as e:
        db.rollback()
        print(f"Error in analyze_and_save: {e}")
        return {'ok': False, 'message': str(e)}
    finally:
        db.close()
