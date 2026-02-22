from models.base.base import SessionLocal
from repositories.product_repository import ProductRepository
from repositories.product_group_repository import ProductGroupRepository
import pandas as pd
import numpy as np
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler
 

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


def get_brands_for_group(group_id):
    """Get brands with product counts for a specific product group"""
    db = SessionLocal()
    try:
        repo = ProductRepository(db)
        df = repo.get_brands_for_group(group_id)
        if df.empty:
            return []
        return [
            {"label": f"{row['brand']} ({row['product_count']})", "value": row['brand']}
            for _, row in df.iterrows()
        ]
    finally:
        db.close()


def get_categories_for_group(group_id, brands=None):
    """Get categories for a group, optionally filtered by brands"""
    db = SessionLocal()
    try:
        repo = ProductRepository(db)
        df = repo.get_categories_for_group(group_id, brands)
        if df.empty:
            return []
        return [
            {"label": f"{row['category']} ({row['product_count']})", "value": row['category']}
            for _, row in df.iterrows()
        ]
    finally:
        db.close()


def get_types_for_group(group_id, brands=None, category=None):
    """Get product types for a group"""
    db = SessionLocal()
    try:
        repo = ProductRepository(db)
        df = repo.get_types_for_group(group_id, brands, category)
        if df.empty:
            return []
        return [
            {"label": f"{row['product_type']} ({row['product_count']})", "value": row['product_type']}
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
    db = SessionLocal()
    try:
        repo = ProductRepository(db)
        return repo.get_iteration_history(group_id, brands, category, types)
    finally:
        db.close()


def reset_iterations(group_id, brands, category, types):
    """Reset all iterations for a category"""
    db = SessionLocal()
    try:
        repo = ProductRepository(db)
        repo.reset_iterations(group_id, brands, category, types)
        return True
    except:
        return False
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
            if 'DBSCAN' in algorithms and pd.notna(row.get('dbs_status')):
                df.at[idx, 'dbscan_is_outlier'] = (row['dbs_status'] == 0)
                df.at[idx, 'dbscan_cluster'] = -1 if row['dbs_status'] == 0 else 0
        
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
        
        return df.to_dict('records')
    finally:
        db.close()


def analyze_products(group_id, brands, category, types, algorithms, h_mult, w_mult, d_mult, dbscan_eps, dbscan_min_samples, iteration=1, is_next=False, load_all=False):
    """Main analysis function with iteration support"""
    db = SessionLocal()
    try:
        repo = ProductRepository(db)
        
        # If load_all=True, load all products from iteration 1 for export
        if load_all:
            df = load_products_filtered(group_id, brands, category, types, iteration=1, for_save=False, for_display=False)
        # If is_next=True, save previous iteration results
        elif is_next and iteration > 1:
            prev_iteration = iteration - 1
            # Load the SAME products that were analyzed in previous iteration
            # Don't reload - we need to save only what was actually processed
            # This is handled by loading products for the previous iteration
            df_prev = load_products_filtered(group_id, brands, category, types, prev_iteration, for_save=False)
            if not df_prev.empty:
                # Run analysis on previous iteration to get outlier status
                multipliers = {'H': h_mult, 'W': w_mult, 'D': d_mult}
                df_analyzed = df_prev.copy()
                
                # Always calculate IQR Analysis
                df_iqr = calculate_dynamic_iqr(df_analyzed.copy(), multipliers=multipliers)
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
                
                df_analyzed['iqr_is_outlier'] = df_iqr['iqr_is_outlier'].values
                
                # Always calculate DBSCAN Analysis
                is_outlier_dbscan, df_dbscan = detect_outliers_dbscan(df_analyzed.copy(), eps=dbscan_eps, min_samples=dbscan_min_samples)
                df_analyzed['dbscan_is_outlier'] = is_outlier_dbscan.values
                df_analyzed['dbscan_cluster'] = df_dbscan['dbscan_cluster']
                
                # Prepare updates for database - update all products
                product_updates = []
                for _, row in df_analyzed.iterrows():
                    iqr_outlier = row['iqr_is_outlier']
                    dbs_outlier = row['dbscan_is_outlier']
                    
                    update = {
                        'product_id': row['product_id'],
                        'iteration_closed': prev_iteration
                    }
                    
                    # Always save both algorithm results
                    update['iqr_status'] = 0 if iqr_outlier else 1
                    if 'H_lower_bound' in df_iqr.columns:
                        h_outlier = row['H'] < df_iqr.loc[row.name, 'H_lower_bound'] or row['H'] > df_iqr.loc[row.name, 'H_upper_bound']
                        w_outlier = row['W'] < df_iqr.loc[row.name, 'W_lower_bound'] or row['W'] > df_iqr.loc[row.name, 'W_upper_bound']
                        d_outlier = row['D'] < df_iqr.loc[row.name, 'D_lower_bound'] or row['D'] > df_iqr.loc[row.name, 'D_upper_bound']
                        update['iqr_height_status'] = 0 if h_outlier else 1
                        update['iqr_width_status'] = 0 if w_outlier else 1
                        update['iqr_depth_status'] = 0 if d_outlier else 1
                    
                    update['dbs_status'] = 0 if dbs_outlier else 1
                    
                    # Calculate final_status based on selected algorithms only
                    if 'IQR' in algorithms and 'DBSCAN' in algorithms:
                        # Both selected: if either is normal, final is normal; if both are outlier, final is outlier
                        update['final_status'] = 1 if (not iqr_outlier or not dbs_outlier) else 0
                    elif 'IQR' in algorithms:
                        # Only IQR selected
                        update['final_status'] = 0 if iqr_outlier else 1
                    elif 'DBSCAN' in algorithms:
                        # Only DBSCAN selected
                        update['final_status'] = 0 if dbs_outlier else 1
                    else:
                        update['final_status'] = None
                    
                    product_updates.append(update)
                
                # Save to database
                repo.update_iteration_results(product_updates)
        
        # Skip loading current iteration data if load_all is True
        if load_all:
            # For load_all, we already loaded iteration 1 data above
            # Now run analysis on it
            multipliers = {'H': h_mult, 'W': w_mult, 'D': d_mult}
            df_combined = df.copy()
            df_combined['is_outlier_combined'] = False
        else:
            # Load data for current iteration
            df = load_products_filtered(group_id, brands, category, types, iteration, for_display=True)
        
            if df.empty or len(df) < 4:
                return None, "Insufficient data 1"
        
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
            
            dbscan_cols = [col for col in df_dbscan.columns if col.startswith('dbscan_')]
            for col in dbscan_cols:
                df_combined[col] = df_dbscan[col]
        
        # Calculate statistics
        total = len(df_combined)
        outliers = df_combined['is_outlier_combined'].sum()
        normals = total - outliers
        
        # Convert to dict and ensure boolean values are proper Python bools
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
