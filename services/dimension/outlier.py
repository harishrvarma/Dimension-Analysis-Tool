"""
Product Outlier Detection System - Helper Functions
"""

import pandas as pd
import os
import numpy as np
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler
from datetime import datetime
import logging

__version__ = "1.1.0"

if not os.path.exists('logs'):
    os.makedirs('logs')

def setup_logger(log_filename):
    logger = logging.getLogger('outlier_analysis')
    logger.setLevel(logging.INFO)
    logger.handlers = []
    fh = logging.FileHandler(log_filename)
    fh.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    return logger

def get_csv_files_from_items_folder():
    items_folder = 'var/items'
    if not os.path.exists(items_folder):
        os.makedirs(items_folder)
        return []
    csv_files = [f for f in os.listdir(items_folder) if f.endswith('.csv')]
    return sorted(csv_files)

def load_data_from_file(filename):
    if not filename:
        return pd.DataFrame()
    
    csv_file = os.path.join('var/items', filename)
    if not os.path.exists(csv_file):
        return pd.DataFrame()
    
    df = pd.read_csv(csv_file, low_memory=False)
    column_mapping = {col.lower(): col for col in df.columns}
    standard_columns = {
        'web_id': 'SKU', 'product_id': 'product_id', 'brand': 'Brand',
        'category': 'Category', 'product_type': 'Type', 'name': 'Name',
        'height': 'H', 'width': 'W', 'depth': 'D', 'weight': 'weight',
        'base_image_url': 'imageUrl', 'url_key': 'url_key',
        'imageurl': 'imageUrl', 'sku': 'SKU', 'type': 'Type',
        'h': 'H', 'w': 'W', 'd': 'D'
    }
    
    rename_dict = {}
    for lower_col, actual_col in column_mapping.items():
        if lower_col in standard_columns:
            rename_dict[actual_col] = standard_columns[lower_col]
    df = df.rename(columns=rename_dict)
    
    for col in ['H', 'W', 'D']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    if 'weight' in df.columns:
        df['weight'] = pd.to_numeric(df['weight'], errors='coerce')
    
    required_cols = ['H', 'W', 'D']
    existing_required = [col for col in required_cols if col in df.columns]
    if len(existing_required) == len(required_cols):
        df = df.dropna(subset=existing_required)
    else:
        return pd.DataFrame()
    return df

def calculate_iqr_bounds(data_subset, dimensions=['H', 'W', 'D'], multiplier=1.5):
    iqr_stats = {}
    for dim in dimensions:
        Q1 = data_subset[dim].quantile(0.25)
        Q3 = data_subset[dim].quantile(0.75)
        IQR = Q3 - Q1
        iqr_stats[dim] = {
            'Q1': Q1, 'Q3': Q3, 'IQR': IQR,
            'lower_bound': Q1 - (multiplier * IQR),
            'upper_bound': Q3 + (multiplier * IQR),
            'multiplier': multiplier
        }
    return iqr_stats

def detect_outliers_iqr(df_subset, iqr_stats):
    outlier_flags = {}
    for dim in ['H', 'W', 'D']:
        lower = iqr_stats[dim]['lower_bound']
        upper = iqr_stats[dim]['upper_bound']
        outlier_flags[dim] = (df_subset[dim] < lower) | (df_subset[dim] > upper)
    is_outlier = outlier_flags['H'] | outlier_flags['W'] | outlier_flags['D']
    return is_outlier, outlier_flags



def detect_outliers_dbscan(filtered_df, eps=1.0, min_samples=4):
    df_dbscan = filtered_df.copy()
    X = df_dbscan[['H', 'W', 'D']].values
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    dbscan = DBSCAN(eps=eps, min_samples=min_samples, metric='euclidean')
    clusters = dbscan.fit_predict(X_scaled)
    is_outlier_dbscan = pd.Series((clusters == -1), index=df_dbscan.index)
    outlier_flags = {'H': is_outlier_dbscan, 'W': is_outlier_dbscan, 'D': is_outlier_dbscan}
    return is_outlier_dbscan, outlier_flags, clusters

def clean_old_files():
    logger = logging.getLogger()
    handlers = logger.handlers[:]
    for handler in handlers:
        handler.close()
        logger.removeHandler(handler)
    if os.path.exists('logs'):
        for file in os.listdir('logs'):
            file_path = os.path.join('logs', file)
            try:
                if os.path.isfile(file_path):
                    os.unlink(file_path)
            except:
                pass

def analyze_and_export(df, brands, categories, selected_algorithms, filename):
    clean_old_files()
    log_filename = os.path.join('logs', 'analysis.log')
    logger = setup_logger(log_filename)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    logger.info("="*80)
    logger.info("OUTLIER ANALYSIS STARTED")
    logger.info("="*80)
    logger.info(f"System Version: {__version__}")
    logger.info(f"Timestamp: {timestamp}")
    logger.info(f"Source File: {filename}")
    logger.info(f"Selected Algorithm(s): {', '.join(selected_algorithms)}")
    logger.info(f"Selected Brand(s): {', '.join(brands) if brands else 'No specific brands (filtering only)'}")
    logger.info(f"Selected Category(ies): {', '.join(categories)}")
    logger.info(f"Total Products in File: {len(df)}")
    logger.info("="*80)
    
    if not categories:
        raise ValueError("Categories must be selected for analysis")
    
    if brands:
        filtered_df = df[(df['Brand'].isin(brands)) & (df['Category'].isin(categories))].copy()
        logger.info(f"Filtering by specific brands: {', '.join(brands)}")
    else:
        filtered_df = df[df['Category'].isin(categories)].copy()
        logger.info("No brand filter applied - including all brands")
    
    logger.info(f"\nFiltered Data: {len(filtered_df)} products match selected filters")
    logger.info(f"Analyzing {len(categories)} category(ies)")
    logger.info("")
    
    if 'IQR' in selected_algorithms:
        filtered_df['iqr_height'] = 'No'
        filtered_df['iqr_width'] = 'No'
        filtered_df['iqr_depth'] = 'No'
        filtered_df['iqr_status'] = 'No'
    
    if 'DBSCAN' in selected_algorithms:
        filtered_df['dbscan_height'] = 'No'
        filtered_df['dbscan_width'] = 'No'
        filtered_df['dbscan_depth'] = 'No'
        filtered_df['dbscan_cluster'] = -999
        filtered_df['dbscan_status'] = 'No'
    
    
    filtered_df['final_status'] = 'No'
    
    for category in categories:
        mask = filtered_df['Category'] == category
        subset_data = filtered_df[mask].copy()
        
        if len(subset_data) < 4:
            logger.warning(f"[{category}] Insufficient data: {len(subset_data)} products (need at least 4). Skipping.")
            continue
        
        logger.info(f"\n{'='*80}")
        logger.info(f"CATEGORY: {category}")
        logger.info(f"{'='*80}")
        logger.info(f"Total Products: {len(subset_data)}")
        
        if 'IQR' in selected_algorithms:
            logger.info("\n--- IQR Analysis ---")
            iqr_stats = calculate_iqr_bounds(subset_data)
            is_outlier_iqr, iqr_flags = detect_outliers_iqr(subset_data, iqr_stats)
            
            for dim in ['H', 'W', 'D']:
                logger.info(f"{dim}: Q1={iqr_stats[dim]['Q1']:.2f}, Q3={iqr_stats[dim]['Q3']:.2f}, "
                           f"IQR={iqr_stats[dim]['IQR']:.2f}, "
                           f"Range=[{iqr_stats[dim]['lower_bound']:.2f}, {iqr_stats[dim]['upper_bound']:.2f}]")
            
            filtered_df.loc[mask, 'iqr_height'] = iqr_flags['H'].map({True: 'No', False: 'Yes'})
            filtered_df.loc[mask, 'iqr_width'] = iqr_flags['W'].map({True: 'No', False: 'Yes'})
            filtered_df.loc[mask, 'iqr_depth'] = iqr_flags['D'].map({True: 'No', False: 'Yes'})
            filtered_df.loc[mask, 'iqr_status'] = (~is_outlier_iqr).map({True: 'Yes', False: 'No'})
            
            normal_count = (~is_outlier_iqr).sum()
            outlier_count = is_outlier_iqr.sum()
            logger.info(f"IQR Results: Normal={normal_count}, Outliers={outlier_count} ({outlier_count/len(subset_data)*100:.2f}%)")
        
        if 'DBSCAN' in selected_algorithms:
            logger.info("\n--- DBSCAN Analysis ---")
            is_outlier_dbscan, dbscan_flags, clusters = detect_outliers_dbscan(subset_data)
            
            filtered_df.loc[mask, 'dbscan_height'] = dbscan_flags['H'].map({True: 'No', False: 'Yes'})
            filtered_df.loc[mask, 'dbscan_width'] = dbscan_flags['W'].map({True: 'No', False: 'Yes'})
            filtered_df.loc[mask, 'dbscan_depth'] = dbscan_flags['D'].map({True: 'No', False: 'Yes'})
            filtered_df.loc[mask, 'dbscan_cluster'] = clusters
            filtered_df.loc[mask, 'dbscan_status'] = (~is_outlier_dbscan).map({True: 'Yes', False: 'No'})
            
            normal_count = (~is_outlier_dbscan).sum()
            outlier_count = is_outlier_dbscan.sum()
            num_clusters = len([c for c in set(clusters) if c != -1])
            logger.info(f"DBSCAN Results: Normal={normal_count}, Outliers={outlier_count} ({outlier_count/len(subset_data)*100:.2f}%)")
            logger.info(f"Clusters Found: {num_clusters}")

    status_columns = []
    if 'IQR' in selected_algorithms:
        status_columns.append('iqr_status')
    if 'DBSCAN' in selected_algorithms:
        status_columns.append('dbscan_status')

    if status_columns:
        filtered_df['final_status'] = filtered_df[status_columns].apply(
            lambda row: 'Yes' if any(row == 'Yes') else 'No', axis=1
        )
    
    logger.info("\n" + "="*80)
    logger.info("FINAL SUMMARY")
    logger.info("="*80)
    total_analyzed = len(filtered_df)
    final_valid = (filtered_df['final_status'] == 'Yes').sum()
    final_invalid = (filtered_df['final_status'] == 'No').sum()
    logger.info(f"Total Products Analyzed: {total_analyzed}")
    logger.info(f"Final Status - Valid (Yes): {final_valid} ({final_valid/total_analyzed*100:.2f}%)")
    logger.info(f"Final Status - Invalid (No): {final_invalid} ({final_invalid/total_analyzed*100:.2f}%)")
    logger.info("="*80)
    logger.info("ANALYSIS COMPLETED")
    logger.info("="*80)
    
    output_filename = "analysis_results.csv"
    output_path = os.path.join('logs', output_filename)
    export_df = filtered_df.copy()
    column_rename = {
        'SKU': 'web_id', 'Brand': 'brand', 'Category': 'category',
        'Type': 'product_type', 'Name': 'name', 'H': 'height',
        'W': 'width', 'D': 'depth', 'imageUrl': 'base_image_url'
    }
    export_df = export_df.rename(columns=column_rename)
    export_df.to_csv(output_path, index=False)
    logger.info(f"\nResults exported to: {output_path}")
    
    summary_data = []
    display_brands = filtered_df['Brand'].unique().tolist()
    for brand in display_brands:
        for category in categories:
            brand_cat_data = filtered_df[(filtered_df['Brand'] == brand) & (filtered_df['Category'] == category)]
            if len(brand_cat_data) > 0:
                total = len(brand_cat_data)
                normal = (brand_cat_data['final_status'] == 'Yes').sum()
                outlier = (brand_cat_data['final_status'] == 'No').sum()
                summary_data.append({
                    'brand': brand, 'category': category, 'total': total,
                    'normal': normal, 'normal_pct': round(normal/total*100, 2),
                    'outlier': outlier, 'outlier_pct': round(outlier/total*100, 2)
                })
    
    return log_filename, output_path, total_analyzed, final_valid, final_invalid, summary_data, filtered_df

def generate_filtered_csvs(processed_df):
    column_rename = {
        'SKU': 'web_id', 'Brand': 'brand', 'Category': 'category',
        'Type': 'product_type', 'Name': 'name', 'H': 'height',
        'W': 'width', 'D': 'depth', 'imageUrl': 'base_image_url'
    }
    export_df = processed_df.rename(columns=column_rename)
    normal_df = export_df[export_df['final_status'] == 'Yes'].copy()
    outlier_df = export_df[export_df['final_status'] == 'No'].copy()
    normal_path = os.path.join('logs', "normal_items.csv")
    outlier_path = os.path.join('logs', "outlier_items.csv")
    normal_df.to_csv(normal_path, index=False)
    outlier_df.to_csv(outlier_path, index=False)
    return normal_path, outlier_path
