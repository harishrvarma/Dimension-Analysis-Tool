from models.base.base import SessionLocal
from repositories.pricing.product_repository import ProductRepository
from sklearn.cluster import DBSCAN, HDBSCAN
from sklearn.preprocessing import StandardScaler
import pandas as pd
import csv
from io import StringIO


def analyze_all_and_export(product_group_id, algorithm='DBSCAN', record_type='all', 
                           configs=None, filters=None, algorithm_settings=None, axis_cols=None):
    """Analyze all products and export results - OPTIMIZED VERSION
    
    Args:
        product_group_id: Product group ID to filter products
        algorithm: 'DBSCAN'
        record_type: 'all' (pending option removed)
        configs: List of (param1, param2) tuples for configurations
        filters: Dict with 'brands', 'categories', 'product_types'
        algorithm_settings: List of settings ['shape', 'size', 'volume']
    """
    import time
    import uuid
    from concurrent.futures import ThreadPoolExecutor, as_completed
    
    start_time = time.time()
    
    # Generate unique number for this analysis
    unique_number = str(uuid.uuid4())[:8]
    
    # Use provided configs or default (reduced for performance)
    if configs:
        config = configs
    else:
        # Reduced config set for better performance
        config = [
            (2.0, 2), (1.5, 2), (1.0, 2), (0.5, 2),
            (2.0, 3), (1.5, 3), (1.0, 3), (0.5, 3),
            (2.0, 4), (1.5, 4), (1.0, 4), (0.5, 4)
        ]
    
    db = SessionLocal()
    try:
        repo = ProductRepository(db)
        
        # Fetch products with filters
        print(f"[{time.time()-start_time:.1f}s] Fetching products...")
        df = repo.get_all_products_for_export(filters, record_type, product_group_id, axis_cols=axis_cols)
        
        if df.empty:
            return None, "No products found"
        
        # Resolve axis column names for CSV headers
        col_x, col_y, col_z = (axis_cols or ['mfr_cost', 'shipping_cost', 'profit_margin'])
        
        print(f"[{time.time()-start_time:.1f}s] Loaded {len(df)} products")
        
        # Get category counts for all products and analyzed/pending products
        category_counts = repo.get_category_product_counts(product_group_id)
        
        # Group by category only
        grouped = df.groupby('category')
        
        total_groups = len(grouped)
        print(f"[{time.time()-start_time:.1f}s] Processing {total_groups} groups...")
        
        results = []
        group_key_counter = 1
        
        for group_idx, (group_key, group_df) in enumerate(grouped, 1):
            if group_idx % 5 == 0:
                print(f"[{time.time()-start_time:.1f}s] Processing group {group_idx}/{total_groups}...")
            
            total_items = len(group_df)
            unique_group_key = group_key_counter
            group_key_counter += 1
            
            # Determine which configs to execute
            executable_configs = [(p1, p2) for p1, p2 in config if total_items >= p2]
            executed = "Yes" if executable_configs else "No"
            
            # Get category counts
            category_val = str(group_key)
            total_all_products = category_counts.get(category_val, {}).get('total', 0)
            analyzed_count = category_counts.get(category_val, {}).get('analyzed', 0)
            pending_count = category_counts.get(category_val, {}).get('pending', 0)
            
            # Initialize result containers
            outlier_counts = {f"{p1} X {p2}": "-" for p1, p2 in config}
            outlier_statuses = {}
            cluster_assignments = {}
            cluster_counts = {}  # Pre-calculated cluster counts
            
            # Run analysis for executable configs with parallel processing
            if executable_configs:
                # Use ThreadPoolExecutor for parallel processing
                with ThreadPoolExecutor(max_workers=min(4, len(executable_configs))) as executor:
                    future_to_config = {
                        executor.submit(run_dbscan_analysis, group_df, p1, p2, algorithm_settings, axis_cols): (p1, p2)
                        for p1, p2 in executable_configs
                    }
                    
                    for future in as_completed(future_to_config):
                        param1, param2 = future_to_config[future]
                        col_name = f"{param1} X {param2}"
                        
                        try:
                            outlier_count, product_statuses, product_clusters = future.result()
                            outlier_counts[col_name] = outlier_count
                            outlier_statuses[col_name] = product_statuses
                            cluster_assignments[col_name] = product_clusters
                            
                            # Calculate cluster counts once per config (optimized)
                            cluster_count_dict = {}
                            for cluster in product_clusters.values():
                                cluster_key = "Noise/Outlier" if cluster == -1 else f"Cluster {cluster}"
                                cluster_count_dict[cluster_key] = cluster_count_dict.get(cluster_key, 0) + 1
                            cluster_counts[col_name] = cluster_count_dict
                            
                        except Exception as e:
                            print(f"Error processing config {param1}x{param2}: {e}")
                            outlier_counts[col_name] = 0
                            outlier_statuses[col_name] = {}
                            cluster_assignments[col_name] = {}
                            cluster_counts[col_name] = {}
            
            # Pre-calculate total items in config for percentage calculations (avoid redundant loops)
            total_items_per_config = {}
            for col_name in outlier_statuses.keys():
                total_items_per_config[col_name] = len([s for s in outlier_statuses[col_name].values() if s != "-"])
            
            # Find config with minimum outliers (>0) for Issue column
            min_outlier_config = None
            min_outlier_count = float('inf')
            
            for param1, param2 in executable_configs:
                col_name = f"{param1} X {param2}"
                count = outlier_counts[col_name]
                if count != "-" and count > 0 and count < min_outlier_count:
                    min_outlier_count = count
                    min_outlier_config = col_name
            
            # Add each product to results
            for idx, row in group_df.iterrows():
                # Handle group_key for category grouping
                brand_val = row['brand']
                category_val = str(group_key)  # Ensure category is string, not tuple
                product_type_val = row['product_type']
                
                result_row = {
                    'System Product Id': row['system_product_id'],
                    'QB Code': row['qb_code'],
                    'Unique Group Key': unique_group_key,
                    'Brand': brand_val,
                    'Category': category_val,
                    'Product Type': product_type_val,
                    'Name': row['name'],
                    'Image URL': row['base_image_url'],
                    'Product URL': row['product_url'],
                    col_x: row[col_x],
                    col_y: row[col_y],
                    col_z: row[col_z],
                    'TOTAL ITEMS': total_all_products,
                    'Analyzed Item Count': analyzed_count,
                    'Pending Item Count': pending_count,
                    'EXECUTED': executed
                }
                
                # Add config columns
                status_values = []
                for p1, p2 in config:
                    col_name = f"{p1} X {p2}"
                    
                    # Add Outliers column
                    result_row[f"{col_name} Outliers"] = outlier_counts[col_name]
                    
                    # Add Status column
                    if col_name in outlier_statuses:
                        status = outlier_statuses[col_name].get(idx, "-")
                        result_row[f"{col_name} Status"] = status
                        if status != "-":
                            status_values.append(status)
                    else:
                        result_row[f"{col_name} Status"] = "-"
                    
                    # Add Cluster column with proper formatting
                    if col_name in cluster_assignments:
                        cluster = cluster_assignments[col_name].get(idx, "-")
                        if cluster == -1:
                            result_row[f"{col_name} Cluster"] = "Noise/Outlier"
                        elif cluster != "-":
                            result_row[f"{col_name} Cluster"] = f"Cluster {cluster}"
                        else:
                            result_row[f"{col_name} Cluster"] = "-"
                    else:
                        result_row[f"{col_name} Cluster"] = "-"
                    
                    # Add Cluster Item Count and Percentage (optimized - no redundant calculations)
                    cluster = cluster_assignments.get(col_name, {}).get(idx, "-")
                    if cluster == -1:
                        cluster_key = "Noise/Outlier"
                    elif cluster != "-":
                        cluster_key = f"Cluster {cluster}"
                    else:
                        cluster_key = "-"
                    
                    if cluster_key != "-" and col_name in cluster_counts and cluster_key in cluster_counts[col_name]:
                        cluster_item_count = cluster_counts[col_name][cluster_key]
                        result_row[f"{col_name} Cluster Item Count"] = cluster_item_count
                        
                        # Calculate cluster item percentage using pre-calculated totals
                        total_in_config = total_items_per_config.get(col_name, 0)
                        if total_in_config > 0:
                            cluster_percentage = (cluster_item_count / total_in_config * 100)
                            result_row[f"{col_name} Cluster Item Percentage"] = round(cluster_percentage, 2)
                        else:
                            result_row[f"{col_name} Cluster Item Percentage"] = 0.0
                    else:
                        result_row[f"{col_name} Cluster Item Count"] = "-"
                        result_row[f"{col_name} Cluster Item Percentage"] = "-"
                
                # Calculate Average Status %
                if status_values:
                    avg_status = (sum(status_values) / len(status_values)) * 100
                    result_row['Average Status %'] = round(avg_status, 2)
                    result_row['Status Column Count'] = len(status_values)
                else:
                    result_row['Average Status %'] = "-"
                    result_row['Status Column Count'] = 0
                
                results.append(result_row)
        
        print(f"[{time.time()-start_time:.1f}s] Analysis complete. Saving to database and generating CSV...")
        
        # Save results to database with batch operations
        try:
            save_analysis_to_database_optimized(unique_number, product_group_id, algorithm, config, results, algorithm_settings, axis_cols=axis_cols)
            print(f"[{time.time()-start_time:.1f}s] Results saved to database with unique_number: {unique_number}")
        except Exception as e:
            print(f"[{time.time()-start_time:.1f}s] Failed to save to database: {str(e)}")
        
        # Generate CSV
        if not results:
            return None, "No results to export"
        
        si = StringIO()
        fieldnames = ['System Product Id', 'QB Code', 'Brand', 'Category', 'Product Type', 'Name', 
                     'Image URL', 'Product URL', col_x, col_y, col_z, 'Unique Group Key', 'TOTAL ITEMS', 'Analyzed Item Count', 'Pending Item Count', 'EXECUTED']
        
        # Add columns based on analysis type
        # Show: Outliers, Status, Cluster, Cluster Item Count, Cluster Item Percentage for each config
        for p1, p2 in config:
            fieldnames.append(f"{p1} X {p2} Outliers")
            fieldnames.append(f"{p1} X {p2} Status")
            fieldnames.append(f"{p1} X {p2} Cluster")
            fieldnames.append(f"{p1} X {p2} Cluster Item Count")
            fieldnames.append(f"{p1} X {p2} Cluster Item Percentage")
        
        # Add Average Status % and Count columns at the end
        fieldnames.append('Average Status %')
        fieldnames.append('Status Column Count')
        
        writer = csv.DictWriter(si, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)
        
        print(f"[{time.time()-start_time:.1f}s] Export complete. Total time: {time.time()-start_time:.1f}s")
        
        return si.getvalue(), None
        
    except Exception as e:
        print(f"Error in analyze_all_and_export: {e}")
        import traceback
        traceback.print_exc()
        return None, str(e)
    finally:
        db.close()


def run_dbscan_analysis(df, eps, min_samples, algorithm_settings=None, axis_cols=None):
    """Run DBSCAN analysis and return outlier count, product statuses, and cluster assignments"""
    from sklearn.preprocessing import StandardScaler
    from sklearn.cluster import DBSCAN
    
    if len(df) < min_samples:
        return 0, {}, {}
    
    if algorithm_settings is None:
        algorithm_settings = ['shape', 'size', 'volume']
    
    col_x, col_y, col_z = (axis_cols or ['mfr_cost', 'shipping_cost', 'profit_margin'])
    required_cols = [col_x, col_y, col_z]
    
    df_dbscan = df.copy()
    
    missing_cols = [c for c in required_cols if c not in df_dbscan.columns]
    if missing_cols:
        return 0, {}, {}
    
    df_dbscan = df_dbscan.dropna(subset=required_cols)
    for _c in required_cols:
        df_dbscan[_c] = pd.to_numeric(df_dbscan[_c], errors='coerce').astype(float)
    df_dbscan = df_dbscan.dropna(subset=required_cols)
    df_dbscan = df_dbscan[(df_dbscan[required_cols] != 0).all(axis=1)]
    
    if len(df_dbscan) < min_samples:
        return 0, {}, {}
    
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
        df_dbscan['_ratio_xy'] = df_dbscan[col_x] / (df_dbscan[col_y] + eps_val)
        df_dbscan['_ratio_yz'] = df_dbscan[col_y] / (df_dbscan[col_z] + eps_val)
        df_dbscan['_ratio_xz'] = df_dbscan[col_x] / (df_dbscan[col_z] + eps_val)
        features.extend(['_ratio_xy', '_ratio_yz', '_ratio_xz'])
    
    if 'volume' in settings:
        df_dbscan['_volume'] = df_dbscan[col_x] * df_dbscan[col_y] * df_dbscan[col_z]
        features.append('_volume')
    
    seen = set()
    features = [f for f in features if not (f in seen or seen.add(f))]
    
    X = df_dbscan[features].values
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    dbscan = DBSCAN(eps=eps, min_samples=min_samples, metric='euclidean')
    clusters = dbscan.fit_predict(X_scaled)
    
    outlier_count = (clusters == -1).sum()
    
    product_statuses = {}
    product_clusters = {}
    for i, cluster in enumerate(clusters):
        idx = df_dbscan.index[i]
        product_statuses[idx] = 0 if cluster == -1 else 1
        product_clusters[idx] = int(cluster) if cluster != -1 else -1
    
    return int(outlier_count), product_statuses, product_clusters


def run_hdbscan_analysis(df, min_cluster_size, min_samples, axis_cols=None):
    """Run HDBSCAN analysis and return outlier count and product statuses"""
    col_x, col_y, col_z = (axis_cols or ['mfr_cost', 'shipping_cost', 'profit_margin'])
    required_cols = [col_x, col_y, col_z]
    
    if len(df) < min_samples:
        return 0, {}
    
    X = df[required_cols].values
    valid_mask = ~pd.isna(X).any(axis=1)
    X_clean = X[valid_mask]
    
    if len(X_clean) < min_samples:
        return 0, {}
    
    # Scale data
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X_clean)
    
    # Run HDBSCAN
    hdbscan = HDBSCAN(min_cluster_size=min_cluster_size, min_samples=min_samples)
    clusters = hdbscan.fit_predict(X_scaled)
    
    # Count outliers (cluster = -1)
    outlier_count = (clusters == -1).sum()
    
    # Map product statuses: 0 for outlier, 1 for normal
    product_statuses = {}
    valid_indices = df.index[valid_mask].tolist()
    for i, cluster in enumerate(clusters):
        idx = valid_indices[i]
        product_statuses[idx] = 0 if cluster == -1 else 1
    
    return int(outlier_count), product_statuses


def save_analysis_to_database(unique_number, product_group_id, algorithm, config, results, algorithm_settings):
    """Save analysis results to pricing_product_iteration and pricing_product_iteration_item tables"""
    from models.base.base import SessionLocal
    from repositories.pricing.product_iteration_repository import ProductIterationRepository
    from repositories.pricing.product_iteration_item_repository import PricingProductIterationItemRepository
    import json
    from datetime import datetime
    
    db = SessionLocal()
    try:
        iteration_repo = ProductIterationRepository(db)
        item_repo = PricingProductIterationItemRepository(db)
        
        # Get unique categories from results
        categories = set(result['Category'] for result in results)
        
        # Create iteration records for each configuration and category combination
        for category in categories:
            for eps, min_samples in config:
                # Get total items count for all products in this category
                from models.pricing.product import Product
                total_items_all = db.query(Product).filter(
                    Product.group_id == product_group_id,
                    Product.category == category,
                    Product.height.isnot(None),
                    Product.width.isnot(None),
                    Product.depth.isnot(None)
                ).count()
                
                # Get analyzed items count (where final_status != null) for this category
                analyzed_items_count = db.query(Product).filter(
                    Product.group_id == product_group_id,
                    Product.category == category,
                    Product.final_status.isnot(None),
                    Product.height.isnot(None),
                    Product.width.isnot(None),
                    Product.depth.isnot(None)
                ).count()
                
                # Get pending items count (where final_status = null) for this category
                pending_items_count = db.query(Product).filter(
                    Product.group_id == product_group_id,
                    Product.category == category,
                    Product.final_status.is_(None),
                    Product.height.isnot(None),
                    Product.width.isnot(None),
                    Product.depth.isnot(None)
                ).count()
                
                # Calculate outlier items for this configuration
                col_name = f"{eps} X {min_samples}"
                outlier_items = len([r for r in results if r['Category'] == category and r.get(f"{col_name} Status") == 0])
                
                iteration_id = iteration_repo.save_iteration(
                    brand=None,
                    category=category,
                    product_types=None,
                    product_group_id=product_group_id,
                    algorithm=algorithm,
                    eps=eps,
                    sample=min_samples,
                    unique_number=unique_number,
                    total_items=total_items_all,
                    analyzed_items=analyzed_items_count,
                    pending_items=pending_items_count,
                    outlier_items=outlier_items
                )
                
                # Create item records for this configuration and category
                item_data = []
                for result in results:
                    # Only save items for this category and configuration
                    if result['Category'] != category:
                        continue
                        
                    col_name = f"{eps} X {min_samples}"
                    if result.get(f"{col_name} Status") != '-':
                        # Determine cluster, outlier_mode, and status from DBSCAN results
                        status_val = result.get(f"{col_name} Status", '-')
                        cluster_val = result.get(f"{col_name} Cluster", None)  # Get cluster from results
                        cluster_items_val = result.get(f"{col_name} Cluster Item Count", 0)
                        outlier_mode_val = None
                        final_status_val = None
                        
                        if status_val == 0:  # Outlier
                            outlier_mode_val = 0  # Auto outlier
                        elif status_val == 1:  # Normal
                            outlier_mode_val = None
                        
                        # Calculate cluster_items_per based on total items in this iteration
                        cluster_items_per_val = 0.0
                        if cluster_items_val and cluster_items_val != '-':
                            # Get total items processed in this configuration for this category
                            total_items_in_config = len([r for r in results if r['Category'] == category and r.get(f"{col_name} Status") != '-'])
                            if total_items_in_config > 0:
                                cluster_items_per_val = (int(cluster_items_val) / total_items_in_config * 100)
                        
                        item_record = {
                            'iteration_id': iteration_id,
                            'system_product_id': result['System Product Id'],
                            'brand': result['Brand'],
                            'category': result['Category'],
                            'product_type': result['Product Type'],
                            'cluster': str(cluster_val) if cluster_val is not None else None,
                            'cluster_items': int(cluster_items_val) if cluster_items_val != '-' else 0,
                            'cluster_items_per': cluster_items_per_val,
                            'outlier_mode': outlier_mode_val,
                            'status': status_val if status_val != '-' else None,
                            'final_status': final_status_val,
                            'analyzed_date': None
                        }
                        item_data.append(item_record)
                
                # Batch insert items for this configuration and category
                if item_data:
                    item_repo.save_items(item_data)
                    print(f"Saved {len(item_data)} items for iteration {iteration_id} (category={category}, eps={eps}, sample={min_samples})")
        
        db.commit()
        
    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()


def save_analysis_to_database_optimized(unique_number, product_group_id, algorithm, config, results, algorithm_settings, axis_cols=None):
    """Save analysis results to pricing_product_iteration and pricing_product_iteration_item tables - OPTIMIZED VERSION"""
    from models.base.base import SessionLocal
    from repositories.pricing.product_iteration_repository import ProductIterationRepository
    from repositories.pricing.product_iteration_item_repository import PricingProductIterationItemRepository
    from models.pricing.product_iteration import ProductIteration
    from models.pricing.product_iteration_item import PricingProductIterationItem
    from sqlalchemy import func, case, text as _text
    import json
    from datetime import datetime
    
    col_x, col_y, col_z = (axis_cols or ['mfr_cost', 'shipping_cost', 'profit_margin'])
    
    db = SessionLocal()
    try:
        iteration_repo = ProductIterationRepository(db)
        item_repo = PricingProductIterationItemRepository(db)
        
        categories = set(result['Category'] for result in results)
        
        # Pre-calculate category statistics using dynamic axis columns
        from repositories.pricing.product_repository import ProductRepository
        prod_repo = ProductRepository(db)
        axis_not_null = prod_repo._axis_not_null_conditions([col_x, col_y, col_z])
        
        cat_placeholders = ', '.join([f':cat{i}' for i in range(len(categories))])
        cat_params = {f'cat{i}': cat for i, cat in enumerate(categories)}
        cat_params['group_id'] = product_group_id
        
        stats_query = _text(f"""
            SELECT category,
                COUNT(*) as total_items,
                SUM(CASE WHEN final_status IS NOT NULL THEN 1 ELSE 0 END) as analyzed_items,
                SUM(CASE WHEN final_status IS NULL THEN 1 ELSE 0 END) as pending_items
            FROM pricing_product
            WHERE group_id = :group_id
            AND category IN ({cat_placeholders})
            {axis_not_null}
            GROUP BY category
        """)
        stats_rows = db.execute(stats_query, cat_params).fetchall()
        
        category_stats = {}
        for stat in stats_rows:
            category_stats[stat[0]] = {
                'total_items': stat[1],
                'analyzed_items': stat[2],
                'pending_items': stat[3]
            }
        
        # Batch create iteration records
        iteration_records = []
        iteration_mapping = {}
        
        for category in categories:
            stats = category_stats.get(category, {'total_items': 0, 'analyzed_items': 0, 'pending_items': 0})
            
            for eps, min_samples in config:
                # Calculate outlier items for this configuration
                col_name = f"{eps} X {min_samples}"
                outlier_items = len([r for r in results if r['Category'] == category and r.get(f"{col_name} Status") == 0])
                
                iteration_record = ProductIteration(
                    brand=None,
                    category=category,
                    product_type=None,
                    product_group_id=product_group_id,
                    algorithm=algorithm,
                    eps=eps,
                    sample=min_samples,
                    unique_number=unique_number,
                    total_items=stats['total_items'],
                    analyzed_items=stats['analyzed_items'],
                    pending_items=stats['pending_items'],
                    outlier_items=outlier_items,
                    timestamp=datetime.now()
                )
                iteration_records.append(iteration_record)
                iteration_mapping[(category, eps, min_samples)] = len(iteration_records) - 1
        
        # Batch insert iterations
        db.add_all(iteration_records)
        db.flush()  # Get IDs without committing
        
        # Batch create item records with optimized processing
        item_records = []
        batch_size = 1000  # Process in batches to avoid memory issues
        
        # Pre-calculate total items per config per category to avoid redundant calculations
        config_totals = {}
        for category in categories:
            for eps, min_samples in config:
                col_name = f"{eps} X {min_samples}"
                total_items_in_config = len([r for r in results if r['Category'] == category and r.get(f"{col_name} Status") != '-'])
                config_totals[(category, col_name)] = total_items_in_config
        
        for result in results:
            category = result['Category']
            
            for eps, min_samples in config:
                col_name = f"{eps} X {min_samples}"
                if result.get(f"{col_name} Status") != '-':
                    # Get iteration record
                    iteration_idx = iteration_mapping[(category, eps, min_samples)]
                    iteration_id = iteration_records[iteration_idx].iteration_id
                    
                    # Determine cluster, outlier_mode, and status from DBSCAN results
                    status_val = result.get(f"{col_name} Status", '-')
                    cluster_val = result.get(f"{col_name} Cluster", None)
                    cluster_items_val = result.get(f"{col_name} Cluster Item Count", 0)
                    outlier_mode_val = None
                    final_status_val = None
                    
                    if status_val == 0:  # Outlier
                        outlier_mode_val = 0  # Auto outlier
                    elif status_val == 1:  # Normal
                        outlier_mode_val = None
                    
                    # Calculate cluster_items_per using pre-calculated totals (OPTIMIZED)
                    cluster_items_per_val = 0.0
                    if cluster_items_val and cluster_items_val != '-':
                        total_items_in_config = config_totals.get((category, col_name), 0)
                        if total_items_in_config > 0:
                            cluster_items_per_val = (int(cluster_items_val) / total_items_in_config * 100)
                    
                    item_record = PricingProductIterationItem(
                        iteration_id=iteration_id,
                        system_product_id=result['System Product Id'],
                        brand=result['Brand'],
                        category=result['Category'],
                        product_type=result['Product Type'],
                        cluster=str(cluster_val) if cluster_val is not None else None,
                        cluster_items=int(cluster_items_val) if cluster_items_val != '-' else 0,
                        cluster_items_per=cluster_items_per_val,
                        outlier_mode=outlier_mode_val,
                        status=status_val if status_val != '-' else None,
                        final_status=final_status_val,
                        analyzed_date=None
                    )
                    item_records.append(item_record)
                    
                    # Batch insert when reaching batch size
                    if len(item_records) >= batch_size:
                        db.add_all(item_records)
                        db.flush()
                        item_records = []
                        print(f"Batch inserted {batch_size} items...")
        
        # Insert remaining items
        if item_records:
            db.add_all(item_records)
            print(f"Final batch inserted {len(item_records)} items...")
        
        db.commit()
        print(f"Batch saved {len(iteration_records)} iterations and all items for unique_number: {unique_number}")
        
    except Exception as e:
        db.rollback()
        print(f"Error in save_analysis_to_database_optimized: {e}")
        import traceback
        traceback.print_exc()
        raise e
    finally:
        db.close()
