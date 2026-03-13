import time
import logging
from datetime import datetime
from models.base.base import SessionLocal
from services.item_match.matcher import ItemMatchService

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def bulk_analyze(brands=None, categories=None, types=None, algorithm_id='tfidf'):
    """
    Bulk analyze all matching items without UI updates.
    Runs silently and logs execution time.
    """
    start_time = time.time()
    msg = f"[BULK ANALYZE] Started at {datetime.now()}"
    print(msg)
    logger.info(msg)
    
    msg = f"[BULK ANALYZE] Filters - Brands: {brands}, Categories: {categories}, Types: {types}, Algorithm: {algorithm_id}"
    print(msg)
    logger.info(msg)
    
    try:
        from services.item_match.algorithms import TfidfAlgorithm, CustomAlgorithm, TfidfPriceAlgorithm
        from services.item_match.attribute_service import AttributeService
        from sqlalchemy import text
        
        # Get algorithm instance
        if algorithm_id == 'tfidf':
            algorithm = TfidfAlgorithm()
        elif algorithm_id == 'custom':
            algorithm = CustomAlgorithm()
        elif algorithm_id == 'tfidf_price':
            algorithm = TfidfPriceAlgorithm()
        else:
            algorithm = TfidfAlgorithm()
        
        msg = f"[BULK ANALYZE] Loading products with filters"
        print(msg)
        logger.info(msg)
        
        db = SessionLocal()
        
        # Load attribute configuration from database
        attr_service = AttributeService(db)
        all_attrs = attr_service.get_attributes_by_type('default')
        
        # Build attribute mapping
        attr_map = {}
        competitor_groups = {}
        for attr in all_attrs:
            attr_map[attr.attribute_name] = {
                'id': attr.attribute_id,
                'competitor_attribute': attr.competitor_attribute,
                'data_type': attr.data_type,
                'weightage': float(attr.default_weightage)
            }
            # Group by competitor_attribute
            comp_attr = attr.competitor_attribute
            if comp_attr:
                if comp_attr not in competitor_groups:
                    competitor_groups[comp_attr] = []
                competitor_groups[comp_attr].append(attr.attribute_name)
        
        msg = f"[BULK ANALYZE] Loaded {len(attr_map)} attributes with {len(competitor_groups)} competitor groups"
        print(msg)
        logger.info(msg)
        
        conditions = []
        params = {}
        
        if brands and len(brands) > 0:
            placeholders = ','.join([f':b{i}' for i in range(len(brands))])
            conditions.append(f"msp.brand IN ({placeholders})")
            for i, b in enumerate(brands):
                params[f'b{i}'] = b
        
        if categories and len(categories) > 0:
            placeholders = ','.join([f':c{i}' for i in range(len(categories))])
            conditions.append(f"msp.category IN ({placeholders})")
            for i, c in enumerate(categories):
                params[f'c{i}'] = c
        
        if types and len(types) > 0:
            placeholders = ','.join([f':t{i}' for i in range(len(types))])
            conditions.append(f"msp.product_type IN ({placeholders})")
            for i, t in enumerate(types):
                params[f't{i}'] = t
        
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        
        # Delete existing scores for filtered products
        msg = f"[BULK ANALYZE] Deleting existing scores"
        print(msg)
        logger.info(msg)
        
        if where_clause == "1=1":
            # No filters - delete ALL scores
            db.execute(text("DELETE FROM matching_score_attributes"))
            db.execute(text("DELETE FROM matching_scores"))
        else:
            # Delete attribute scores first
            delete_attrs_query = text(f"""
                DELETE msa FROM matching_score_attributes msa
                INNER JOIN matching_scores ms ON msa.score_id = ms.score_id
                INNER JOIN matching_system_product msp ON ms.system_product_id = msp.system_product_id
                WHERE {where_clause}
            """)
            db.execute(delete_attrs_query, params)
            
            # Then delete main scores
            delete_scores_query = text(f"""
                DELETE ms FROM matching_scores ms
                INNER JOIN matching_system_product msp ON ms.system_product_id = msp.system_product_id
                WHERE {where_clause}
            """)
            db.execute(delete_scores_query, params)
        
        db.commit()
        
        msg = f"[BULK ANALYZE] Loading ALL products and competitors in single query"
        print(msg)
        logger.info(msg)
        
        # Build dynamic SELECT for system product columns based on attributes
        system_cols = ['msp.product_id', 'msp.system_product_id']
        for attr_name in attr_map.keys():
            system_cols.append(f'msp.{attr_name}')
        
        # Build dynamic SELECT for competitor columns based on competitor_attributes
        comp_cols = ['mcp.competitor_product_id']
        for comp_attr in competitor_groups.keys():
            comp_cols.append(f'mcp.{comp_attr}')
        
        # Single query with JOIN to get all products + competitors
        query = text(f"""
            SELECT {', '.join(system_cols)}, {', '.join(comp_cols)}
            FROM matching_system_product msp
            INNER JOIN matching_competitor_product mcp ON msp.system_product_id = mcp.system_product_id
            WHERE {where_clause}
        """)
        
        result = db.execute(query, params) if params else db.execute(query)
        rows = result.fetchall()
        total_rows = len(rows)
        
        msg = f"[BULK ANALYZE] Found {total_rows} product-competitor pairs to analyze"
        print(msg)
        logger.info(msg)
        
        # Calculate ALL scores in memory first
        msg = f"[BULK ANALYZE] Calculating scores in memory"
        print(msg)
        logger.info(msg)
        
        all_scores = []
        all_attrs = []
        
        # Calculate column indices
        sys_col_count = len(system_cols)
        
        for row in rows:
            # Extract product IDs
            product_id = row[0]
            sys_product_id = row[1]
            
            # Extract system product values
            sys_values = {}
            for idx, attr_name in enumerate(attr_map.keys()):
                sys_values[attr_name] = row[2 + idx]
            
            # Extract competitor values
            comp_values = {}
            for idx, comp_attr in enumerate(competitor_groups.keys()):
                comp_values[comp_attr] = row[sys_col_count + idx]
            
            comp_id = row[sys_col_count]  # First comp column is competitor_product_id
            
            try:
                # Calculate scores for each competitor attribute group
                attr_scores = {}
                processed_comp_attrs = set()
                
                for attr_name, attr_info in attr_map.items():
                    comp_attr = attr_info['competitor_attribute']
                    data_type = attr_info['data_type']
                    attr_id = attr_info['id']
                    
                    # Skip if already processed
                    if comp_attr in processed_comp_attrs:
                        continue
                    
                    # Get all system attributes for this competitor attribute
                    system_attrs = competitor_groups.get(comp_attr, [attr_name])
                    comp_val = comp_values.get(comp_attr, '')
                    
                    # Calculate scores for all system attributes
                    group_scores = []
                    for sys_attr in system_attrs:
                        if sys_attr not in sys_values:
                            continue
                        
                        sys_val = sys_values[sys_attr]
                        
                        # Choose comparison method based on data_type
                        if data_type == 'number':
                            score_val = algorithm.score_price(sys_val, comp_val) if hasattr(algorithm, 'score_price') else 0
                        elif data_type == 'url':
                            score_val = algorithm.score_url(sys_val, comp_val) if hasattr(algorithm, 'score_url') else algorithm.score(sys_val, comp_val)
                        else:  # string
                            score_val = algorithm.score_sku(sys_val, comp_val) if hasattr(algorithm, 'score_sku') else algorithm.score(sys_val, comp_val)
                        
                        group_scores.append(score_val)
                    
                    # Pick BEST score from group
                    best_score = max(group_scores) if group_scores else 0.0
                    
                    # Store with first attribute ID in group
                    first_attr_id = attr_map[system_attrs[0]]['id'] if system_attrs else attr_id
                    attr_scores[first_attr_id] = best_score
                    
                    processed_comp_attrs.add(comp_attr)
                
                # Calculate total score using weights
                total_weight = 0
                weighted_sum = 0
                for attr_name, attr_info in attr_map.items():
                    attr_id = attr_info['id']
                    if attr_id in attr_scores:
                        weighted_sum += attr_scores[attr_id] * attr_info['weightage']
                        total_weight += attr_info['weightage']
                
                total_score = (weighted_sum / total_weight) if total_weight > 0 else 0.0
                
                if total_score >= 89:
                    status = 'Matched'
                elif total_score >= 70:
                    status = 'Review'
                else:
                    status = 'Not Matched'
                
                all_scores.append({
                    'system_product_id': product_id,
                    'competitor_product_id': comp_id,
                    'algorithm_id': algorithm_id,
                    'total_score': total_score,
                    'score_status': status
                })
                
                # Store attributes for later insert
                all_attrs.append({
                    'system_product_id': product_id,
                    'competitor_product_id': comp_id,
                    'algorithm_id': algorithm_id,
                    'attr_scores': attr_scores
                })
            except Exception as e:
                msg = f"[BULK ANALYZE] Error calculating score for product {product_id}, competitor {comp_id}: {str(e)}"
                logger.error(msg)
                continue
        
        msg = f"[BULK ANALYZE] Calculated {len(all_scores)} scores, inserting in batches of 10000"
        print(msg)
        logger.info(msg)
        
        # Bulk insert in batches of 10000 records
        batch_size = 10000
        total_inserted = 0
        
        for i in range(0, len(all_scores), batch_size):
            batch_scores = all_scores[i:i+batch_size]
            batch_attrs = all_attrs[i:i+batch_size]
            
            msg = f"[BULK ANALYZE] Inserting batch {i//batch_size + 1} ({len(batch_scores)} records)"
            print(msg)
            logger.info(msg)
            
            try:
                # Insert or update scores
                db.execute(text("""
                    INSERT INTO matching_scores (system_product_id, competitor_product_id, algorithm_id, total_score, score_status)
                    VALUES (:system_product_id, :competitor_product_id, :algorithm_id, :total_score, :score_status)
                    ON DUPLICATE KEY UPDATE 
                        algorithm_id = VALUES(algorithm_id),
                        total_score = VALUES(total_score),
                        score_status = VALUES(score_status)
                """), batch_scores)
                
                # Build attribute insert values dynamically
                attr_values = []
                for attr in batch_attrs:
                    for attr_id, score_val in attr['attr_scores'].items():
                        attr_values.append({
                            'system_product_id': attr['system_product_id'],
                            'competitor_product_id': attr['competitor_product_id'],
                            'attribute_id': attr_id,
                            'algorithm_id': attr['algorithm_id'],
                            'score': score_val
                        })
                
                # Insert attributes with score_id lookup
                db.execute(text("""
                    INSERT INTO matching_score_attributes (score_id, system_product_id, competitor_product_id, attribute_id, algorithm_id, score)
                    SELECT ms.score_id, :system_product_id, :competitor_product_id, :attribute_id, :algorithm_id, :score
                    FROM matching_scores ms
                    WHERE ms.system_product_id = :system_product_id AND ms.competitor_product_id = :competitor_product_id
                """), attr_values)
                
                db.commit()
                total_inserted += len(batch_scores)
            except Exception as e:
                db.rollback()
                msg = f"[BULK ANALYZE] Error inserting batch: {str(e)}"
                print(msg)
                logger.error(msg)
                raise
        
        db.close()
        
        end_time = time.time()
        elapsed = end_time - start_time
        
        msg = f"[BULK ANALYZE] Completed at {datetime.now()}"
        print(msg)
        logger.info(msg)
        
        msg = f"[BULK ANALYZE] Total time: {elapsed:.2f} seconds ({elapsed/60:.2f} minutes)"
        print(msg)
        logger.info(msg)
        
        msg = f"[BULK ANALYZE] Processed: {len(all_scores)} records"
        print(msg)
        logger.info(msg)
        
        if len(all_scores) > 0:
            msg = f"[BULK ANALYZE] Average time per record: {elapsed/len(all_scores):.4f} seconds"
        else:
            msg = "[BULK ANALYZE] No records processed"
        print(msg)
        logger.info(msg)
        
        return {
            'success': True,
            'total': len(all_scores),
            'processed': len(all_scores),
            'time_seconds': round(elapsed, 2),
            'time_minutes': round(elapsed/60, 2)
        }
        
    except Exception as e:
        msg = f"[BULK ANALYZE] Fatal error: {str(e)}"
        print(msg)
        logger.error(msg)
        import traceback
        traceback.print_exc()
        return {'success': False, 'error': str(e)}
