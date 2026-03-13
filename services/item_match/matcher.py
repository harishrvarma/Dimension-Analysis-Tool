from models.base.base import SessionLocal
from repositories.dimension.product_repository import ProductRepository
import pandas as pd
import numpy as np
import re
from sqlalchemy import text
from services.item_match.algorithms import AlgorithmFactory
from services.item_match.attribute_service import AttributeService
from services.item_match.score_service import ScoreService
from services.item_match.configuration_service import ConfigurationService


class ItemMatcher:
    """Multi-algorithm item matching"""
    
    STATUS_MATCHED = "Matched"
    STATUS_REVIEW = "Review"
    STATUS_NOT_MATCHED = "Not Matched"
    
    def __init__(self, algorithms=None, attributes=None, weights=None, thresholds=None, price_config=None, session=None):
        self.session = session or SessionLocal()
        self.attr_service = AttributeService(self.session)
        
        # Get all attributes from DB
        all_attrs = self.attr_service.get_all_attributes()
        
        # Separate by type
        self.scoring_attrs = [a for a in all_attrs if a.attribute_type == 'default']
        self.price_attrs = {a.attribute_name: a.default_weightage for a in all_attrs if a.attribute_type == 'price'}
        self.status_attrs = {a.attribute_name: a.default_weightage for a in all_attrs if a.attribute_type == 'status'}
        
        # Build attr_dict for ALL scoring attributes (not filtered by selection)
        self.all_attr_dict = {a.attribute_name: {
            'id': a.attribute_id, 
            'weightage': a.default_weightage, 
            'type': a.attribute_type,
            'competitor_attribute': a.competitor_attribute,
            'data_type': a.data_type
        } for a in self.scoring_attrs}
        
        # Store which attributes are SELECTED for final score calculation
        self.selected_attributes = attributes or list(self.all_attr_dict.keys())
        
        # Build attr_dict for SELECTED attributes only (for final score calculation)
        self.attr_dict = {k: v for k, v in self.all_attr_dict.items() if k in self.selected_attributes}
        
        # Group ALL attributes by competitor_attribute for dynamic comparison
        self.competitor_groups = {}
        for attr_name, attr_info in self.all_attr_dict.items():
            comp_attr = attr_info['competitor_attribute']
            if comp_attr:
                if comp_attr not in self.competitor_groups:
                    self.competitor_groups[comp_attr] = []
                self.competitor_groups[comp_attr].append(attr_name)
        
        # Group SELECTED attributes by competitor_attribute for final score calculation
        self.selected_competitor_groups = {}
        for attr_name, attr_info in self.attr_dict.items():
            comp_attr = attr_info['competitor_attribute']
            if comp_attr:
                if comp_attr not in self.selected_competitor_groups:
                    self.selected_competitor_groups[comp_attr] = []
                self.selected_competitor_groups[comp_attr].append(attr_name)
        
        self.attributes = list(self.attr_dict.keys())
        self.all_attributes = list(self.all_attr_dict.keys())
        self.algorithms = algorithms or ['tfidf']
        self.weights = weights or {k: v['weightage'] for k, v in self.attr_dict.items()}
        
        # Use price config from DB or override
        self.price_config = price_config or self.price_attrs
        
        # Use thresholds from DB or override
        self.thresholds = thresholds or self.status_attrs
        
        self.algo_instances = {algo: AlgorithmFactory.get_algorithm(algo, self.price_config) for algo in self.algorithms}
    
    def calculate_score(self, prod_data, comp_data):
        scores = {}
        
        # STEP 1: Calculate scores for ALL attributes (not just selected ones)
        for attr_name, attr_info in self.all_attr_dict.items():
            comp_attr = attr_info['competitor_attribute']
            data_type = attr_info['data_type']
            
            # Get competitor value
            comp_val = comp_data.get(comp_attr, '')
            prod_val = prod_data.get(attr_name, '')
            
            # Calculate score for THIS attribute
            for algo_name in self.algorithms:
                algo = self.algo_instances.get(algo_name)
                if not algo:
                    continue
                
                try:
                    # Choose comparison method based on data_type
                    if data_type == 'number':
                        if algo_name in ['custom', 'tfidf_price']:
                            score_val = algo.score_price(prod_val, comp_val)
                        else:
                            score_val = algo.score(str(prod_val), str(comp_val))
                    elif data_type == 'url':
                        if algo_name in ['custom', 'tfidf_price']:
                            score_val = algo.score_url(prod_val, comp_val)
                        else:
                            score_val = algo.score(str(prod_val), str(comp_val))
                    else:
                        # String comparison (default)
                        if algo_name in ['custom', 'tfidf_price']:
                            score_val = algo.score_sku(prod_val, comp_val)
                        else:
                            score_val = algo.score(str(prod_val), str(comp_val))
                    
                    scores[f'{attr_name}_{algo_name}'] = score_val
                except Exception as e:
                    print(f"Error scoring {attr_name} with {algo_name}: {type(e).__name__}: {e}")
                    scores[f'{attr_name}_{algo_name}'] = 0.0
        
        # STEP 2: Calculate final score using ONLY SELECTED attributes
        total_weight = 0
        weighted_sum = 0
        processed_comp_attrs = set()
        perfect_match_found = False
        
        print(f"[CALCULATE_SCORE] Starting final score calculation with SELECTED attributes: {self.selected_attributes}")
        print(f"[CALCULATE_SCORE] All calculated scores: {scores}")
        
        # Only iterate over SELECTED attributes for final score
        for attr_name, attr_info in self.attr_dict.items():
            comp_attr = attr_info['competitor_attribute']
            
            # Skip if this competitor attribute group already processed
            if comp_attr in processed_comp_attrs:
                continue
            
            # Get all SELECTED system attributes that map to this competitor attribute
            system_attrs = self.selected_competitor_groups.get(comp_attr, [attr_name])
            
            # Collect scores from all SELECTED attributes in this group
            group_scores = []
            for sys_attr in system_attrs:
                if sys_attr not in self.attr_dict:
                    continue
                attr_scores = [v for k, v in scores.items() if k.startswith(f'{sys_attr}_')]
                if attr_scores:
                    avg_score = sum(attr_scores) / len(attr_scores)
                    group_scores.append(avg_score)
            
            # Use BEST score from the group
            if group_scores:
                best_score = max(group_scores)
                
                # Check for perfect match (100%)
                if best_score >= 100.0:
                    perfect_match_found = True
                    print(f"[CALCULATE_SCORE] Perfect match found in {comp_attr}: {best_score}% - Setting final score to 100%")
                
                # Apply weight of the FIRST SELECTED attribute in the group
                first_attr = system_attrs[0] if system_attrs else attr_name
                weight = self.weights.get(first_attr, 0)
                weighted_sum += best_score * weight
                total_weight += weight
                print(f"[CALCULATE_SCORE] {comp_attr}: best_score={best_score}, weight={weight}, contribution={best_score * weight}")
            
            processed_comp_attrs.add(comp_attr)
        
        # If perfect match found, set final score to 100%
        if perfect_match_found:
            final_score = 100.0
            print(f"[CALCULATE_SCORE] Perfect match detected - Final score set to 100%")
        else:
            final_score = (weighted_sum / total_weight) if total_weight > 0 else 0.0
            print(f"[CALCULATE_SCORE] Final calculation: {weighted_sum} / {total_weight} = {final_score}")
        
        # If all individual attribute scores are 0, set final score to 0
        all_attr_scores = [v for k, v in scores.items() if not k.endswith('_final_score')]
        if all_attr_scores and all(score == 0 for score in all_attr_scores):
            final_score = 0.0
        
        scores['final_score'] = round(final_score, 2)
        
        return {k: round(v, 2) for k, v in scores.items()}
    
    def get_status(self, score: float) -> str:
        matched_threshold = self.thresholds.get('matched', 85) or 85
        review_threshold = self.thresholds.get('review', 70) or 70
        
        if score >= matched_threshold:
            return self.STATUS_MATCHED
        elif score >= review_threshold:
            return self.STATUS_REVIEW
        else:
            return self.STATUS_NOT_MATCHED
    
    def match_items(self, products_df: pd.DataFrame, competitors_df: pd.DataFrame):
        print(f"[ANALYZE] Starting analysis for {len(products_df)} products")
        print(f"[ANALYZE] Products columns: {products_df.columns.tolist()}")
        print(f"[ANALYZE] Competitors columns: {competitors_df.columns.tolist()}")
        
        # Merge products with competitors
        merged = products_df.merge(
            competitors_df,
            left_on='system_product_id',
            right_on='matching_product_id',
            how='inner'
        )
        
        if merged.empty:
            print(f"[ANALYZE] No matches found")
            return pd.DataFrame()
        
        print(f"[ANALYZE] Merged columns: {merged.columns.tolist()}")
        print(f"[ANALYZE] Calculating scores for {len(merged)} product-competitor pairs")
        
        # Calculate scores vectorized
        scores_list = []
        for idx, row in merged.iterrows():
            # Pass ALL attributes to calculate_score (not just selected ones)
            prod_data = {attr: row.get(attr, '') for attr in self.all_attributes}
            comp_data = {attr: row.get(f"competitor_{attr}", '') for attr in self.all_attributes}
            result = self.calculate_score(prod_data, comp_data)
            final_score = result['final_score']
            scores_list.append(final_score)
            
            if (idx + 1) % 10000 == 0:
                print(f"[ANALYZE] Scored {idx + 1}/{len(merged)} pairs")
        
        merged['score'] = scores_list
        merged['status'] = merged['score'].apply(self.get_status)
        
        print(f"[ANALYZE] Aggregating results")
        
        # Aggregate separately to avoid pandas issues
        grouped = merged.groupby('system_product_id')
        
        # Start with mandatory columns
        summary_data = {
            'product_id': grouped.size().index,
            'max_score': grouped['score'].max(),
            'total_items': grouped['score'].count()
        }
        
        # Add columns only if they exist in merged data
        if 'sku' in merged.columns:
            summary_data['sku'] = grouped['sku'].first()
        if 'part_number' in merged.columns:
            summary_data['part_number'] = grouped['part_number'].first()
        if 'brand' in merged.columns:
            summary_data['brand'] = grouped['brand'].first()
        if 'category' in merged.columns:
            summary_data['category'] = grouped['category'].first()
        if 'product_type' in merged.columns:
            summary_data['product_type'] = grouped['product_type'].first()
        if 'name' in merged.columns:
            summary_data['name'] = grouped['name'].first()
        
        summary = pd.DataFrame(summary_data)
        
        # Calculate status counts
        summary['matched_count'] = grouped['status'].apply(lambda x: (x == self.STATUS_MATCHED).sum())
        summary['review_count'] = grouped['status'].apply(lambda x: (x == self.STATUS_REVIEW).sum())
        summary['not_matched_count'] = grouped['status'].apply(lambda x: (x == self.STATUS_NOT_MATCHED).sum())
        summary['status'] = summary['max_score'].apply(self.get_status)
        
        # Ensure max_score is properly formatted as float with 2 decimal places
        summary['max_score'] = summary['max_score'].round(2).astype(float)
        
        # Handle any NaN values that might cause issues
        summary['max_score'] = summary['max_score'].fillna(0.0)
        
        summary = summary.reset_index(drop=True)
        
        # Debug: Print a few sample max_score values to verify they're not 0
        if len(summary) > 0:
            print(f"[SUMMARY DEBUG] Sample max_score values: {summary['max_score'].head(5).tolist()}")
            print(f"[SUMMARY DEBUG] Max score range: {summary['max_score'].min()} - {summary['max_score'].max()}")
        
        print(f"[ANALYZE] Completed analysis for {len(summary)} products")
        return summary


class ItemMatchService:
    def __init__(self):
        self.session = SessionLocal()
        self.product_repo = ProductRepository(self.session)
        self.attr_service = AttributeService(self.session)
        self.score_service = ScoreService(self.session)
        self.config_service = ConfigurationService(self.session)
    
    def __del__(self):
        if hasattr(self, 'session'):
            self.session.close()
    
    def get_filter_options(self, brands=None, categories=None):
        try:
            conn = self.session.connection()
            
            # Get brands with score status
            brand_query = text("""SELECT msp.brand, 
                COUNT(DISTINCT msp.product_id) as total_count,
                COUNT(DISTINCT CASE WHEN ms.total_score IS NOT NULL THEN msp.product_id END) as scored_count
                FROM matching_system_product msp 
                 
                JOIN matching_competitor_product mcp ON msp.system_product_id = mcp.system_product_id 
                LEFT JOIN matching_scores ms ON msp.system_product_id = ms.system_product_id
                WHERE msp.brand IS NOT NULL AND msp.brand != '' 
                GROUP BY msp.brand ORDER BY msp.brand""")
            brand_result = conn.execute(brand_query)
            brands_list = []
            for row in brand_result:
                status = 'complete' if row[2] == row[1] and row[2] > 0 else ('partial' if row[2] > 0 else 'none')
                brands_list.append({'label': f"{row[0]} ({row[1]})", 'value': row[0], 'status': status})
            
            # Get categories with score status
            if brands and len(brands) > 0:
                placeholders = ','.join([':b' + str(i) for i in range(len(brands))])
                cat_query = text(f"""SELECT msp.category, 
                    COUNT(DISTINCT msp.product_id) as total_count,
                    COUNT(DISTINCT CASE WHEN ms.total_score IS NOT NULL THEN msp.product_id END) as scored_count
                    FROM matching_system_product msp 
                     
                    JOIN matching_competitor_product mcp ON msp.system_product_id = mcp.system_product_id 
                    LEFT JOIN matching_scores ms ON msp.system_product_id = ms.system_product_id
                    WHERE msp.brand IN ({placeholders}) AND msp.category IS NOT NULL AND msp.category != '' 
                    GROUP BY msp.category ORDER BY msp.category""")
                cat_result = conn.execute(cat_query, {f'b{i}': b for i, b in enumerate(brands)})
            else:
                cat_query = text("""SELECT msp.category, 
                    COUNT(DISTINCT msp.product_id) as total_count,
                    COUNT(DISTINCT CASE WHEN ms.total_score IS NOT NULL THEN msp.product_id END) as scored_count
                    FROM matching_system_product msp 
                     
                    JOIN matching_competitor_product mcp ON msp.system_product_id = mcp.system_product_id 
                    LEFT JOIN matching_scores ms ON msp.system_product_id = ms.system_product_id
                    WHERE msp.category IS NOT NULL AND msp.category != '' 
                    GROUP BY msp.category ORDER BY msp.category""")
                cat_result = conn.execute(cat_query)
            categories_list = []
            for row in cat_result:
                status = 'complete' if row[2] == row[1] and row[2] > 0 else ('partial' if row[2] > 0 else 'none')
                categories_list.append({'label': f"{row[0]} ({row[1]})", 'value': row[0], 'status': status})
            
            # Get types with score status
            type_conditions = []
            type_params = {}
            if brands and len(brands) > 0:
                placeholders = ','.join([':b' + str(i) for i in range(len(brands))])
                type_conditions.append(f"msp.brand IN ({placeholders})")
                for i, b in enumerate(brands):
                    type_params[f'b{i}'] = b
            if categories and len(categories) > 0:
                placeholders = ','.join([':c' + str(i) for i in range(len(categories))])
                type_conditions.append(f"msp.category IN ({placeholders})")
                for i, c in enumerate(categories):
                    type_params[f'c{i}'] = c
            
            if type_conditions:
                type_where = " AND ".join(type_conditions)
                type_query = text(f"""SELECT msp.product_type, 
                    COUNT(DISTINCT msp.product_id) as total_count,
                    COUNT(DISTINCT CASE WHEN ms.total_score IS NOT NULL THEN msp.product_id END) as scored_count
                    FROM matching_system_product msp 
                     
                    JOIN matching_competitor_product mcp ON msp.system_product_id = mcp.system_product_id 
                    LEFT JOIN matching_scores ms ON msp.system_product_id = ms.system_product_id
                    WHERE {type_where} AND msp.product_type IS NOT NULL AND msp.product_type != '' 
                    GROUP BY msp.product_type ORDER BY msp.product_type""")
                type_result = conn.execute(type_query, type_params)
            else:
                type_query = text("""SELECT msp.product_type, 
                    COUNT(DISTINCT msp.product_id) as total_count,
                    COUNT(DISTINCT CASE WHEN ms.total_score IS NOT NULL THEN msp.product_id END) as scored_count
                    FROM matching_system_product msp 
                     
                    JOIN matching_competitor_product mcp ON msp.system_product_id = mcp.system_product_id 
                    LEFT JOIN matching_scores ms ON msp.system_product_id = ms.system_product_id
                    WHERE msp.product_type IS NOT NULL AND msp.product_type != '' 
                    GROUP BY msp.product_type ORDER BY msp.product_type""")
                type_result = conn.execute(type_query)
            types_list = []
            for row in type_result:
                status = 'complete' if row[2] == row[1] and row[2] > 0 else ('partial' if row[2] > 0 else 'none')
                types_list.append({'label': f"{row[0]} ({row[1]})", 'value': row[0], 'status': status})
            
            return {
                'brands': brands_list,
                'categories': categories_list,
                'types': types_list
            }
        except Exception as e:
            print(f"Error in get_filter_options: {e}")
            import traceback
            traceback.print_exc()
            return {'brands': [], 'categories': [], 'types': []}
    
    def get_counts(self, brands=None, categories=None, types=None):
        try:
            conn = self.session.connection()
            conditions = []
            params = {}
            
            if brands and len(brands) > 0:
                placeholders = ','.join([':b' + str(i) for i in range(len(brands))])
                conditions.append(f"msp.brand IN ({placeholders})")
                for i, b in enumerate(brands):
                    params[f'b{i}'] = b
            if categories and len(categories) > 0:
                placeholders = ','.join([':c' + str(i) for i in range(len(categories))])
                conditions.append(f"msp.category IN ({placeholders})")
                for i, c in enumerate(categories):
                    params[f'c{i}'] = c
            if types and len(types) > 0:
                placeholders = ','.join([':t' + str(i) for i in range(len(types))])
                conditions.append(f"msp.product_type IN ({placeholders})")
                for i, t in enumerate(types):
                    params[f't{i}'] = t
            
            where_clause = " AND ".join(conditions) if conditions else "1=1"
            count_query = text(f"""SELECT COUNT(DISTINCT msp.product_id) 
                FROM matching_system_product msp 
                 
                JOIN matching_competitor_product mcp ON msp.system_product_id = mcp.system_product_id 
                WHERE {where_clause}""")
            
            result = conn.execute(count_query, params) if params else conn.execute(count_query)
            product_count = result.fetchone()[0]
            
            return {
                'product_count': product_count,
                'competitor_count': 0
            }
        except Exception as e:
            print(f"Error in get_counts: {e}")
            import traceback
            traceback.print_exc()
            return {'product_count': 0, 'competitor_count': 0}
    
    def run_analysis(self, brands=None, categories=None, types=None, algorithms=None, attributes=None, weights=None, thresholds=None, price_config=None, save_score=False, save_top_most=False, hard_refresh=False, product_ids=None):
        # Get only scoring attributes (type='default')
        if not attributes:
            scoring_attrs = self.attr_service.get_attributes_by_type('default')
            attributes = [a.attribute_name for a in scoring_attrs]
        
        # Load products first to get filtered product IDs
        products_df = self._load_products(brands, categories, types, pd.DataFrame(), attributes, product_ids)
        if products_df.empty:
            return {'error': 'No products found'}
        
        # Load competitors only for the filtered products
        competitors_df = self._load_competitors(attributes, products_df['system_product_id'].tolist())
        if competitors_df.empty:
            return {'error': 'No competitor data available'}
        
        matcher = ItemMatcher(algorithms=algorithms, attributes=attributes, weights=weights, thresholds=thresholds, price_config=price_config, session=self.session)
        summary = matcher.match_items(products_df, competitors_df)
        
        if summary.empty:
            return {'error': 'No products with competitors found'}
        
        if save_score or save_top_most or hard_refresh:
            # Always delete existing scores for current algorithm and filtered products before saving new ones
            if len(products_df) > 0:
                conn = self.session.connection()
                product_ids = products_df['product_id'].tolist()
                placeholders = ','.join([':p' + str(i) for i in range(len(product_ids))])
                params = {f'p{i}': pid for i, pid in enumerate(product_ids)}
                
                # Always reset review_status for products being analyzed
                reset_query = text(f"UPDATE matching_system_product SET review_status = 0, competitor_product_id = NULL, matched_date = NULL WHERE product_id IN ({placeholders})")
                conn.execute(reset_query, params)
                
                # NO DELETION - ScoreService handles upsert logic
                # algorithm_id = matcher.algorithms[0] if matcher.algorithms else 'tfidf'
                # params['algo_id'] = algorithm_id
                # delete_attrs_query = text(f"DELETE msa FROM matching_score_attributes msa JOIN matching_scores ms ON msa.score_id = ms.score_id WHERE ms.system_product_id IN ({placeholders}) AND msa.algorithm_id = :algo_id")
                # conn.execute(delete_attrs_query, params)
                # delete_scores_query = text(f"DELETE ms FROM matching_scores ms WHERE ms.system_product_id IN ({placeholders}) AND NOT EXISTS (SELECT 1 FROM matching_score_attributes msa WHERE msa.score_id = ms.score_id)")
                # conn.execute(delete_scores_query, {f'p{i}': pid for i, pid in enumerate(product_ids)})
                
                self.session.commit()
            
            self._save_scores(products_df, competitors_df, matcher, save_top_most, reset_on_first=False, hard_refresh=hard_refresh)
        
        return {
            'summary': summary.to_dict('records'),
            'algorithms': algorithms or ['tfidf'],
            'attributes': attributes,
            'stats': {
                'total': len(summary),
                'matched': int((summary['status'] == 'Matched').sum()),
                'review': int((summary['status'] == 'Review').sum()),
                'not_matched': int((summary['status'] == 'Not Matched').sum())
            }
        }
    
    def get_item_comparison_details(self, product_id, algorithms=None, attributes=None, weights=None, thresholds=None, price_config=None):
        conn = self.session.connection()
        
        # Get ALL attributes for fetching and calculation
        all_scoring_attrs = self.attr_service.get_attributes_by_type('default')
        all_attr_names = [a.attribute_name for a in all_scoring_attrs]
        
        # Use selected attributes for display and final score
        selected_attributes = attributes or all_attr_names
        
        # Build dynamic SELECT for product - fetch ALL attributes
        prod_cols = ["msp.product_id", "msp.system_product_id", "msp.name", "msp.brand", "msp.category", "msp.product_type", "msp.part_number", "msp.competitor_product_id", "msp.review_status"]
        for attr in all_attr_names:
            prod_cols.append(f"msp.{attr}")
        
        query = text(f"SELECT {', '.join(prod_cols)} FROM matching_system_product msp  WHERE msp.system_product_id = :pid")
        result = conn.execute(query, {'pid': product_id})
        row = result.fetchone()
        
        if not row:
            return {'error': 'Product not found'}
        
        prod = {
            'product_id': row[0],
            'system_product_id': row[1],
            'name': row[2],
            'brand': row[3],
            'category': row[4],
            'product_type': row[5],
            'part_number': row[6],
            'competitor_product_id': row[7],
            'review_status': row[8]
        }
        
        # Add ALL attributes - keep original URLs for display, normalize for comparison
        for i, attr in enumerate(all_attr_names):
            val = row[9 + i]
            if attr == 'url':
                prod['url_display'] = val  # Original for display
                prod[attr] = self._normalize_url_for_comparison(val)  # Normalized for scoring
            else:
                prod[attr] = val
        
        # Build dynamic SELECT for competitors - fetch ALL attributes
        comp_cols = ["competitor_product_id", "system_product_id", "competitor_id", "COALESCE(part_number, '')"]
        for attr in all_attr_names:
            comp_cols.append(f"COALESCE({attr}, '')")
        
        comp_query = text(f"SELECT {', '.join(comp_cols)} FROM matching_competitor_product WHERE system_product_id = :pid")
        comp_result = conn.execute(comp_query, {'pid': prod['system_product_id']})
        
        # Create matcher with SELECTED attributes for final score calculation
        matcher = ItemMatcher(algorithms=algorithms, attributes=selected_attributes, weights=weights, thresholds=thresholds, price_config=price_config, session=self.session)
        details = []
        
        for comp_row in comp_result:
            # Pass ALL attributes to calculate_score
            prod_data = {attr: prod.get(attr, '') for attr in all_attr_names}
            comp_data = {}
            for i, attr in enumerate(all_attr_names):
                val = comp_row[4 + i]
                if attr == 'url':
                    comp_data['url_display'] = val  # Original for display
                    comp_data[attr] = self._normalize_url_for_comparison(val)  # Normalized for scoring
                else:
                    comp_data[attr] = val
            
            result = matcher.calculate_score(prod_data, comp_data)
            
            # Get saved scores from matching_scores table if they exist
            saved_score_query = text("""SELECT ms.total_score, ms.score_status, msa.attribute_id, msa.score
                FROM matching_scores ms
                LEFT JOIN matching_score_attributes msa ON ms.score_id = msa.score_id
                WHERE ms.system_product_id = :sys_prod_id AND ms.competitor_product_id = :comp_id""")
            saved_result = conn.execute(saved_score_query, {'sys_prod_id': prod['system_product_id'], 'comp_id': comp_row[0]})
            saved_rows = saved_result.fetchall()
            
            saved_total_score = None
            saved_status = None
            saved_attr_scores = {}
            
            if saved_rows:
                saved_total_score = saved_rows[0][0]
                saved_status = saved_rows[0][1]
                for sr in saved_rows:
                    if sr[2] and sr[3] is not None:
                        # Map attribute_id back to attribute_name
                        attr_info = next((a for a in matcher.scoring_attrs if a.attribute_id == sr[2]), None)
                        if attr_info:
                            saved_attr_scores[attr_info.attribute_name] = sr[3]
            
            # Calculate average score per attribute - for ALL attributes (not just selected)
            attr_scores = {}
            for attr_name in all_attr_names:
                attr_algo_scores = [v for k, v in result.items() if k.startswith(f'{attr_name}_')]
                if attr_algo_scores:
                    attr_scores[f'{attr_name}_score'] = round(sum(attr_algo_scores) / len(attr_algo_scores), 2)
                else:
                    attr_scores[f'{attr_name}_score'] = 0
            
            detail = {
                'competitor_ref_id': comp_row[0],
                'competitor_id': comp_row[2],
                'is_saved': comp_row[0] == prod['competitor_product_id'],
                'review_status': prod['review_status'] if comp_row[0] == prod['competitor_product_id'] else 0,
                **attr_scores,
                'final_score': result['final_score'],
                'status': matcher.get_status(result['final_score']),
                'saved_score': saved_total_score,
                'saved_status': saved_status
            }
            
            # Add competitor attribute values for ALL attributes - use display version for URLs
            for i, attr in enumerate(all_attr_names):
                if attr == 'url':
                    detail[f'comp_{attr}'] = comp_data.get('url_display', comp_row[4 + i])
                else:
                    detail[f'comp_{attr}'] = comp_row[4 + i]
            
            # Add part_number
            detail['comp_part_number'] = comp_row[3]
            
            details.append(detail)
        
        # Build product dict for response - use display version for URLs, include only SELECTED attributes
        product_dict = {
            'brand': prod['brand'],
            'category': prod['category'],
            'product_type': prod['product_type'],
            'part_number': prod['part_number'],
            'product_id': prod['system_product_id'],
            'internal_product_id': prod['product_id']
        }
        for attr in selected_attributes:
            if attr == 'url':
                product_dict[attr] = prod.get('url_display', prod.get(attr, ''))
            else:
                product_dict[attr] = prod.get(attr, '')
        
        return {
            'product': product_dict,
            'comparisons': details,
            'algorithms': algorithms or ['tfidf'],
            'attributes': selected_attributes  # Return only selected attributes for display
        }
    
    def _normalize_url_for_comparison(self, url):
        """Normalize URL to extract product tokens for comparison"""
        if pd.isna(url) or not url:
            return ""
        
        # Remove protocol and domain
        path = re.sub(r'https?://[^/]+/', '', str(url))
        # Remove query parameters
        path = re.sub(r'\?.*$', '', path)
        # Remove file extensions
        path = re.sub(r'\.(html|htm|php|asp|aspx)$', '', path)
        # Normalize - replace hyphens, underscores, slashes with spaces
        path = path.lower()
        path = re.sub(r'[-_/]', ' ', path)
        path = re.sub(r'[^a-z0-9 ]', ' ', path)
        
        # Split into tokens and filter
        tokens = path.split()
        stopwords = {'products', 'product', 'item', 'items', 'furniture', 'pdp', 'p', 
                     'catalog', 'shop', 'buy', 'detail', 'details', 'view', 'page'}
        
        # Keep meaningful tokens
        meaningful_tokens = []
        for token in tokens:
            if token in stopwords:
                continue
            # Keep if it has both letters and numbers (SKU pattern) or is 3+ chars
            if (re.search(r'[a-z]', token) and re.search(r'[0-9]', token)) or len(token) >= 3:
                meaningful_tokens.append(token)
        
        return ' '.join(meaningful_tokens)
    
    def _load_products(self, brands, categories, types, competitors_df, attributes=None, product_ids=None):
        conn = self.session.connection()
        conditions = []
        params = {}
        
        # ALWAYS fetch ALL scoring attributes from database, regardless of selection
        all_scoring_attrs = self.attr_service.get_attributes_by_type('default')
        all_attr_names = [a.attribute_name for a in all_scoring_attrs]
        
        if product_ids and len(product_ids) > 0:
            placeholders = ','.join([':pid' + str(i) for i in range(len(product_ids))])
            conditions.append(f"msp.system_product_id IN ({placeholders})")
            for i, pid in enumerate(product_ids):
                params[f'pid{i}'] = pid
        else:
            if brands and len(brands) > 0:
                placeholders = ','.join([':b' + str(i) for i in range(len(brands))])
                conditions.append(f"msp.brand IN ({placeholders})")
                for i, b in enumerate(brands):
                    params[f'b{i}'] = b
            if categories and len(categories) > 0:
                placeholders = ','.join([':c' + str(i) for i in range(len(categories))])
                conditions.append(f"msp.category IN ({placeholders})")
                for i, c in enumerate(categories):
                    params[f'c{i}'] = c
            if types and len(types) > 0:
                placeholders = ','.join([':t' + str(i) for i in range(len(types))])
                conditions.append(f"msp.product_type IN ({placeholders})")
                for i, t in enumerate(types):
                    params[f't{i}'] = t
        
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        
        select_cols = ["msp.product_id", "msp.system_product_id", "msp.name",
                      "COALESCE(msp.brand, '') as brand", "COALESCE(msp.category, '') as category",
                      "COALESCE(msp.product_type, '') as product_type"]
        
        # Fetch ALL attributes from database
        for attr in all_attr_names:
            select_cols.append(f"COALESCE(msp.{attr}, '') as {attr}")
        
        query = text(f"""SELECT {', '.join(select_cols)}
                    FROM matching_system_product msp 
                     
                    WHERE {where_clause}""")
        
        result = conn.execute(query, params) if params else conn.execute(query)
        
        columns = ['product_id', 'system_product_id', 'name', 'brand', 'category', 'product_type'] + all_attr_names
        df = pd.DataFrame(result.fetchall(), columns=columns)
        
        print(f"[LOAD PRODUCTS] Loaded {len(df)} products with ALL attributes: {all_attr_names}")
        if len(df) > 0:
            print(f"[LOAD PRODUCTS] Sample product IDs: {df['system_product_id'].head(3).tolist()}")
        
        # Keep original URL for display, create normalized version for comparison
        if 'url' in df.columns:
            print(f"[LOAD PRODUCTS] Normalizing {len(df)} product URLs for comparison")
            df['url_display'] = df['url']  # Keep original for display
            df['url'] = df['url'].apply(self._normalize_url_for_comparison)  # Normalize for scoring
        
        return df
    
    def _load_competitors(self, attributes=None, product_ids=None):
        conn = self.session.connection()
        
        # ALWAYS fetch ALL scoring attributes from database, regardless of selection
        all_scoring_attrs = self.attr_service.get_attributes_by_type('default')
        all_attr_names = [a.attribute_name for a in all_scoring_attrs]
        
        select_cols = ["system_product_id"]
        for attr in all_attr_names:
            select_cols.append(f"COALESCE({attr}, '') as competitor_{attr}")
        select_cols.append("competitor_product_id as competitor_id")
        
        # ALWAYS require product_ids - never load all competitors
        if not product_ids or len(product_ids) == 0:
            columns = ['matching_product_id'] + [f'competitor_{attr}' for attr in all_attr_names] + ['competitor_id']
            return pd.DataFrame(columns=columns)
        
        placeholders = ','.join([f':pid{i}' for i in range(len(product_ids))])
        query = text(f"SELECT {', '.join(select_cols)} FROM matching_competitor_product WHERE system_product_id IN ({placeholders})")
        params = {f'pid{i}': pid for i, pid in enumerate(product_ids)}
        result = conn.execute(query, params)
        
        columns = ['matching_product_id'] + [f'competitor_{attr}' for attr in all_attr_names] + ['competitor_id']
        df = pd.DataFrame(result.fetchall(), columns=columns)
        
        print(f"[LOAD COMPETITORS] Loaded {len(df)} competitor records with ALL attributes: {all_attr_names}")
        if len(df) > 0:
            print(f"[LOAD COMPETITORS] Sample matching_product_ids: {df['matching_product_id'].head(3).tolist()}")
        
        # Keep original URL for display, create normalized version for comparison
        if 'competitor_url' in df.columns:
            print(f"[LOAD COMPETITORS] Normalizing {len(df)} competitor URLs for comparison")
            df['competitor_url_display'] = df['competitor_url']  # Keep original for display
            df['competitor_url'] = df['competitor_url'].apply(self._normalize_url_for_comparison)  # Normalize for scoring
        
        return df

    
    def save_match(self, competitor_ref_id, internal_product_id, save_top_most=False, score=None, status=None, sku_score=None, url_score=None, price_score=None):
        try:
            # Save top most reference if checkbox is checked
            if save_top_most:
                from datetime import datetime
                conn = self.session.connection()
                update_query = text("UPDATE matching_system_product SET competitor_product_id = :ref_id, matched_date = :match_date WHERE product_id = :prod_id")
                conn.execute(update_query, {'ref_id': competitor_ref_id, 'match_date': datetime.now(), 'prod_id': internal_product_id})
            
            self.session.commit()
            return {'success': True}
        except Exception as e:
            self.session.rollback()
            return {'success': False, 'error': str(e)}
    
    def _save_scores(self, products_df, competitors_df, matcher, save_top_most=False, reset_on_first=False, hard_refresh=False):
        try:
            saved_count = 0
            
            # Note: Deletion is now handled in run_analysis() before calling this method
            
            # Update configuration table first
            algorithm_id = matcher.algorithms[0] if matcher.algorithms else 'tfidf'
            self.config_service.update_configuration(
                algorithm_id=algorithm_id,
                weights=matcher.weights,
                thresholds=matcher.thresholds,
                price_config=matcher.price_config
            )
            
            # Get fresh connection after config update
            conn = self.session.connection()
            
            for _, prod in products_df.iterrows():
                prod_id = str(prod["system_product_id"])
                internal_prod_id = prod["product_id"]
                system_prod_id = prod["system_product_id"]  # This is what we want to store
                
                # Pass ALL attributes to calculate_score (not just selected ones)
                prod_data = {attr: prod.get(attr, '') for attr in matcher.all_attributes}
                # Add part_number for SKU comparison
                prod_data['part_number'] = prod.get('part_number', '')
                
                # CRITICAL: Only get competitors for THIS specific product from the filtered competitors_df
                comp_subset = competitors_df[competitors_df["matching_product_id"].astype(str) == prod_id]
                
                best_ref_id = None
                best_score = -1
                
                for _, comp in comp_subset.iterrows():
                    # Pass ALL attributes to calculate_score (not just selected ones)
                    comp_data = {attr: comp.get(f"competitor_{attr}", '') for attr in matcher.all_attributes}
                    
                    result = matcher.calculate_score(prod_data, comp_data)
                    
                    # Calculate average score per attribute - only for SELECTED attributes
                    attr_scores = {}
                    for attr_name in matcher.attributes:
                        if attr_name in matcher.attr_dict and matcher.attr_dict[attr_name]['type'] != 'status':
                            scores_for_attr = [v for k, v in result.items() if k.startswith(f'{attr_name}_')]
                            if scores_for_attr:
                                avg = sum(scores_for_attr) / len(scores_for_attr)
                                attr_scores[matcher.attr_dict[attr_name]['id']] = round(avg, 2)
                    
                    final_score = result['final_score']
                    final_status = matcher.get_status(final_score)
                    
                    comp_id = int(comp['competitor_id'])
                    
                    # Save to schema with system_product_id (not internal product_id)
                    self.score_service.save_score(
                        system_product_id=int(system_prod_id),  # Using system_product_id
                        competitor_product_id=comp_id,
                        algorithm_id=algorithm_id,
                        total_score=final_score,
                        score_status=final_status,
                        attribute_scores=attr_scores,
                        hard_refresh=hard_refresh
                    )
                    saved_count += 1
                    
                    if final_score > best_score:
                        best_score = final_score
                        best_ref_id = comp_id
                
                if save_top_most and best_ref_id:
                    from datetime import datetime
                    conn = self.session.connection()
                    update_system_query = text("UPDATE matching_system_product SET competitor_product_id = :ref_id, matched_date = :match_date WHERE product_id = :prod_id")
                    conn.execute(update_system_query, {'ref_id': best_ref_id, 'match_date': datetime.now(), 'prod_id': internal_prod_id})
            
            self.session.commit()
            print(f"Saved {saved_count} competitor scores to database")
        except Exception as e:
            self.session.rollback()
            print(f"Error saving scores: {e}")
            import traceback
            traceback.print_exc()
    
    def update_review_status(self, comp_id, review_status):
        try:
            conn = self.session.connection()
            
            # Get product_id from competitor table
            get_comp_query = text("SELECT system_product_id FROM matching_competitor_product WHERE competitor_product_id = :comp_id")
            comp_result = conn.execute(get_comp_query, {'comp_id': comp_id})
            comp_row = comp_result.fetchone()
            
            if not comp_row:
                return {'success': False, 'error': f'Competitor product {comp_id} not found'}
            
            product_id = comp_row[0]
            
            # Map action to review_status value
            status_map = {'approved': 1, 'rejected': 2, 'pending': 0}
            status_value = status_map.get(review_status, 0)
            
            # If approve or reject, update competitor_product_id and review_status
            if review_status in ['approved', 'rejected']:
                from datetime import datetime
                update_system_query = text("UPDATE matching_system_product SET competitor_product_id = :ref_id, matched_date = :match_date, review_status = :status WHERE system_product_id = :prod_id")
                conn.execute(update_system_query, {'ref_id': comp_id, 'match_date': datetime.now(), 'status': status_value, 'prod_id': product_id})
            else:
                # For pending, just update review_status
                update_query = text("UPDATE matching_system_product SET review_status = :status WHERE system_product_id = :prod_id")
                conn.execute(update_query, {'status': status_value, 'prod_id': product_id})
            
            self.session.commit()
            return {'success': True}
        except Exception as e:
            self.session.rollback()
            import traceback
            return {'success': False, 'error': str(e), 'traceback': traceback.format_exc()}
    
    def get_total_grid_count(self, brands=None, categories=None, types=None, status_filter=None, product_ids=None):
        try:
            conditions = []
            params = {}
            
            if brands and len(brands) > 0:
                placeholders = ','.join([':b' + str(i) for i in range(len(brands))])
                conditions.append(f"msp.brand IN ({placeholders})")
                for i, b in enumerate(brands):
                    params[f'b{i}'] = b
            if categories and len(categories) > 0:
                placeholders = ','.join([':c' + str(i) for i in range(len(categories))])
                conditions.append(f"msp.category IN ({placeholders})")
                for i, c in enumerate(categories):
                    params[f'c{i}'] = c
            if types and len(types) > 0:
                placeholders = ','.join([':t' + str(i) for i in range(len(types))])
                conditions.append(f"msp.product_type IN ({placeholders})")
                for i, t in enumerate(types):
                    params[f't{i}'] = t
            if status_filter and len(status_filter) > 0:
                placeholders = ','.join([':s' + str(i) for i in range(len(status_filter))])
                conditions.append(f"ms.score_status IN ({placeholders})")
                for i, s in enumerate(status_filter):
                    params[f's{i}'] = s
            if product_ids and len(product_ids) > 0:
                placeholders = ','.join([':p' + str(i) for i in range(len(product_ids))])
                conditions.append(f"msp.product_id IN ({placeholders})")
                for i, pid in enumerate(product_ids):
                    params[f'p{i}'] = pid
            
            where_clause = " AND ".join(conditions) if conditions else "1=1"
            query = text(f"""SELECT COUNT(*) FROM matching_system_product msp
                    
                    WHERE {where_clause}""")
            
            with self.session.connection() as conn:
                result = conn.execute(query, params) if params else conn.execute(query)
                total = result.scalar()
            
            return {'total': total}
        except Exception as e:
            return {'error': str(e)}

    def get_matching_items_chunk(self, offset=0, limit=100, brands=None, categories=None, types=None, status_filter=None, product_ids=None):
        try:
            conn = self.session.connection()
            conditions = []
            params = {'offset': offset, 'limit': limit}
            
            if brands and len(brands) > 0:
                placeholders = ','.join([':b' + str(i) for i in range(len(brands))])
                conditions.append(f"msp.brand IN ({placeholders})")
                for i, b in enumerate(brands):
                    params[f'b{i}'] = b
            if categories and len(categories) > 0:
                placeholders = ','.join([':c' + str(i) for i in range(len(categories))])
                conditions.append(f"msp.category IN ({placeholders})")
                for i, c in enumerate(categories):
                    params[f'c{i}'] = c
            if types and len(types) > 0:
                placeholders = ','.join([':t' + str(i) for i in range(len(types))])
                conditions.append(f"msp.product_type IN ({placeholders})")
                for i, t in enumerate(types):
                    params[f't{i}'] = t
            if status_filter and len(status_filter) > 0:
                placeholders = ','.join([':s' + str(i) for i in range(len(status_filter))])
                conditions.append(f"ms.score_status IN ({placeholders})")
                for i, s in enumerate(status_filter):
                    params[f's{i}'] = s
            if product_ids and len(product_ids) > 0:
                placeholders = ','.join([':p' + str(i) for i in range(len(product_ids))])
                conditions.append(f"msp.product_id IN ({placeholders})")
                for i, pid in enumerate(product_ids):
                    params[f'p{i}'] = pid
            
            where_clause = " AND ".join(conditions) if conditions else "1=1"
            query = text(f"""SELECT msp.product_id, msp.system_product_id, msp.sku, msp.name, msp.price, msp.url,
                    msp.brand, msp.category, msp.product_type, msp.review_status, msp.competitor_product_id,
                    (SELECT ms2.total_score FROM matching_scores ms2 WHERE ms2.system_product_id = msp.system_product_id ORDER BY ms2.total_score DESC LIMIT 1) as max_score,
                    (SELECT ms3.score_status FROM matching_scores ms3 WHERE ms3.system_product_id = msp.system_product_id ORDER BY ms3.total_score DESC LIMIT 1) as top_status,
                    (SELECT ms4.competitor_product_id FROM matching_scores ms4 WHERE ms4.system_product_id = msp.system_product_id ORDER BY ms4.total_score DESC LIMIT 1) as top_competitor_id,
                    (SELECT COUNT(*) FROM matching_competitor_product mcp2 WHERE mcp2.system_product_id = msp.system_product_id) as competitor_count
                    FROM matching_system_product msp
                    
                    WHERE {where_clause}
                    ORDER BY msp.product_id DESC
                    LIMIT :limit OFFSET :offset""")
                
            result = conn.execute(query, params) if params else conn.execute(query)
            rows = result.fetchall()
            
            # Get competitor details for top competitors
            top_comp_ids = [r[13] for r in rows if r[13]]
            comp_map = {}
            if top_comp_ids:
                comp_query = text("SELECT competitor_product_id, competitor_id FROM matching_competitor_product WHERE competitor_product_id IN :ids")
                comp_result = conn.execute(comp_query, {'ids': tuple(top_comp_ids)})
                for cr in comp_result:
                    comp_map[cr[0]] = cr[1]
            
            data = []
            for row in rows:
                top_comp_id = row[13]
                data.append({
                    'product_id': row[0],
                    'system_product_id': row[1],
                    'sku': row[2],
                    'name': row[3],
                    'price': row[4],
                    'url': row[5],
                    'brand': row[6],
                    'category': row[7],
                    'product_type': row[8],
                    'review_status': row[9],
                    'competitor_id': comp_map.get(top_comp_id) if top_comp_id else None,
                    'score': row[11],
                    'score_status': row[12],
                    'competitor_count': row[14]
                })
            
            return {'data': data}
        except Exception as e:
            return {'error': str(e)}

    def get_matching_items_grid(self, brands=None, categories=None, types=None, status_filter=None, product_ids=None):
        try:
            conn = self.session.connection()
            conditions = []
            params = {}
            
            if brands and len(brands) > 0:
                placeholders = ','.join([':b' + str(i) for i in range(len(brands))])
                conditions.append(f"msp.brand IN ({placeholders})")
                for i, b in enumerate(brands):
                    params[f'b{i}'] = b
            if categories and len(categories) > 0:
                placeholders = ','.join([':c' + str(i) for i in range(len(categories))])
                conditions.append(f"msp.category IN ({placeholders})")
                for i, c in enumerate(categories):
                    params[f'c{i}'] = c
            if types and len(types) > 0:
                placeholders = ','.join([':t' + str(i) for i in range(len(types))])
                conditions.append(f"msp.product_type IN ({placeholders})")
                for i, t in enumerate(types):
                    params[f't{i}'] = t
            if status_filter and len(status_filter) > 0:
                placeholders = ','.join([':s' + str(i) for i in range(len(status_filter))])
                conditions.append(f"ms.score_status IN ({placeholders})")
                for i, s in enumerate(status_filter):
                    params[f's{i}'] = s
            if product_ids and len(product_ids) > 0:
                placeholders = ','.join([':p' + str(i) for i in range(len(product_ids))])
                conditions.append(f"msp.product_id IN ({placeholders})")
                for i, pid in enumerate(product_ids):
                    params[f'p{i}'] = pid
            
            where_clause = " AND ".join(conditions) if conditions else "1=1"
            query = text(f"""SELECT msp.product_id, msp.system_product_id, msp.sku, msp.part_number, msp.name, msp.price, msp.url,
                    msp.brand, msp.category, msp.product_type, msp.review_status, msp.competitor_product_id,
                    (SELECT ms2.total_score FROM matching_scores ms2 WHERE ms2.system_product_id = msp.system_product_id ORDER BY ms2.total_score DESC LIMIT 1) as max_score,
                    (SELECT ms3.score_status FROM matching_scores ms3 WHERE ms3.system_product_id = msp.system_product_id ORDER BY ms3.total_score DESC LIMIT 1) as top_status,
                    (SELECT ms4.competitor_product_id FROM matching_scores ms4 WHERE ms4.system_product_id = msp.system_product_id ORDER BY ms4.total_score DESC LIMIT 1) as top_competitor_id,
                    (SELECT COUNT(*) FROM matching_competitor_product mcp2 WHERE mcp2.system_product_id = msp.system_product_id) as competitor_count
                    FROM matching_system_product msp
                    
                    WHERE {where_clause}
                    ORDER BY msp.product_id DESC""")
            
            result = conn.execute(query, params) if params else conn.execute(query)
            rows = result.fetchall()
            
            # Get competitor details for top competitors
            top_comp_ids = [r[14] for r in rows if r[14]]
            comp_map = {}
            if top_comp_ids:
                comp_query = text("SELECT competitor_product_id, competitor_id FROM matching_competitor_product WHERE competitor_product_id IN :ids")
                comp_result = conn.execute(comp_query, {'ids': tuple(top_comp_ids)})
                for cr in comp_result:
                    comp_map[cr[0]] = cr[1]
            
            data = []
            for row in rows:
                top_comp_id = row[14]
                data.append({
                    'product_id': row[0],
                    'system_product_id': row[1],
                    'sku': row[2],
                    'part_number': row[3],
                    'name': row[4],
                    'price': row[5],
                    'url': row[6],
                    'brand': row[7],
                    'category': row[8],
                    'product_type': row[9],
                    'review_status': row[10],
                    'competitor_id': comp_map.get(top_comp_id) if top_comp_id else None,
                    'score': row[12],
                    'score_status': row[13],
                    'competitor_count': row[15]
                })
            
            return {'data': data}
        except Exception as e:
            return {'error': str(e)}

    def update_match_action(self, competitor_ref_id, internal_product_id, score=None, status=None, sku_score=None, url_score=None, price_score=None, action=None):
        try:
            conn = self.session.connection()
            
            if action == 'approve':
                from datetime import datetime
                update_query = text("UPDATE matching_system_product SET competitor_product_id = :ref_id, matched_date = :match_date, review_status = 1 WHERE product_id = :prod_id")
                conn.execute(update_query, {'ref_id': competitor_ref_id, 'match_date': datetime.now(), 'prod_id': internal_product_id})
            elif action == 'reject':
                from datetime import datetime
                update_query = text("UPDATE matching_system_product SET competitor_product_id = :ref_id, matched_date = :match_date, review_status = 2 WHERE product_id = :prod_id")
                conn.execute(update_query, {'ref_id': competitor_ref_id, 'match_date': datetime.now(), 'prod_id': internal_product_id})
            else:
                update_query = text("UPDATE matching_system_product SET review_status = 0 WHERE product_id = :prod_id")
                conn.execute(update_query, {'prod_id': internal_product_id})
            
            self.session.commit()
            return {'success': True}
        except Exception as e:
            self.session.rollback()
            return {'success': False, 'error': str(e)}

    def get_comparison_data(self, product_id, attributes=None):
        try:
            if not attributes:
                scoring_attrs = self.attr_service.get_attributes_by_type('default')
                attributes = [a.attribute_name for a in scoring_attrs]
            
            with self.session.connection() as conn:
                # Build dynamic SELECT for system product
                prod_cols = ['msp.product_id', 'msp.system_product_id', 'msp.name', 'msp.brand', 'msp.category', 'msp.product_type', 'msp.part_number', 'msp.competitor_product_id', 'msp.review_status']
                for attr in attributes:
                    prod_cols.append(f'msp.{attr}')
                
                system_query = text(f"""SELECT {', '.join(prod_cols)}
                    FROM matching_system_product msp
                    
                    WHERE msp.product_id = :pid""")
                system_result = conn.execute(system_query, {'pid': product_id})
                system_row = system_result.fetchone()
                
                if not system_row:
                    return {'error': 'Product not found'}
                
                system_product = {
                    'product_id': system_row[0],
                    'system_product_id': system_row[1],
                    'name': system_row[2],
                    'brand': system_row[3],
                    'category': system_row[4],
                    'product_type': system_row[5],
                    'part_number': system_row[6]
                }
                competitor_product_id = system_row[7]
                review_status = system_row[8]
                
                # Add dynamic attributes
                for i, attr in enumerate(attributes):
                    system_product[attr] = system_row[9 + i]
                
                # Build dynamic SELECT for competitors
                comp_cols = ['mcp.competitor_product_id']
                for attr in attributes:
                    comp_cols.append(f"COALESCE(mcp.{attr}, '') as {attr}")
                
                query = text(f"""SELECT {', '.join(comp_cols)}, ms.total_score, ms.score_status
                    FROM matching_competitor_product mcp
                    LEFT JOIN matching_scores ms ON mcp.competitor_product_id = ms.competitor_product_id 
                        AND ms.system_product_id = :spid
                    WHERE mcp.system_product_id = :spid LIMIT 1000""")
                result = conn.execute(query, {'spid': system_product['system_product_id']})
                rows = result.fetchall()
                
                # Get attribute scores for all competitors
                attr_scores_query = text("""SELECT msa.competitor_product_id, ma.attribute_name, msa.score
                    FROM matching_score_attributes msa
                    JOIN matching_attribute ma ON msa.attribute_id = ma.attribute_id
                    WHERE msa.system_product_id = :spid""")
                attr_result = conn.execute(attr_scores_query, {'spid': system_product['system_product_id']})
                
                # Build map of competitor_id -> {attribute_name: score}
                attr_scores_map = {}
                for ar in attr_result:
                    comp_id = ar[0]
                    attr_name = ar[1]
                    score = ar[2]
                    if comp_id not in attr_scores_map:
                        attr_scores_map[comp_id] = {}
                    attr_scores_map[comp_id][attr_name] = score
            
            comparisons = []
            for row in rows:
                comp_id = row[0]
                attr_scores = attr_scores_map.get(comp_id, {})
                
                comp_data = {'id': comp_id}
                
                # Add dynamic attribute values
                for i, attr in enumerate(attributes):
                    comp_data[attr] = row[1 + i]
                
                # Add total score and status
                comp_data['score'] = row[len(attributes) + 1]
                comp_data['status'] = row[len(attributes) + 2]
                
                # Add attribute scores dynamically
                for attr in attributes:
                    comp_data[f'{attr}_score'] = attr_scores.get(attr)
                
                comp_data['is_matched'] = comp_id == competitor_product_id
                comp_data['review_status'] = review_status if comp_id == competitor_product_id else 0
                
                comparisons.append(comp_data)
            
            return {'data': {'system_product': system_product, 'comparisons': comparisons, 'attributes': attributes}}
        except Exception as e:
            return {'error': str(e)}


    
    def recalculate_scores(self, attributes, weights, thresholds, algorithm=None, brands=None, categories=None, types=None, product_ids=None, price_config=None):
        """Recalculate scores for existing products - both attribute scores and total scores"""
        try:
            # If price_config is provided, we need to recalculate attribute scores, not just total scores
            if price_config:
                # Full recalculation with new price config
                return self.run_analysis(
                    brands=brands, categories=categories, types=types,
                    algorithms=[algorithm] if algorithm else ['tfidf'],
                    attributes=attributes, weights=weights, thresholds=thresholds,
                    price_config=price_config, save_score=True, product_ids=product_ids
                )
            
            # Otherwise, just recalculate total scores using existing attribute scores
            conditions = []
            params = {}
            
            # If product_ids provided, use them; otherwise use brand/category/type filters
            if product_ids and len(product_ids) > 0:
                placeholders = ','.join([':p' + str(i) for i in range(len(product_ids))])
                conditions.append(f"msp.product_id IN ({placeholders})")
                for i, pid in enumerate(product_ids):
                    params[f'p{i}'] = pid
            else:
                if brands and len(brands) > 0:
                    placeholders = ','.join([':b' + str(i) for i in range(len(brands))])
                    conditions.append(f"msp.brand IN ({placeholders})")
                    for i, b in enumerate(brands):
                        params[f'b{i}'] = b
                if categories and len(categories) > 0:
                    placeholders = ','.join([':c' + str(i) for i in range(len(categories))])
                    conditions.append(f"msp.category IN ({placeholders})")
                    for i, c in enumerate(categories):
                        params[f'c{i}'] = c
                if types and len(types) > 0:
                    placeholders = ','.join([':t' + str(i) for i in range(len(types))])
                    conditions.append(f"msp.product_type IN ({placeholders})")
                    for i, t in enumerate(types):
                        params[f't{i}'] = t
            
            where_clause = " AND ".join(conditions) if conditions else "1=1"
            algorithm_id = algorithm or 'tfidf'
            
            # Update configuration table first
            self.config_service.update_configuration(
                algorithm_id=algorithm_id,
                weights=weights,
                thresholds=thresholds
            )
            
            # Now work with scores
            conn = self.session.connection()
            
            # Get products with existing score attributes for this algorithm
            existing_query = text(f"""SELECT DISTINCT msp.system_product_id
                FROM matching_system_product msp
                
                JOIN matching_scores ms ON msp.system_product_id = ms.system_product_id
                JOIN matching_score_attributes msa ON ms.score_id = msa.score_id
                WHERE msa.algorithm_id = :algo_id AND {where_clause}""")
            
            existing_result = conn.execute(existing_query, {**params, 'algo_id': algorithm_id})
            products_with_scores = [row[0] for row in existing_result.fetchall()]
            
            if not products_with_scores:
                return {'success': True, 'updated': 0, 'message': 'No existing scores found for selected filters'}
            
            # Get scores with attributes for this specific algorithm
            query = text("""SELECT ms.score_id, msa.attribute_id, msa.score
                FROM matching_scores ms
                JOIN matching_score_attributes msa ON ms.score_id = msa.score_id
                WHERE ms.system_product_id IN :pids AND msa.algorithm_id = :algo_id
                ORDER BY ms.score_id""")
            result = conn.execute(query, {'pids': tuple(products_with_scores), 'algo_id': algorithm_id})
            rows = result.fetchall()
            
            score_groups = {}
            for row in rows:
                score_id = row[0]
                if score_id not in score_groups:
                    score_groups[score_id] = {'attr_scores': {}}
                if row[1] and row[2] is not None:
                    score_groups[score_id]['attr_scores'][row[1]] = row[2]
            
            attr_id_map = {}
            for attr in self.attr_service.get_all_attributes():
                if attr.attribute_name in attributes:
                    attr_id_map[attr.attribute_id] = attr.attribute_name
            
            updated_count = 0
            for score_id, score_data in score_groups.items():
                total_weight = 0
                weighted_sum = 0
                
                for attr_id, score_val in score_data['attr_scores'].items():
                    attr_name = attr_id_map.get(attr_id)
                    if attr_name and attr_name in weights:
                        weighted_sum += score_val * weights[attr_name]
                        total_weight += weights[attr_name]
                
                new_total_score = round(weighted_sum / total_weight, 2) if total_weight > 0 else 0.0
                matched_threshold = thresholds.get('matched', 85)
                review_threshold = thresholds.get('review', 70)
                
                if new_total_score >= matched_threshold:
                    new_status = 'Matched'
                elif new_total_score >= review_threshold:
                    new_status = 'Review'
                else:
                    new_status = 'Not Matched'
                
                update_query = text("UPDATE matching_scores SET algorithm_id = :algo_id, total_score = :score, score_status = :status WHERE score_id = :score_id")
                conn.execute(update_query, {'algo_id': algorithm_id, 'score': new_total_score, 'status': new_status, 'score_id': score_id})
                updated_count += 1
            
            self.session.commit()
            recalc_query = text("SELECT ms.system_product_id, ms.total_score, ms.score_status FROM matching_scores ms WHERE ms.system_product_id IN :pids AND ms.algorithm_id = :algo_id")
            recalc_result = conn.execute(recalc_query, {"pids": tuple(products_with_scores), "algo_id": algorithm_id})
            recalc_rows = recalc_result.fetchall()
            
            recalculated_scores = []
            for row in recalc_rows:
                recalculated_scores.append({"product_id": row[0], "max_score": row[1], "status": row[2]})
            
            return {"success": True, "updated": updated_count, "recalculated_scores": recalculated_scores}
        except Exception as e:
            self.session.rollback()
            import traceback
            traceback.print_exc()
            return {'success': False, 'error': str(e)}


    def get_score_distribution(self, brands=None, categories=None, types=None, status_filter=None, product_ids=None):
        """Get score distribution by ranges (0-10, 10-20, etc.)"""
        try:
            conn = self.session.connection()
            conditions = []
            params = {}
            
            if brands and len(brands) > 0:
                placeholders = ','.join([':b' + str(i) for i in range(len(brands))])
                conditions.append(f"msp.brand IN ({placeholders})")
                for i, b in enumerate(brands):
                    params[f'b{i}'] = b
            if categories and len(categories) > 0:
                placeholders = ','.join([':c' + str(i) for i in range(len(categories))])
                conditions.append(f"msp.category IN ({placeholders})")
                for i, c in enumerate(categories):
                    params[f'c{i}'] = c
            if types and len(types) > 0:
                placeholders = ','.join([':t' + str(i) for i in range(len(types))])
                conditions.append(f"msp.product_type IN ({placeholders})")
                for i, t in enumerate(types):
                    params[f't{i}'] = t
            if status_filter and len(status_filter) > 0:
                placeholders = ','.join([':s' + str(i) for i in range(len(status_filter))])
                conditions.append(f"ms.score_status IN ({placeholders})")
                for i, s in enumerate(status_filter):
                    params[f's{i}'] = s
            if product_ids and len(product_ids) > 0:
                placeholders = ','.join([':p' + str(i) for i in range(len(product_ids))])
                conditions.append(f"msp.product_id IN ({placeholders})")
                for i, pid in enumerate(product_ids):
                    params[f'p{i}'] = pid
            
            where_clause = " AND ".join(conditions) if conditions else "1=1"
            
            query = text(f"""
                SELECT 
                    CASE 
                        WHEN ms.total_score < 10 THEN '0-10'
                        WHEN ms.total_score < 20 THEN '10-20'
                        WHEN ms.total_score < 30 THEN '20-30'
                        WHEN ms.total_score < 40 THEN '30-40'
                        WHEN ms.total_score < 50 THEN '40-50'
                        WHEN ms.total_score < 60 THEN '50-60'
                        WHEN ms.total_score < 70 THEN '60-70'
                        WHEN ms.total_score < 80 THEN '70-80'
                        WHEN ms.total_score < 90 THEN '80-90'
                        ELSE '90-100'
                    END as `range`,
                    COUNT(DISTINCT ms.score_id) as count
                FROM matching_system_product msp
                INNER JOIN matching_scores ms ON msp.product_id = ms.system_product_id
                WHERE {where_clause}
                GROUP BY `range`
                ORDER BY CAST(SUBSTRING_INDEX(`range`, '-', 1) AS UNSIGNED)
            """)
            
            result = conn.execute(query, params) if params else conn.execute(query)
            rows = result.fetchall()
            
            distribution = {}
            ranges = ['0-10', '10-20', '20-30', '30-40', '40-50', '50-60', '60-70', '70-80', '80-90', '90-100']
            for r in ranges:
                distribution[r] = 0
            
            for row in rows:
                if row[0]:
                    distribution[row[0]] = row[1]
            
            return {
                'ranges': ranges,
                'data': [distribution[r] for r in ranges]
            }
        except Exception as e:
            print(f"Error in get_score_distribution: {e}")
            return {'ranges': [], 'data': []}
