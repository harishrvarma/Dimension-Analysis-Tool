from models.base.base import SessionLocal
from repositories.product_repository import ProductRepository
import pandas as pd
import numpy as np
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
        
        # Build attr_dict for scoring attributes only
        self.attr_dict = {a.attribute_name: {'id': a.attribute_id, 'weightage': a.default_weightage, 'type': a.attribute_type} for a in self.scoring_attrs}
        
        # Filter by requested attributes if provided
        if attributes:
            self.attr_dict = {k: v for k, v in self.attr_dict.items() if k in attributes}
        
        self.attributes = list(self.attr_dict.keys())
        self.algorithms = algorithms or ['tfidf']
        self.weights = weights or {k: v['weightage'] for k, v in self.attr_dict.items()}
        
        # Use price config from DB or override
        self.price_config = price_config or self.price_attrs
        
        # Use thresholds from DB or override
        self.thresholds = thresholds or self.status_attrs
        
        self.algo_instances = {algo: AlgorithmFactory.get_algorithm(algo, self.price_config) for algo in self.algorithms}
    
    def calculate_score(self, prod_data, comp_data):
        scores = {}
        
        for attr_name, attr_info in self.attr_dict.items():
            prod_val = prod_data.get(attr_name, '')
            comp_val = comp_data.get(attr_name, '')
            
            for algo_name in self.algorithms:
                algo = self.algo_instances[algo_name]
                
                if attr_name == 'price':
                    if algo_name in ['custom', 'tfidf_price']:
                        score_val = algo.score_price(prod_val, comp_val)
                    else:
                        score_val = algo.score(str(prod_val), str(comp_val))
                elif attr_name == 'sku':
                    if algo_name == 'custom':
                        score_val = algo.score_sku(prod_val, comp_val)
                    else:
                        score_val = algo.score(str(prod_val), str(comp_val))
                elif attr_name == 'url':
                    if algo_name == 'custom':
                        score_val = algo.score_url(prod_val, comp_val)
                    else:
                        score_val = algo.score(str(prod_val), str(comp_val))
                else:
                    score_val = algo.score(str(prod_val), str(comp_val))
                
                scores[f'{attr_name}_{algo_name}'] = score_val
        
        # Calculate final score
        total_weight = 0
        weighted_sum = 0
        
        for attr_name in self.attr_dict.keys():
            attr_scores = [v for k, v in scores.items() if k.startswith(f'{attr_name}_')]
            if attr_scores:
                avg_score = sum(attr_scores) / len(attr_scores)
                weighted_sum += avg_score * self.weights.get(attr_name, 0)
                total_weight += self.weights.get(attr_name, 0)
        
        final_score = (weighted_sum / total_weight) if total_weight > 0 else 0.0
        scores['final_score'] = round(final_score, 2)
        
        return {k: round(v, 2) for k, v in scores.items()}
    
    def get_status(self, score: float) -> str:
        matched_threshold = self.thresholds.get('matched', 85)
        review_threshold = self.thresholds.get('review', 70)
        
        if score >= matched_threshold:
            return self.STATUS_MATCHED
        elif score >= review_threshold:
            return self.STATUS_REVIEW
        else:
            return self.STATUS_NOT_MATCHED
    
    def match_items(self, products_df: pd.DataFrame, competitors_df: pd.DataFrame):
        summary_rows = []
        
        for _, prod in products_df.iterrows():
            prod_id = str(prod["system_product_id"])
            
            prod_data = {attr: prod.get(attr, '') for attr in self.attributes}
            
            comp_subset = competitors_df[competitors_df["matching_product_id"].astype(str) == prod_id]
            
            if comp_subset.empty:
                continue
            
            scores = []
            statuses = []
            
            for _, comp in comp_subset.iterrows():
                comp_data = {attr: comp.get(f"competitor_{attr}", '') for attr in self.attributes}
                
                result = self.calculate_score(prod_data, comp_data)
                scores.append(result['final_score'])
                statuses.append(self.get_status(result['final_score']))
            
            max_score = float(np.max(scores))
            
            summary_rows.append({
                "product_id": prod["system_product_id"],
                "sku": prod.get("sku", ""),
                "brand": prod.get("brand", ""),
                "category": prod.get("category", ""),
                "product_type": prod.get("product_type", ""),
                "name": prod.get("name", ""),
                "max_score": round(max_score, 2),
                "total_items": len(scores),
                "matched_count": statuses.count(self.STATUS_MATCHED),
                "review_count": statuses.count(self.STATUS_REVIEW),
                "not_matched_count": statuses.count(self.STATUS_NOT_MATCHED),
                "status": self.get_status(max_score)
            })
        
        return pd.DataFrame(summary_rows)


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
            brand_query = text("""SELECT p.brand, 
                COUNT(DISTINCT msp.product_id) as total_count,
                COUNT(DISTINCT CASE WHEN ms.total_score IS NOT NULL THEN msp.product_id END) as scored_count
                FROM matching_system_product msp 
                JOIN product p ON msp.system_product_id = p.system_product_id 
                JOIN matching_competitor_product mcp ON msp.system_product_id = mcp.system_product_id 
                LEFT JOIN matching_scores ms ON msp.product_id = ms.system_product_id
                WHERE p.brand IS NOT NULL AND p.brand != '' 
                GROUP BY p.brand ORDER BY p.brand""")
            brand_result = conn.execute(brand_query)
            brands_list = []
            for row in brand_result:
                status = 'complete' if row[2] == row[1] and row[2] > 0 else ('partial' if row[2] > 0 else 'none')
                brands_list.append({'label': f"{row[0]} ({row[1]})", 'value': row[0], 'status': status})
            
            # Get categories with score status
            if brands and len(brands) > 0:
                placeholders = ','.join([':b' + str(i) for i in range(len(brands))])
                cat_query = text(f"""SELECT p.category, 
                    COUNT(DISTINCT msp.product_id) as total_count,
                    COUNT(DISTINCT CASE WHEN ms.total_score IS NOT NULL THEN msp.product_id END) as scored_count
                    FROM matching_system_product msp 
                    JOIN product p ON msp.system_product_id = p.system_product_id 
                    JOIN matching_competitor_product mcp ON msp.system_product_id = mcp.system_product_id 
                    LEFT JOIN matching_scores ms ON msp.product_id = ms.system_product_id
                    WHERE p.brand IN ({placeholders}) AND p.category IS NOT NULL AND p.category != '' 
                    GROUP BY p.category ORDER BY p.category""")
                cat_result = conn.execute(cat_query, {f'b{i}': b for i, b in enumerate(brands)})
            else:
                cat_query = text("""SELECT p.category, 
                    COUNT(DISTINCT msp.product_id) as total_count,
                    COUNT(DISTINCT CASE WHEN ms.total_score IS NOT NULL THEN msp.product_id END) as scored_count
                    FROM matching_system_product msp 
                    JOIN product p ON msp.system_product_id = p.system_product_id 
                    JOIN matching_competitor_product mcp ON msp.system_product_id = mcp.system_product_id 
                    LEFT JOIN matching_scores ms ON msp.product_id = ms.system_product_id
                    WHERE p.category IS NOT NULL AND p.category != '' 
                    GROUP BY p.category ORDER BY p.category""")
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
                type_conditions.append(f"p.brand IN ({placeholders})")
                for i, b in enumerate(brands):
                    type_params[f'b{i}'] = b
            if categories and len(categories) > 0:
                placeholders = ','.join([':c' + str(i) for i in range(len(categories))])
                type_conditions.append(f"p.category IN ({placeholders})")
                for i, c in enumerate(categories):
                    type_params[f'c{i}'] = c
            
            if type_conditions:
                type_where = " AND ".join(type_conditions)
                type_query = text(f"""SELECT p.product_type, 
                    COUNT(DISTINCT msp.product_id) as total_count,
                    COUNT(DISTINCT CASE WHEN ms.total_score IS NOT NULL THEN msp.product_id END) as scored_count
                    FROM matching_system_product msp 
                    JOIN product p ON msp.system_product_id = p.system_product_id 
                    JOIN matching_competitor_product mcp ON msp.system_product_id = mcp.system_product_id 
                    LEFT JOIN matching_scores ms ON msp.product_id = ms.system_product_id
                    WHERE {type_where} AND p.product_type IS NOT NULL AND p.product_type != '' 
                    GROUP BY p.product_type ORDER BY p.product_type""")
                type_result = conn.execute(type_query, type_params)
            else:
                type_query = text("""SELECT p.product_type, 
                    COUNT(DISTINCT msp.product_id) as total_count,
                    COUNT(DISTINCT CASE WHEN ms.total_score IS NOT NULL THEN msp.product_id END) as scored_count
                    FROM matching_system_product msp 
                    JOIN product p ON msp.system_product_id = p.system_product_id 
                    JOIN matching_competitor_product mcp ON msp.system_product_id = mcp.system_product_id 
                    LEFT JOIN matching_scores ms ON msp.product_id = ms.system_product_id
                    WHERE p.product_type IS NOT NULL AND p.product_type != '' 
                    GROUP BY p.product_type ORDER BY p.product_type""")
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
                conditions.append(f"p.brand IN ({placeholders})")
                for i, b in enumerate(brands):
                    params[f'b{i}'] = b
            if categories and len(categories) > 0:
                placeholders = ','.join([':c' + str(i) for i in range(len(categories))])
                conditions.append(f"p.category IN ({placeholders})")
                for i, c in enumerate(categories):
                    params[f'c{i}'] = c
            if types and len(types) > 0:
                placeholders = ','.join([':t' + str(i) for i in range(len(types))])
                conditions.append(f"p.product_type IN ({placeholders})")
                for i, t in enumerate(types):
                    params[f't{i}'] = t
            
            where_clause = " AND ".join(conditions) if conditions else "1=1"
            count_query = text(f"""SELECT COUNT(DISTINCT msp.product_id) 
                FROM matching_system_product msp 
                JOIN product p ON msp.system_product_id = p.system_product_id 
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
    
    def run_analysis(self, brands=None, categories=None, types=None, algorithms=None, attributes=None, weights=None, thresholds=None, price_config=None, save_score=False, save_top_most=False):
        # Get only scoring attributes (type='default')
        if not attributes:
            scoring_attrs = self.attr_service.get_attributes_by_type('default')
            attributes = [a.attribute_name for a in scoring_attrs]
        
        competitors_df = self._load_competitors(attributes)
        if competitors_df.empty:
            return {'error': 'No competitor data available'}
        
        products_df = self._load_products(brands, categories, types, competitors_df, attributes)
        if products_df.empty:
            return {'error': 'No products found'}
        
        matcher = ItemMatcher(algorithms=algorithms, attributes=attributes, weights=weights, thresholds=thresholds, price_config=price_config, session=self.session)
        summary = matcher.match_items(products_df, competitors_df)
        
        if summary.empty:
            return {'error': 'No products with competitors found'}
        
        if save_score or save_top_most:
            self._save_scores(products_df, competitors_df, matcher, save_top_most, reset_on_first=True)
        
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
        
        if not attributes:
            scoring_attrs = self.attr_service.get_attributes_by_type('default')
            attributes = [a.attribute_name for a in scoring_attrs]
        
        # Build dynamic SELECT for product
        prod_cols = ["msp.product_id", "msp.system_product_id", "msp.name", "p.brand", "p.category", "p.product_type", "msp.competitor_product_id", "msp.review_status"]
        for attr in attributes:
            prod_cols.append(f"msp.{attr}")
        
        query = text(f"SELECT {', '.join(prod_cols)} FROM matching_system_product msp JOIN product p ON msp.system_product_id = p.system_product_id WHERE msp.system_product_id = :pid")
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
            'competitor_product_id': row[6],
            'review_status': row[7]
        }
        
        # Add dynamic attributes
        for i, attr in enumerate(attributes):
            prod[attr] = row[8 + i]
        
        # Build dynamic SELECT for competitors
        comp_cols = ["competitor_product_id", "system_product_id", "competitor_id"]
        for attr in attributes:
            comp_cols.append(f"COALESCE({attr}, '')")
        
        comp_query = text(f"SELECT {', '.join(comp_cols)} FROM matching_competitor_product WHERE system_product_id = :pid")
        comp_result = conn.execute(comp_query, {'pid': prod['system_product_id']})
        
        matcher = ItemMatcher(algorithms=algorithms, attributes=attributes, weights=weights, thresholds=thresholds, price_config=price_config, session=self.session)
        details = []
        
        for comp_row in comp_result:
            prod_data = {attr: prod.get(attr, '') for attr in attributes}
            comp_data = {attr: comp_row[3 + i] for i, attr in enumerate(attributes)}
            
            result = matcher.calculate_score(prod_data, comp_data)
            
            # Get saved scores from matching_scores table if they exist
            saved_score_query = text("""SELECT ms.total_score, ms.score_status, msa.attribute_id, msa.score
                FROM matching_scores ms
                LEFT JOIN matching_score_attributes msa ON ms.score_id = msa.matching_score_id
                WHERE ms.system_product_id = :sys_prod_id AND ms.competitor_product_id = :comp_id""")
            saved_result = conn.execute(saved_score_query, {'sys_prod_id': prod['product_id'], 'comp_id': comp_row[0]})
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
            
            detail = {
                'competitor_ref_id': comp_row[0],
                'competitor_id': comp_row[2],
                'is_saved': comp_row[0] == prod['competitor_product_id'],
                'review_status': prod['review_status'] if comp_row[0] == prod['competitor_product_id'] else 0,
                **result,
                'status': matcher.get_status(result['final_score']),
                'saved_score': saved_total_score,
                'saved_status': saved_status
            }
            
            # Add competitor attribute values
            for i, attr in enumerate(attributes):
                detail[f'comp_{attr}'] = comp_row[3 + i]
            
            # Add saved attribute scores if available
            for attr_name, score in saved_attr_scores.items():
                detail[f'saved_{attr_name}_score'] = score
            
            details.append(detail)
        
        # Build product dict for response
        product_dict = {
            'brand': prod['brand'],
            'category': prod['category'],
            'product_type': prod['product_type'],
            'product_id': prod['system_product_id'],
            'internal_product_id': prod['product_id']
        }
        for attr in attributes:
            product_dict[attr] = prod.get(attr, '')
        
        return {
            'product': product_dict,
            'comparisons': details,
            'algorithms': algorithms or ['tfidf'],
            'attributes': attributes
        }
    
    def _load_products(self, brands, categories, types, competitors_df, attributes=None):
        conn = self.session.connection()
        conditions = []
        params = {}
        
        if not attributes:
            scoring_attrs = self.attr_service.get_attributes_by_type('default')
            attributes = [a.attribute_name for a in scoring_attrs]
        
        if brands and len(brands) > 0:
            placeholders = ','.join([':b' + str(i) for i in range(len(brands))])
            conditions.append(f"p.brand IN ({placeholders})")
            for i, b in enumerate(brands):
                params[f'b{i}'] = b
        if categories and len(categories) > 0:
            placeholders = ','.join([':c' + str(i) for i in range(len(categories))])
            conditions.append(f"p.category IN ({placeholders})")
            for i, c in enumerate(categories):
                params[f'c{i}'] = c
        if types and len(types) > 0:
            placeholders = ','.join([':t' + str(i) for i in range(len(types))])
            conditions.append(f"p.product_type IN ({placeholders})")
            for i, t in enumerate(types):
                params[f't{i}'] = t
        
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        
        select_cols = ["msp.product_id", "msp.system_product_id", "msp.name",
                      "COALESCE(p.brand, '') as brand", "COALESCE(p.category, '') as category",
                      "COALESCE(p.product_type, '') as product_type"]
        
        for attr in attributes:
            select_cols.append(f"COALESCE(msp.{attr}, '') as {attr}")
        
        query = text(f"""SELECT {', '.join(select_cols)}
                    FROM matching_system_product msp 
                    JOIN product p ON msp.system_product_id = p.system_product_id 
                    WHERE {where_clause}""")
        
        result = conn.execute(query, params) if params else conn.execute(query)
        
        columns = ['product_id', 'system_product_id', 'name', 'brand', 'category', 'product_type'] + attributes
        df = pd.DataFrame(result.fetchall(), columns=columns)
        return df
    
    def _load_competitors(self, attributes=None):
        conn = self.session.connection()
        
        if not attributes:
            scoring_attrs = self.attr_service.get_attributes_by_type('default')
            attributes = [a.attribute_name for a in scoring_attrs]
        
        select_cols = ["system_product_id"]
        for attr in attributes:
            select_cols.append(f"COALESCE({attr}, '') as competitor_{attr}")
        select_cols.append("competitor_product_id as competitor_id")
        
        query = text(f"SELECT {', '.join(select_cols)} FROM matching_competitor_product")
        result = conn.execute(query)
        
        columns = ['matching_product_id'] + [f'competitor_{attr}' for attr in attributes] + ['competitor_id']
        df = pd.DataFrame(result.fetchall(), columns=columns)
        
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
    
    def _save_scores(self, products_df, competitors_df, matcher, save_top_most=False, reset_on_first=False):
        try:
            conn = self.session.connection()
            saved_count = 0
            
            # Reset review_status for products being analyzed
            if reset_on_first and len(products_df) > 0:
                product_ids = products_df['product_id'].tolist()
                placeholders = ','.join([':p' + str(i) for i in range(len(product_ids))])
                reset_query = text(f"UPDATE matching_system_product SET review_status = 0, competitor_product_id = NULL, matched_date = NULL WHERE product_id IN ({placeholders})")
                params = {f'p{i}': pid for i, pid in enumerate(product_ids)}
                conn.execute(reset_query, params)
                self.session.commit()
            
            # Get or create configuration group
            config_group_id = self.config_service.get_or_create_config_group(
                weights=matcher.weights,
                thresholds=matcher.thresholds,
                price_config=matcher.price_config
            )
            
            for _, prod in products_df.iterrows():
                prod_id = str(prod["system_product_id"])
                internal_prod_id = prod["product_id"]  # For WHERE clause in UPDATE
                
                prod_data = {attr: prod.get(attr, '') for attr in matcher.attributes}
                
                comp_subset = competitors_df[competitors_df["matching_product_id"].astype(str) == prod_id]
                
                best_ref_id = None
                best_score = -1
                
                for _, comp in comp_subset.iterrows():
                    comp_data = {attr: comp.get(f"competitor_{attr}", '') for attr in matcher.attributes}
                    
                    result = matcher.calculate_score(prod_data, comp_data)
                    
                    # Calculate average score per attribute
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
                    
                    # Save to new schema with config_group_id
                    self.score_service.save_score(
                        system_product_id=int(internal_prod_id),
                        competitor_product_id=comp_id,
                        configuration_group_id=config_group_id,
                        total_score=final_score,
                        score_status=final_status,
                        attribute_scores=attr_scores
                    )
                    saved_count += 1
                    
                    if final_score > best_score:
                        best_score = final_score
                        best_ref_id = comp_id
                
                if save_top_most and best_ref_id:
                    from datetime import datetime
                    # Get fresh connection after save_score commits
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
            
            # Get product_id and current scores from competitor table using id (primary key)
            get_comp_query = text("SELECT system_product_id, score, score_status, sku_score, url_score, price_score FROM matching_competitor_product WHERE competitor_product_id = :comp_id")
            comp_result = conn.execute(get_comp_query, {'comp_id': comp_id})
            comp_row = comp_result.fetchone()
            
            if not comp_row:
                return {'success': False, 'error': 'Competitor not found'}
            
            product_id = comp_row[0]
            
            # Map action to review_status value
            status_map = {'approved': 1, 'rejected': 2, 'pending': 0}
            status_value = status_map.get(review_status, 0)
            
            # If approve or reject, update competitor_product_id (don't clear old scores)
            if review_status in ['approved', 'rejected']:
                # Update matching_system_product with competitor_product_id and review_status
                from datetime import datetime
                update_system_query = text("UPDATE matching_system_product SET competitor_product_id = :ref_id, matched_date = :match_date, review_status = :status WHERE product_id = :prod_id")
                conn.execute(update_system_query, {'ref_id': comp_id, 'match_date': datetime.now(), 'status': status_value, 'prod_id': product_id})
            else:
                # For pending, just update review_status
                update_query = text("UPDATE matching_system_product SET review_status = :status WHERE product_id = :prod_id")
                conn.execute(update_query, {'status': status_value, 'prod_id': product_id})
            
            self.session.commit()
            return {'success': True}
        except Exception as e:
            self.session.rollback()
            return {'success': False, 'error': str(e)}
    
    def get_total_grid_count(self, brands=None, categories=None, types=None, status_filter=None):
        try:
            conditions = []
            params = {}
            
            if brands and len(brands) > 0:
                placeholders = ','.join([':b' + str(i) for i in range(len(brands))])
                conditions.append(f"p.brand IN ({placeholders})")
                for i, b in enumerate(brands):
                    params[f'b{i}'] = b
            if categories and len(categories) > 0:
                placeholders = ','.join([':c' + str(i) for i in range(len(categories))])
                conditions.append(f"p.category IN ({placeholders})")
                for i, c in enumerate(categories):
                    params[f'c{i}'] = c
            if types and len(types) > 0:
                placeholders = ','.join([':t' + str(i) for i in range(len(types))])
                conditions.append(f"p.product_type IN ({placeholders})")
                for i, t in enumerate(types):
                    params[f't{i}'] = t
            if status_filter and len(status_filter) > 0:
                placeholders = ','.join([':s' + str(i) for i in range(len(status_filter))])
                conditions.append(f"msp.review_status IN ({placeholders})")
                for i, s in enumerate(status_filter):
                    params[f's{i}'] = s
            
            where_clause = " AND ".join(conditions) if conditions else "1=1"
            query = text(f"""SELECT COUNT(*) FROM matching_system_product msp
                    JOIN product p ON msp.system_product_id = p.system_product_id
                    WHERE {where_clause}""")
            
            with self.session.connection() as conn:
                result = conn.execute(query, params) if params else conn.execute(query)
                total = result.scalar()
            
            return {'total': total}
        except Exception as e:
            return {'error': str(e)}

    def get_matching_items_chunk(self, offset=0, limit=100, brands=None, categories=None, types=None, status_filter=None):
        try:
            conditions = []
            params = {'offset': offset, 'limit': limit}
            
            if brands and len(brands) > 0:
                placeholders = ','.join([':b' + str(i) for i in range(len(brands))])
                conditions.append(f"p.brand IN ({placeholders})")
                for i, b in enumerate(brands):
                    params[f'b{i}'] = b
            if categories and len(categories) > 0:
                placeholders = ','.join([':c' + str(i) for i in range(len(categories))])
                conditions.append(f"p.category IN ({placeholders})")
                for i, c in enumerate(categories):
                    params[f'c{i}'] = c
            if types and len(types) > 0:
                placeholders = ','.join([':t' + str(i) for i in range(len(types))])
                conditions.append(f"p.product_type IN ({placeholders})")
                for i, t in enumerate(types):
                    params[f't{i}'] = t
            if status_filter and len(status_filter) > 0:
                placeholders = ','.join([':s' + str(i) for i in range(len(status_filter))])
                conditions.append(f"msp.review_status IN ({placeholders})")
                for i, s in enumerate(status_filter):
                    params[f's{i}'] = s
            
            where_clause = " AND ".join(conditions) if conditions else "1=1"
            query = text(f"""SELECT msp.product_id, msp.system_product_id, msp.sku, msp.name, msp.price, msp.url,
                    p.brand, p.category, p.product_type, msp.review_status, msp.competitor_product_id,
                    (SELECT ms2.total_score FROM matching_scores ms2 WHERE ms2.system_product_id = msp.product_id ORDER BY ms2.total_score DESC LIMIT 1) as max_score,
                    (SELECT ms3.score_status FROM matching_scores ms3 WHERE ms3.system_product_id = msp.product_id ORDER BY ms3.total_score DESC LIMIT 1) as top_status,
                    (SELECT ms4.competitor_product_id FROM matching_scores ms4 WHERE ms4.system_product_id = msp.product_id ORDER BY ms4.total_score DESC LIMIT 1) as top_competitor_id
                    FROM matching_system_product msp
                    JOIN product p ON msp.system_product_id = p.system_product_id
                    WHERE {where_clause}
                    ORDER BY msp.product_id DESC
                    LIMIT :limit OFFSET :offset""")
            
            with self.session.connection() as conn:
                result = conn.execute(query, params)
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
                    'competitor_count': 0
                })
            
            return {'data': data}
        except Exception as e:
            return {'error': str(e)}

    def get_matching_items_grid(self, brands=None, categories=None, types=None, status_filter=None):
        try:
            conn = self.session.connection()
            conditions = []
            params = {}
            
            if brands and len(brands) > 0:
                placeholders = ','.join([':b' + str(i) for i in range(len(brands))])
                conditions.append(f"p.brand IN ({placeholders})")
                for i, b in enumerate(brands):
                    params[f'b{i}'] = b
            if categories and len(categories) > 0:
                placeholders = ','.join([':c' + str(i) for i in range(len(categories))])
                conditions.append(f"p.category IN ({placeholders})")
                for i, c in enumerate(categories):
                    params[f'c{i}'] = c
            if types and len(types) > 0:
                placeholders = ','.join([':t' + str(i) for i in range(len(types))])
                conditions.append(f"p.product_type IN ({placeholders})")
                for i, t in enumerate(types):
                    params[f't{i}'] = t
            if status_filter and len(status_filter) > 0:
                placeholders = ','.join([':s' + str(i) for i in range(len(status_filter))])
                conditions.append(f"msp.review_status IN ({placeholders})")
                for i, s in enumerate(status_filter):
                    params[f's{i}'] = s
            
            where_clause = " AND ".join(conditions) if conditions else "1=1"
            query = text(f"""SELECT msp.product_id, msp.system_product_id, msp.sku, msp.name, msp.price, msp.url,
                    p.brand, p.category, p.product_type, msp.review_status, msp.competitor_product_id,
                    (SELECT ms2.total_score FROM matching_scores ms2 WHERE ms2.system_product_id = msp.product_id ORDER BY ms2.total_score DESC LIMIT 1) as max_score,
                    (SELECT ms3.score_status FROM matching_scores ms3 WHERE ms3.system_product_id = msp.product_id ORDER BY ms3.total_score DESC LIMIT 1) as top_status,
                    (SELECT ms4.competitor_product_id FROM matching_scores ms4 WHERE ms4.system_product_id = msp.product_id ORDER BY ms4.total_score DESC LIMIT 1) as top_competitor_id
                    FROM matching_system_product msp
                    JOIN product p ON msp.system_product_id = p.system_product_id
                    WHERE {where_clause}
                    ORDER BY msp.product_id DESC""")
            
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
                    'competitor_count': 0
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
                prod_cols = ['msp.product_id', 'msp.system_product_id', 'msp.name', 'p.brand', 'p.category', 'p.product_type', 'msp.competitor_product_id', 'msp.review_status']
                for attr in attributes:
                    prod_cols.append(f'msp.{attr}')
                
                system_query = text(f"""SELECT {', '.join(prod_cols)}
                    FROM matching_system_product msp
                    JOIN product p ON msp.system_product_id = p.system_product_id
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
                    'product_type': system_row[5]
                }
                competitor_product_id = system_row[6]
                review_status = system_row[7]
                
                # Add dynamic attributes
                for i, attr in enumerate(attributes):
                    system_product[attr] = system_row[8 + i]
                
                # Build dynamic SELECT for competitors
                comp_cols = ['mcp.competitor_product_id']
                for attr in attributes:
                    comp_cols.append(f"COALESCE(mcp.{attr}, '') as {attr}")
                
                query = text(f"""SELECT {', '.join(comp_cols)}, ms.total_score, ms.score_status
                    FROM matching_competitor_product mcp
                    LEFT JOIN matching_scores ms ON mcp.competitor_product_id = ms.competitor_product_id 
                        AND ms.system_product_id = :pid
                    WHERE mcp.system_product_id = :spid LIMIT 1000""")
                result = conn.execute(query, {'pid': system_product['product_id'], 'spid': system_product['system_product_id']})
                rows = result.fetchall()
                
                # Get attribute scores for all competitors
                attr_scores_query = text("""SELECT msa.competitor_product_id, ma.attribute_name, msa.score
                    FROM matching_score_attributes msa
                    JOIN matching_attribute ma ON msa.attribute_id = ma.attribute_id
                    WHERE msa.system_product_id = :pid""")
                attr_result = conn.execute(attr_scores_query, {'pid': system_product['product_id']})
                
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

    def analyze_all_products(self, algorithms=None, attributes=None, weights=None, thresholds=None, price_config=None):
        try:
            conn = self.session.connection()
            
            # Get all brands with products that have competitors
            brand_query = text("""SELECT DISTINCT p.brand 
                FROM matching_system_product msp 
                JOIN product p ON msp.system_product_id = p.system_product_id 
                JOIN matching_competitor_product mcp ON msp.system_product_id = mcp.system_product_id 
                WHERE p.brand IS NOT NULL AND p.brand != '' 
                ORDER BY p.brand""")
            brand_result = conn.execute(brand_query)
            brands = [row[0] for row in brand_result]
            
            total_processed = 0
            total_products = 0
            
            # Count total products
            count_query = text("""SELECT COUNT(DISTINCT msp.product_id) 
                FROM matching_system_product msp 
                JOIN product p ON msp.system_product_id = p.system_product_id 
                JOIN matching_competitor_product mcp ON msp.system_product_id = mcp.system_product_id""")
            total_result = conn.execute(count_query)
            total_products = total_result.fetchone()[0]
            
            # Reset all scores
            reset_system_query = text("UPDATE matching_system_product SET competitor_product_id = NULL, matched_date = NULL, review_status = 0")
            conn.execute(reset_system_query)
            self.session.commit()
            
            # Process each brand
            for brand in brands:
                # Get categories for this brand
                cat_query = text("""SELECT DISTINCT p.category 
                    FROM matching_system_product msp 
                    JOIN product p ON msp.system_product_id = p.system_product_id 
                    JOIN matching_competitor_product mcp ON msp.system_product_id = mcp.system_product_id 
                    WHERE p.brand = :brand AND p.category IS NOT NULL AND p.category != '' 
                    ORDER BY p.category""")
                cat_result = conn.execute(cat_query, {'brand': brand})
                categories = [row[0] for row in cat_result]
                
                # Process each category
                for category in categories:
                    if not attributes:
                        scoring_attrs = self.attr_service.get_attributes_by_type('default')
                        attributes = [a.attribute_name for a in scoring_attrs]
                    
                    competitors_df = self._load_competitors(attributes)
                    products_df = self._load_products([brand], [category], None, competitors_df, attributes)
                    
                    if not products_df.empty:
                        matcher = ItemMatcher(algorithms=algorithms, attributes=attributes, weights=weights, thresholds=thresholds, price_config=price_config, session=self.session)
                        self._save_scores(products_df, competitors_df, matcher, save_top_most=True)
                        total_processed += len(products_df)
            
            return {
                'success': True,
                'total_products': total_products,
                'processed': total_processed,
                'brands_processed': len(brands)
            }
        except Exception as e:
            self.session.rollback()
            return {'success': False, 'error': str(e)}

    def get_brand_category_list(self, brands=None, categories=None, types=None):
        try:
            conn = self.session.connection()
            
            # Build query with optional filters
            conditions = ["p.brand IS NOT NULL", "p.brand != ''", "p.category IS NOT NULL", "p.category != ''"]
            params = {}
            
            if brands and len(brands) > 0:
                placeholders = ','.join([':b' + str(i) for i in range(len(brands))])
                conditions.append(f"p.brand IN ({placeholders})")
                for i, b in enumerate(brands):
                    params[f'b{i}'] = b
            
            if categories and len(categories) > 0:
                placeholders = ','.join([':c' + str(i) for i in range(len(categories))])
                conditions.append(f"p.category IN ({placeholders})")
                for i, c in enumerate(categories):
                    params[f'c{i}'] = c
            
            if types and len(types) > 0:
                placeholders = ','.join([':t' + str(i) for i in range(len(types))])
                conditions.append(f"p.product_type IN ({placeholders})")
                for i, t in enumerate(types):
                    params[f't{i}'] = t
            
            where_clause = " AND ".join(conditions)
            
            query = text(f"""SELECT DISTINCT p.brand, p.category
                FROM matching_system_product msp 
                JOIN product p ON msp.system_product_id = p.system_product_id 
                JOIN matching_competitor_product mcp ON msp.system_product_id = mcp.system_product_id 
                WHERE {where_clause}
                ORDER BY p.brand, p.category""")
            
            result = conn.execute(query, params) if params else conn.execute(query)
            
            brand_categories = {}
            for row in result:
                brand = row[0]
                category = row[1]
                if brand not in brand_categories:
                    brand_categories[brand] = []
                brand_categories[brand].append(category)
            
            return {'success': True, 'brand_categories': brand_categories}
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def analyze_brand_category(self, brand, category, algorithms=None, attributes=None, weights=None, thresholds=None, price_config=None, reset_scores=False):
        try:
            if reset_scores:
                conn = self.session.connection()
                reset_system_query = text("UPDATE matching_system_product SET competitor_product_id = NULL, matched_date = NULL, review_status = 0")
                conn.execute(reset_system_query)
                self.session.commit()
            
            # Get attributes if not provided
            if not attributes:
                scoring_attrs = self.attr_service.get_attributes_by_type('default')
                attributes = [a.attribute_name for a in scoring_attrs]
            
            # Load products for this brand/category
            competitors_df = self._load_competitors(attributes)
            products_df = self._load_products([brand], [category], None, competitors_df, attributes)
            
            if products_df.empty or competitors_df.empty:
                return {'success': True, 'processed': 0}
            
            matcher = ItemMatcher(algorithms=algorithms, attributes=attributes, weights=weights, thresholds=thresholds, price_config=price_config, session=self.session)
            self._save_scores(products_df, competitors_df, matcher, save_top_most=True)
            
            return {'success': True, 'processed': len(products_df), 'brand': brand, 'category': category}
        except Exception as e:
            self.session.rollback()
            import traceback
            traceback.print_exc()
            return {'success': False, 'error': str(e)}
    
    def get_brand_progress(self, brand):
        try:
            # Create a new session for this query to avoid connection issues
            from models.base.base import SessionLocal
            temp_session = SessionLocal()
            try:
                progress_query = text("""SELECT 
                    p.category,
                    COUNT(DISTINCT msp.product_id) as total_products,
                    COUNT(DISTINCT CASE WHEN ms.total_score IS NOT NULL THEN msp.product_id END) as processed_products,
                    COUNT(DISTINCT CASE WHEN ms.score_status = 'Matched' THEN msp.product_id END) as matched,
                    COUNT(DISTINCT CASE WHEN ms.score_status = 'Review' THEN msp.product_id END) as review,
                    COUNT(DISTINCT CASE WHEN ms.score_status = 'Not Matched' THEN msp.product_id END) as not_matched
                    FROM matching_system_product msp
                    JOIN product p ON msp.system_product_id = p.system_product_id
                    LEFT JOIN matching_scores ms ON msp.product_id = ms.system_product_id
                    WHERE p.brand = :brand
                    GROUP BY p.category
                    ORDER BY p.category""")
                result = temp_session.execute(progress_query, {'brand': brand})
                
                progress = {}
                for row in result:
                    category = row[0]
                    total = row[1]
                    processed = row[2]
                    matched = row[3]
                    review = row[4]
                    not_matched = row[5]
                    
                    progress[category] = {
                        'total': total,
                        'processed': processed,
                        'completed': processed >= total,
                        'matched': matched,
                        'review': review,
                        'not_matched': not_matched
                    }
                
                return {brand: progress}
            finally:
                temp_session.close()
        except Exception as e:
            print(f"Error getting brand progress: {e}")
            import traceback
            traceback.print_exc()
            return {}
        try:
            conn = self.session.connection()
            count_query = text("""SELECT COUNT(DISTINCT msp.product_id) 
                FROM matching_system_product msp 
                JOIN product p ON msp.system_product_id = p.system_product_id 
                JOIN matching_competitor_product mcp ON msp.system_product_id = mcp.system_product_id""")
            result = conn.execute(count_query)
            total = result.fetchone()[0]
            return {'success': True, 'total': total}
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def get_analysis_stats(self):
        try:
            with self.session.connection() as conn:
                stats_query = text("""SELECT 
                    COUNT(DISTINCT CASE WHEN score_status = 'Matched' THEN system_product_id END) as matched,
                    COUNT(DISTINCT CASE WHEN score_status = 'Review' THEN system_product_id END) as review,
                    COUNT(DISTINCT CASE WHEN score_status = 'Not Matched' THEN system_product_id END) as not_matched
                    FROM matching_scores
                    WHERE total_score IS NOT NULL""")
                result = conn.execute(stats_query)
                row = result.fetchone()
                return {'matched': row[0] or 0, 'review': row[1] or 0, 'not_matched': row[2] or 0}
        except Exception as e:
            return {'matched': 0, 'review': 0, 'not_matched': 0}
    
    def analyze_chunk_by_range(self, offset, limit, algorithms=None, attributes=None, weights=None, thresholds=None, price_config=None, reset_scores=False):
        try:
            if reset_scores:
                conn = self.session.connection()
                reset_system_query = text("UPDATE matching_system_product SET competitor_product_id = NULL, matched_date = NULL, review_status = 0")
                conn.execute(reset_system_query)
                self.session.commit()
            
            # Get product IDs
            conn = self.session.connection()
            product_query = text("""SELECT DISTINCT msp.product_id
                FROM matching_system_product msp 
                JOIN product p ON msp.system_product_id = p.system_product_id 
                JOIN matching_competitor_product mcp ON msp.system_product_id = mcp.system_product_id
                ORDER BY msp.product_id
                LIMIT :limit OFFSET :offset""")
            result = conn.execute(product_query, {'limit': limit, 'offset': offset})
            product_ids = [row[0] for row in result.fetchall()]
            
            if not product_ids:
                return {'success': True, 'processed': 0, 'last_product': None}
            
            # Get attributes if not provided
            if not attributes:
                scoring_attrs = self.attr_service.get_attributes_by_type('default')
                attributes = [a.attribute_name for a in scoring_attrs]
            
            # Load products and competitors
            import pandas as pd
            
            # Build dynamic columns for products
            prod_cols = ['product_id', 'system_product_id', 'sku', 'name', 'brand', 'category', 'product_type'] + attributes
            prod_select = ['msp.product_id', 'msp.system_product_id', 'msp.sku', 'msp.name',
                          'COALESCE(p.brand, \'\') as brand', 'COALESCE(p.category, \'\') as category',
                          'COALESCE(p.product_type, \'\') as product_type']
            for attr in attributes:
                prod_select.append(f"COALESCE(msp.{attr}, '') as {attr}")
            
            products_query = text(f"""SELECT {', '.join(prod_select)}
                FROM matching_system_product msp 
                JOIN product p ON msp.system_product_id = p.system_product_id 
                WHERE msp.product_id IN :ids""")
            products_result = conn.execute(products_query, {'ids': tuple(product_ids)})
            products_df = pd.DataFrame(products_result.fetchall(), columns=prod_cols)
            
            # Build dynamic columns for competitors
            comp_cols = ['matching_product_id', 'competitor_id'] + [f'competitor_{attr}' for attr in attributes]
            comp_select = ['system_product_id', 'competitor_product_id as competitor_id']
            for attr in attributes:
                comp_select.append(f"COALESCE({attr}, '') as competitor_{attr}")
            
            comp_query = text(f"SELECT {', '.join(comp_select)} FROM matching_competitor_product WHERE system_product_id IN :ids")
            comp_result = conn.execute(comp_query, {'ids': tuple(product_ids)})
            competitors_df = pd.DataFrame(comp_result.fetchall(), columns=comp_cols)
            
            if products_df.empty or competitors_df.empty:
                return {'success': True, 'processed': 0, 'last_product': None}
            
            matcher = ItemMatcher(algorithms=algorithms, attributes=attributes, weights=weights, thresholds=thresholds, price_config=price_config, session=self.session)
            self._save_scores(products_df, competitors_df, matcher, save_top_most=True)
            
            last_product = products_df.iloc[-1]
            last_product_info = {
                'sku': last_product['sku'],
                'name': last_product['name'],
                'brand': last_product['brand'],
                'category': last_product['category']
            }
            
            # Get brand/category progress with a fresh connection - only for current chunk
            brand_category_progress = self._get_brand_category_progress(products_df)
            
            return {'success': True, 'processed': len(products_df), 'last_product': last_product_info, 'brand_category_progress': brand_category_progress}
        except Exception as e:
            self.session.rollback()
            import traceback
            traceback.print_exc()
            return {'success': False, 'error': str(e), 'last_product': None}
    
    def _get_brand_category_progress(self, products_df):
        try:
            # Get unique brands and categories from current chunk
            brands = products_df['brand'].unique().tolist()
            categories = products_df['category'].unique().tolist()
            
            if not brands or not categories:
                return {}
            
            conn = self.session.connection()
            
            # Build query for only current brands/categories
            brand_placeholders = ','.join([f':b{i}' for i in range(len(brands))])
            cat_placeholders = ','.join([f':c{i}' for i in range(len(categories))])
            params = {f'b{i}': b for i, b in enumerate(brands)}
            params.update({f'c{i}': c for i, c in enumerate(categories)})
            
            progress_query = text(f"""SELECT 
                p.brand, 
                p.category,
                COUNT(DISTINCT msp.product_id) as total_products,
                COUNT(DISTINCT CASE WHEN ms.total_score IS NOT NULL THEN msp.product_id END) as processed_products,
                COUNT(DISTINCT CASE WHEN ms.score_status = 'Matched' THEN msp.product_id END) as matched,
                COUNT(DISTINCT CASE WHEN ms.score_status = 'Review' THEN msp.product_id END) as review,
                COUNT(DISTINCT CASE WHEN ms.score_status = 'Not Matched' THEN msp.product_id END) as not_matched
                FROM matching_system_product msp
                JOIN product p ON msp.system_product_id = p.system_product_id
                LEFT JOIN matching_scores ms ON msp.product_id = ms.system_product_id
                WHERE p.brand IN ({brand_placeholders}) AND p.category IN ({cat_placeholders})
                GROUP BY p.brand, p.category
                ORDER BY p.brand, p.category""")
            result = conn.execute(progress_query, params)
            
            progress = {}
            for row in result:
                brand = row[0]
                category = row[1]
                total = row[2]
                processed = row[3]
                matched = row[4]
                review = row[5]
                not_matched = row[6]
                
                if brand not in progress:
                    progress[brand] = {}
                
                progress[brand][category] = {
                    'total': total,
                    'processed': processed,
                    'completed': processed >= total,
                    'matched': matched,
                    'review': review,
                    'not_matched': not_matched
                }
            
            return progress
        except Exception as e:
            print(f"Error getting brand/category progress: {e}")
            import traceback
            traceback.print_exc()
            return {}
