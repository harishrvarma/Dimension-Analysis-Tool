from models.base.base import SessionLocal
from repositories.product_repository import ProductRepository
import pandas as pd
import numpy as np
from sqlalchemy import text
from services.item_match.algorithms import AlgorithmFactory
from services.item_match.attribute_service import AttributeService
from services.item_match.score_service import ScoreService


class ItemMatcherRefactored:
    """Multi-algorithm item matching with dynamic attributes"""
    
    STATUS_MATCHED = "Matched"
    STATUS_REVIEW = "Review"
    STATUS_NOT_MATCHED = "Not Matched"
    
    def __init__(self, algorithms=None, attribute_names=None, weights=None, thresholds=None, price_config=None, session=None):
        self.session = session or SessionLocal()
        self._should_close = session is None
        
        # Load dynamic attributes
        self.attr_service = AttributeService(self.session)
        self.attr_dict = self.attr_service.get_attribute_dict()
        
        # Filter attributes by provided names or use all
        if attribute_names:
            self.attributes = {k: v for k, v in self.attr_dict.items() if k in attribute_names}
        else:
            self.attributes = self.attr_dict
        
        self.algorithms = algorithms or ['tfidf']
        self.weights = weights or {name: info['weightage'] for name, info in self.attributes.items()}
        self.thresholds = thresholds or {'matched': 85, 'review': 70}
        self.price_config = price_config or {'margin': 25, 'margin_diff': 10}
        self.algo_instances = {algo: AlgorithmFactory.get_algorithm(algo, self.price_config) for algo in self.algorithms}
    
    def __del__(self):
        if self._should_close and hasattr(self, 'session'):
            self.session.close()
    
    def calculate_score(self, product_data, competitor_data):
        """Calculate scores dynamically based on attributes"""
        scores = {}
        
        for attr_name, attr_info in self.attributes.items():
            attr_type = attr_info['type']
            
            # Get values from data
            prod_val = product_data.get(attr_name, '')
            comp_val = competitor_data.get(attr_name, '')
            
            # Score based on attribute type
            for algo_name in self.algorithms:
                algo = self.algo_instances[algo_name]
                
                if attr_type == 'price':
                    # Use price algorithm
                    if algo_name in ['custom', 'tfidf_price']:
                        score_val = algo.score_price(prod_val, comp_val)
                    else:
                        score_val = algo.score(str(prod_val), str(comp_val))
                elif attr_type == 'default':
                    # Use standard algorithm
                    if algo_name == 'custom':
                        if attr_name == 'sku':
                            score_val = algo.score_sku(prod_val, comp_val)
                        elif attr_name == 'url':
                            score_val = algo.score_url(prod_val, comp_val)
                        else:
                            score_val = algo.score(str(prod_val), str(comp_val))
                    else:
                        score_val = algo.score(str(prod_val), str(comp_val))
                else:
                    # Status type - not scored
                    continue
                
                scores[f'{attr_name}_{algo_name}'] = score_val
        
        # Calculate final score
        total_weight = 0
        weighted_sum = 0
        
        for attr_name in self.attributes.keys():
            if self.attributes[attr_name]['type'] == 'status':
                continue
            attr_scores = [v for k, v in scores.items() if k.startswith(f'{attr_name}_')]
            if attr_scores:
                avg_score = sum(attr_scores) / len(attr_scores)
                weighted_sum += avg_score * self.weights.get(attr_name, 0)
                total_weight += self.weights.get(attr_name, 0)
        
        final_score = (weighted_sum / total_weight) if total_weight > 0 else 0.0
        scores['final_score'] = round(final_score, 2)
        
        return {k: round(v, 2) for k, v in scores.items()}
    
    def get_status(self, score: float) -> str:
        if score >= self.thresholds['matched']:
            return self.STATUS_MATCHED
        elif score >= self.thresholds['review']:
            return self.STATUS_REVIEW
        else:
            return self.STATUS_NOT_MATCHED
    
    def match_items(self, products_df: pd.DataFrame, competitors_df: pd.DataFrame):
        summary_rows = []
        
        for _, prod in products_df.iterrows():
            prod_id = str(prod["product_id"])
            
            # Build product data dict from attributes
            prod_data = {attr_name: prod.get(attr_name, '') for attr_name in self.attributes.keys()}
            
            comp_subset = competitors_df[competitors_df["matching_product_id"].astype(str) == prod_id]
            
            if comp_subset.empty:
                continue
            
            scores = []
            statuses = []
            
            for _, comp in comp_subset.iterrows():
                # Build competitor data dict
                comp_data = {attr_name: comp.get(f"competitor_{attr_name}", '') for attr_name in self.attributes.keys()}
                
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


class ItemMatchServiceRefactored:
    """Refactored service using dynamic attributes and new schema"""
    
    def __init__(self):
        self.session = SessionLocal()
        self.product_repo = ProductRepository(self.session)
        self.attr_service = AttributeService(self.session)
        self.score_service = ScoreService(self.session)
    
    def __del__(self):
        if hasattr(self, 'session'):
            self.session.close()
    
    def get_available_attributes(self):
        """Return available attributes for UI"""
        attrs = self.attr_service.get_all_attributes()
        return [
            {
                'value': attr.attribute_name,
                'label': attr.attribute_name.upper(),
                'type': attr.attribute_type,
                'default_weight': attr.default_weightage
            }
            for attr in attrs
        ]
    
    def run_analysis(self, brands=None, categories=None, types=None, algorithms=None, 
                    attribute_names=None, weights=None, thresholds=None, price_config=None, 
                    save_score=False, save_top_most=False):
        
        # Load dynamic attributes if not specified
        if not attribute_names:
            attribute_names = self.attr_service.get_attribute_names()
        
        competitors_df = self._load_competitors(attribute_names)
        if competitors_df.empty:
            return {'error': 'No competitor data available'}
        
        products_df = self._load_products(brands, categories, types, competitors_df, attribute_names)
        if products_df.empty:
            return {'error': 'No products found'}
        
        # Reset scores if save_score is checked
        if save_score:
            product_ids = products_df['product_id'].tolist()
            if product_ids:
                self.score_service.reset_scores(product_ids)
                # Also reset matched_ref_id in system product
                conn = self.session.connection()
                placeholders = ','.join([f':p{i}' for i in range(len(product_ids))])
                params = {f'p{i}': pid for i, pid in enumerate(product_ids)}
                reset_query = text(f"UPDATE matching_system_product SET matched_ref_id = NULL, matched_date = NULL, review_status = 0 WHERE product_id IN ({placeholders})")
                conn.execute(reset_query, params)
                self.session.commit()
        
        matcher = ItemMatcherRefactored(algorithms, attribute_names, weights, thresholds, price_config, self.session)
        summary = matcher.match_items(products_df, competitors_df)
        
        if summary.empty:
            return {'error': 'No products with competitors found'}
        
        if save_score or save_top_most:
            self._save_scores(products_df, competitors_df, matcher, save_top_most)
        
        return {
            'summary': summary.to_dict('records'),
            'algorithms': algorithms or ['tfidf'],
            'attributes': attribute_names,
            'stats': {
                'total': len(summary),
                'matched': int((summary['status'] == 'Matched').sum()),
                'review': int((summary['status'] == 'Review').sum()),
                'not_matched': int((summary['status'] == 'Not Matched').sum())
            }
        }
    
    def _load_products(self, brands, categories, types, competitors_df, attribute_names):
        """Load products with dynamic attributes"""
        conn = self.session.connection()
        conditions = []
        params = {}
        
        if brands and len(brands) > 0:
            placeholders = ','.join([f':b{i}' for i in range(len(brands))])
            conditions.append(f"p.brand IN ({placeholders})")
            for i, b in enumerate(brands):
                params[f'b{i}'] = b
        if categories and len(categories) > 0:
            placeholders = ','.join([f':c{i}' for i in range(len(categories))])
            conditions.append(f"p.category IN ({placeholders})")
            for i, c in enumerate(categories):
                params[f'c{i}'] = c
        if types and len(types) > 0:
            placeholders = ','.join([f':t{i}' for i in range(len(types))])
            conditions.append(f"p.product_type IN ({placeholders})")
            for i, t in enumerate(types):
                params[f't{i}'] = t
        
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        
        # Build SELECT dynamically based on attributes
        select_cols = ["msp.product_id", "msp.system_product_id", "msp.name",
                      "COALESCE(p.brand, '') as brand", "COALESCE(p.category, '') as category",
                      "COALESCE(p.product_type, '') as product_type"]
        
        for attr_name in attribute_names:
            select_cols.append(f"COALESCE(msp.{attr_name}, '') as {attr_name}")
        
        query = text(f"""SELECT {', '.join(select_cols)}
                    FROM matching_system_product msp 
                    JOIN product p ON msp.system_product_id = p.system_product_id 
                    WHERE {where_clause}""")
        
        result = conn.execute(query, params) if params else conn.execute(query)
        
        columns = ['product_id', 'system_product_id', 'name', 'brand', 'category', 'product_type'] + attribute_names
        df = pd.DataFrame(result.fetchall(), columns=columns)
        return df
    
    def _load_competitors(self, attribute_names):
        """Load competitors with dynamic attributes"""
        conn = self.session.connection()
        
        # Build SELECT dynamically
        select_cols = ["product_id", "id as competitor_id"]
        for attr_name in attribute_names:
            select_cols.append(f"COALESCE({attr_name}, '') as competitor_{attr_name}")
        
        query = text(f"SELECT {', '.join(select_cols)} FROM matching_competitor_product")
        result = conn.execute(query)
        
        columns = ['matching_product_id', 'competitor_id'] + [f'competitor_{attr}' for attr in attribute_names]
        df = pd.DataFrame(result.fetchall(), columns=columns)
        
        return df
    
    def _save_scores(self, products_df, competitors_df, matcher, save_top_most=False):
        """Save scores using new schema"""
        try:
            conn = self.session.connection()
            saved_count = 0
            
            for _, prod in products_df.iterrows():
                prod_id = str(prod["product_id"])
                
                # Build product data
                prod_data = {attr_name: prod.get(attr_name, '') for attr_name in matcher.attributes.keys()}
                
                comp_subset = competitors_df[competitors_df["matching_product_id"].astype(str) == prod_id]
                
                best_ref_id = None
                best_score = -1
                
                for _, comp in comp_subset.iterrows():
                    # Build competitor data
                    comp_data = {attr_name: comp.get(f"competitor_{attr_name}", '') for attr_name in matcher.attributes.keys()}
                    
                    result = matcher.calculate_score(prod_data, comp_data)
                    
                    # Calculate average score per attribute
                    attr_scores = {}
                    for attr_name, attr_info in matcher.attributes.items():
                        if attr_info['type'] == 'status':
                            continue
                        scores_for_attr = [v for k, v in result.items() if k.startswith(f'{attr_name}_')]
                        if scores_for_attr:
                            avg = sum(scores_for_attr) / len(scores_for_attr)
                            attr_scores[attr_info['id']] = round(avg, 2)
                    
                    final_score = result['final_score']
                    final_status = matcher.get_status(final_score)
                    
                    # Get competitor_product_id (id from matching_competitor_product)
                    comp_id = int(comp['competitor_id'])
                    
                    # Save to new schema
                    self.score_service.save_score(
                        system_product_ref_id=int(prod_id),
                        competitor_product_id=comp_id,
                        configuration_group_id=None,
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
                    update_query = text("UPDATE matching_system_product SET matched_ref_id = :ref_id, matched_date = :match_date WHERE product_id = :prod_id")
                    conn.execute(update_query, {'ref_id': best_ref_id, 'match_date': datetime.now(), 'prod_id': prod_id})
            
            self.session.commit()
            print(f"Saved {saved_count} scores to new schema")
        except Exception as e:
            self.session.rollback()
            print(f"Error saving scores: {e}")
            import traceback
            traceback.print_exc()
