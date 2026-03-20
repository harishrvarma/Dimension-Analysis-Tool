from models.pricing.product_iteration import ProductIteration
from datetime import datetime
from sqlalchemy import and_


class ProductIterationRepository:
    def __init__(self, db):
        self.db = db

    def find_existing_iteration(self, brand, category, product_types, eps, sample, algorithm, product_group_id):
        """Find existing iteration matching criteria"""
        sorted_types = '|'.join(sorted(product_types)) if isinstance(product_types, list) else product_types
        
        try:
            iteration = self.db.query(ProductIteration).filter(
                ProductIteration.brand == brand,
                ProductIteration.category == category,
                ProductIteration.product_type == sorted_types,
                ProductIteration.eps == eps,
                ProductIteration.sample == sample,
                ProductIteration.algorithm == algorithm,
                ProductIteration.product_group_id == product_group_id
            ).order_by(ProductIteration.timestamp.desc()).first()
            
            return iteration
        except Exception as e:
            print(f"Error finding existing iteration: {e}")
            return None

    def delete_iteration_with_items(self, iteration_id):
        """Delete iteration and its items"""
        from repositories.pricing.product_iteration_item_repository import PricingProductIterationItemRepository
        
        try:
            item_repo = PricingProductIterationItemRepository(self.db)
            item_repo.delete_by_iteration_id(iteration_id)
            
            self.db.query(ProductIteration).filter(
                ProductIteration.iteration_id == iteration_id
            ).delete(synchronize_session=False)
            
            self.db.commit()
            return True
        except Exception as e:
            self.db.rollback()
            print(f"Error deleting iteration with items: {e}")
            return False

    def save_iteration(self, brand, category, product_types, product_group_id, algorithm, eps, sample, unique_number=None, total_items=None, analyzed_items=None, pending_items=None, outlier_items=None, x_axis=None, y_axis=None, z_axis=None, x_axis_com=None, y_axis_com=None, z_axis_com=None):
        """Save new iteration and return iteration_id"""
        sorted_types = '|'.join(sorted(product_types)) if isinstance(product_types, list) else product_types
        
        try:
            iteration = ProductIteration(
                product_group_id=product_group_id,
                algorithm=algorithm,
                brand=brand,
                category=category,
                product_type=sorted_types,
                eps=eps,
                sample=sample,
                timestamp=datetime.now(),
                unique_number=unique_number,
                total_items=total_items,
                analyzed_items=analyzed_items,
                pending_items=pending_items,
                outlier_items=outlier_items,
                x_axis=x_axis,
                y_axis=y_axis,
                z_axis=z_axis,
                x_axis_com=x_axis_com,
                y_axis_com=y_axis_com,
                z_axis_com=z_axis_com
            )
            self.db.add(iteration)
            self.db.flush()
            
            return iteration.iteration_id
        except Exception as e:
            self.db.rollback()
            print(f"Error saving iteration: {e}")
            return None

    def save_iteration_results(self, iteration_data_list):
        """Save multiple iteration results"""
        try:
            for data in iteration_data_list:
                iteration_record = ProductIteration(
                    system_product_id=data['system_product_id'],
                    iteration_number=data['iteration_number'],
                    algo_id=data['algo_id'],
                    brand=data.get('brand'),
                    category=data.get('category'),
                    timezone=datetime.now(),
                    eps=data.get('eps'),
                    sample=data.get('sample'),
                    cluster=data.get('cluster'),
                    outlier_mode=data.get('outlier_mode', 0),
                    status=data.get('status')
                )
                self.db.add(iteration_record)
            
            self.db.commit()
            return True
        except Exception as e:
            self.db.rollback()
            print(f"Error saving iteration results: {e}")
            return False
    
    def get_iterations_by_brand_category(self, brands, category):
        """Get all iteration data for given brand(s) and category"""
        try:
            query = self.db.query(ProductIteration).filter(
                ProductIteration.category == category
            )
            
            # Filter by brands if provided
            if brands and len(brands) > 0:
                brand_str = ', '.join(brands)
                query = query.filter(ProductIteration.brand == brand_str)
            
            results = query.all()
            
            return [{
                'system_product_id': r.system_product_id,
                'iteration_number': r.iteration_number,
                'algo_id': r.algo_id,
                'status': r.status,
                'outlier_mode': r.outlier_mode
            } for r in results]
        except Exception as e:
            print(f"Error fetching iterations by brand/category: {e}")
            return []
    
    def get_products_for_iteration(self, brands, category, iteration_number, analysis_mode='all'):
        """Get products for a specific iteration based on analysis mode"""
        from sqlalchemy import func
        from models.pricing.product import Product
        
        try:
            # Build brand filter
            brand_str = ', '.join(brands) if brands and len(brands) > 0 else None
            
            # Get products from previous iteration
            prev_iteration = iteration_number - 1
            
            if analysis_mode == 'all':
                # For 'all' mode: get all products from product table (like iteration 1)
                query = self.db.query(Product).filter(
                    Product.category == category,
                    Product.mfr_cost.isnot(None),
                    Product.shipping_cost.isnot(None),
                    Product.profit_margin.isnot(None)
                )
                
                if brands and len(brands) > 0:
                    query = query.filter(Product.brand.in_(brands))
                
                return query.all()
            else:
                # For 'normal' mode: get only normal products from previous iteration
                subquery = self.db.query(
                    ProductIteration.system_product_id,
                    func.max(ProductIteration.status).label('status')
                ).filter(
                    ProductIteration.category == category,
                    ProductIteration.iteration_number == prev_iteration
                )
                
                if brand_str:
                    subquery = subquery.filter(ProductIteration.brand == brand_str)
                
                subquery = subquery.group_by(ProductIteration.system_product_id).subquery()
                
                query = self.db.query(Product).join(
                    subquery,
                    Product.system_product_id == subquery.c.system_product_id
                ).filter(subquery.c.status == 1)
                
                return query.all()
            
        except Exception as e:
            print(f"Error fetching products for iteration: {e}")
            return []

    def update_cluster_outliers_in_iteration(self, system_product_ids, iteration_number, brands, category):
        """Update outlier status for cluster products in product_iteration table"""
        from sqlalchemy import and_
        
        if not system_product_ids:
            return
        
        brand_str = ', '.join(brands) if brands and len(brands) > 0 else None
        
        # Update all records for these products in the current iteration
        query = self.db.query(ProductIteration).filter(
            and_(
                ProductIteration.system_product_id.in_(system_product_ids),
                ProductIteration.iteration_number == iteration_number,
                ProductIteration.category == category
            )
        )
        
        if brand_str:
            query = query.filter(ProductIteration.brand == brand_str)
        
        # Update status to outlier (0) and outlier_mode to manual (1)
        query.update({
            'status': 0,
            'outlier_mode': 1
        }, synchronize_session=False)
        
        self.db.commit()

    def is_iteration_saved(self, brands, category, iteration_number):
        """Check if an iteration is saved in the database"""
        brand_str = ', '.join(brands) if brands and len(brands) > 0 else None
        
        query = self.db.query(ProductIteration).filter(
            ProductIteration.category == category,
            ProductIteration.iteration_number == iteration_number
        )
        
        if brand_str:
            query = query.filter(ProductIteration.brand == brand_str)
        
        return query.first() is not None

    def delete_iterations_by_brand_category(self, brands, category):
        """Delete all iteration records for given brand(s) and category"""
        try:
            brand_str = ', '.join(brands) if brands and len(brands) > 0 else None
            
            query = self.db.query(ProductIteration).filter(
                ProductIteration.category == category
            )
            
            if brand_str:
                query = query.filter(ProductIteration.brand == brand_str)
            
            query.delete(synchronize_session=False)
            self.db.commit()
            return True
        except Exception as e:
            self.db.rollback()
            print(f"Error deleting iterations: {e}")
            return False

    def delete_by_filters(self, group_id, brands=None, category=None, types=None):
        """Delete iteration records based on filters"""
        from models.pricing.product import Product
        
        try:
            # Get system_product_ids matching filters
            query = self.db.query(Product.system_product_id).filter(
                Product.group_id == group_id
            )
            
            if brands and len(brands) > 0:
                query = query.filter(Product.brand.in_(brands))
            
            if category:
                query = query.filter(Product.category == category)
            
            if types and len(types) > 0:
                query = query.filter(Product.product_type.in_(types))
            
            system_product_ids = [r[0] for r in query.all()]
            
            if system_product_ids:
                self.db.query(ProductIteration).filter(
                    ProductIteration.system_product_id.in_(system_product_ids)
                ).delete(synchronize_session=False)
                self.db.commit()
            
            return True
        except Exception as e:
            self.db.rollback()
            print(f"Error deleting iterations by filters: {e}")
            return False


    def delete_by_filters(self, product_group_id, brand=None, category=None, product_types=None, eps=None, sample=None, algorithm=None):
        """Delete iterations by filters"""
        from models.pricing.product_iteration import ProductIteration
        
        try:
            query = self.db.query(ProductIteration).filter(
                ProductIteration.product_group_id == product_group_id
            )
            
            if brand:
                query = query.filter(ProductIteration.brand == brand)
            if category:
                query = query.filter(ProductIteration.category == category)
            if product_types:
                sorted_types = '|'.join(sorted(product_types)) if isinstance(product_types, list) else product_types
                query = query.filter(ProductIteration.product_type == sorted_types)
            if eps is not None and eps != 0.1:
                query = query.filter(ProductIteration.eps == eps)
            if sample is not None and sample != 1:
                query = query.filter(ProductIteration.sample == sample)
            if algorithm:
                query = query.filter(ProductIteration.algorithm == algorithm)
            
            iterations = query.all()
            
            for iteration in iterations:
                self.delete_iteration_with_items(iteration.iteration_id)
            
            return True
        except Exception as e:
            print(f"Error deleting iterations by filters: {e}")
            return False

    def get_iteration_summary_by_group_brand(self, group_id, brand_ids):
        """Get iteration summary filtered by brand IDs only (no category filter)"""
        from sqlalchemy import func, text
        from models.pricing.product_iteration_item import PricingProductIterationItem
        from models.pricing.product import Product

        try:
            brand_conditions = " OR ".join(
                [f"FIND_IN_SET(:bid{i}, pricing_product_iteration.brand) > 0" for i in range(len(brand_ids))]
            )
            bind_params = {f"bid{i}": str(bid) for i, bid in enumerate(brand_ids)}

            q = self.db.query(
                ProductIteration.iteration_id,
                ProductIteration.eps,
                ProductIteration.sample,
                ProductIteration.x_axis,
                ProductIteration.y_axis,
                ProductIteration.z_axis
            ).join(
                PricingProductIterationItem,
                ProductIteration.iteration_id == PricingProductIterationItem.iteration_id
            ).filter(
                ProductIteration.product_group_id == group_id,
                text(f"({brand_conditions})").bindparams(**bind_params)
            ).group_by(
                ProductIteration.iteration_id,
                ProductIteration.eps,
                ProductIteration.sample
            ).order_by(
                ProductIteration.iteration_id.asc()
            ).all()

            summary = []
            for row in q:
                iter_id = row.iteration_id
                total = self.db.query(func.count(PricingProductIterationItem.id)).join(
                    Product, PricingProductIterationItem.system_product_id == Product.system_product_id
                ).filter(
                    PricingProductIterationItem.iteration_id == iter_id,
                    (Product.skip_status.is_(None)) | (Product.skip_status != 1)
                ).scalar() or 0
                normal = self.db.query(func.count(PricingProductIterationItem.id)).join(
                    Product, PricingProductIterationItem.system_product_id == Product.system_product_id
                ).filter(
                    PricingProductIterationItem.iteration_id == iter_id,
                    PricingProductIterationItem.status == 1,
                    PricingProductIterationItem.final_status.is_(None),
                    (Product.skip_status.is_(None)) | (Product.skip_status != 1)
                ).scalar() or 0
                outlier = self.db.query(func.count(PricingProductIterationItem.id)).join(
                    Product, PricingProductIterationItem.system_product_id == Product.system_product_id
                ).filter(
                    PricingProductIterationItem.iteration_id == iter_id,
                    PricingProductIterationItem.status == 0,
                    PricingProductIterationItem.final_status.is_(None),
                    (Product.skip_status.is_(None)) | (Product.skip_status != 1)
                ).scalar() or 0
                manual_outlier = self.db.query(func.count(PricingProductIterationItem.id)).join(
                    Product, PricingProductIterationItem.system_product_id == Product.system_product_id
                ).filter(
                    PricingProductIterationItem.iteration_id == iter_id,
                    PricingProductIterationItem.final_status == 0,
                    (Product.skip_status.is_(None)) | (Product.skip_status != 1)
                ).scalar() or 0
                manual_normal = self.db.query(func.count(PricingProductIterationItem.id)).join(
                    Product, PricingProductIterationItem.system_product_id == Product.system_product_id
                ).filter(
                    PricingProductIterationItem.iteration_id == iter_id,
                    PricingProductIterationItem.final_status == 1,
                    (Product.skip_status.is_(None)) | (Product.skip_status != 1)
                ).scalar() or 0
                summary.append({
                    'iteration': iter_id,
                    'iteration_id': iter_id,
                    'eps': float(row.eps) if row.eps else None,
                    'sample': int(row.sample) if row.sample else None,
                    'x_axis': row.x_axis,
                    'y_axis': row.y_axis,
                    'z_axis': row.z_axis,
                    'total_count': int(total),
                    'normal_count': int(normal),
                    'outlier_count': int(outlier),
                    'manual_outlier_count': int(manual_outlier),
                    'manual_normal_count': int(manual_normal)
                })
            return summary
        except Exception as e:
            print(f"Error getting iteration summary by brand: {e}")
            return []

    def get_iteration_summary_by_group_category(self, group_id, category, brand_ids=None):
        """Get iteration summary by group_id and category"""
        from sqlalchemy import func, text
        from models.pricing.product_iteration_item import PricingProductIterationItem
        from models.pricing.product import Product
        
        try:
            q = self.db.query(
                ProductIteration.iteration_id,
                ProductIteration.eps,
                ProductIteration.sample,
                ProductIteration.x_axis,
                ProductIteration.y_axis,
                ProductIteration.z_axis
            ).join(
                PricingProductIterationItem,
                ProductIteration.iteration_id == PricingProductIterationItem.iteration_id
            ).filter(
                ProductIteration.product_group_id == group_id,
                text("FIND_IN_SET(:cat, pricing_product_iteration.category) > 0").bindparams(cat=str(category))
            )

            if brand_ids:
                brand_conditions = " OR ".join(
                    [f"FIND_IN_SET(:bid{i}, pricing_product_iteration.brand) > 0" for i in range(len(brand_ids))]
                )
                bind_params = {f"bid{i}": str(bid) for i, bid in enumerate(brand_ids)}
                q = q.filter(text(f"({brand_conditions})").bindparams(**bind_params))

            results = q.group_by(
                ProductIteration.iteration_id,
                ProductIteration.eps,
                ProductIteration.sample
            ).order_by(
                ProductIteration.iteration_id.asc()
            ).all()
            
            summary = []
            for row in results:
                iter_id = row.iteration_id
                
                # Get counts for this iteration, excluding skipped products
                total = self.db.query(func.count(PricingProductIterationItem.id)).join(
                    Product,
                    PricingProductIterationItem.system_product_id == Product.system_product_id
                ).filter(
                    PricingProductIterationItem.iteration_id == iter_id,
                    (Product.skip_status.is_(None)) | (Product.skip_status != 1)
                ).scalar() or 0
                
                # Normal: status = 1 AND final_status IS NULL
                normal = self.db.query(func.count(PricingProductIterationItem.id)).join(
                    Product,
                    PricingProductIterationItem.system_product_id == Product.system_product_id
                ).filter(
                    PricingProductIterationItem.iteration_id == iter_id,
                    PricingProductIterationItem.status == 1,
                    PricingProductIterationItem.final_status.is_(None),
                    (Product.skip_status.is_(None)) | (Product.skip_status != 1)
                ).scalar() or 0
                
                # Outlier: status = 0 AND final_status IS NULL
                outlier = self.db.query(func.count(PricingProductIterationItem.id)).join(
                    Product,
                    PricingProductIterationItem.system_product_id == Product.system_product_id
                ).filter(
                    PricingProductIterationItem.iteration_id == iter_id,
                    PricingProductIterationItem.status == 0,
                    PricingProductIterationItem.final_status.is_(None),
                    (Product.skip_status.is_(None)) | (Product.skip_status != 1)
                ).scalar() or 0
                
                # Manual Outlier: final_status = 0
                manual_outlier = self.db.query(func.count(PricingProductIterationItem.id)).join(
                    Product,
                    PricingProductIterationItem.system_product_id == Product.system_product_id
                ).filter(
                    PricingProductIterationItem.iteration_id == iter_id,
                    PricingProductIterationItem.final_status == 0,
                    (Product.skip_status.is_(None)) | (Product.skip_status != 1)
                ).scalar() or 0
                
                # Manual Normal: final_status = 1
                manual_normal = self.db.query(func.count(PricingProductIterationItem.id)).join(
                    Product,
                    PricingProductIterationItem.system_product_id == Product.system_product_id
                ).filter(
                    PricingProductIterationItem.iteration_id == iter_id,
                    PricingProductIterationItem.final_status == 1,
                    (Product.skip_status.is_(None)) | (Product.skip_status != 1)
                ).scalar() or 0
                
                summary.append({
                    'iteration': iter_id,
                    'iteration_id': iter_id,
                    'eps': float(row.eps) if row.eps else None,
                    'sample': int(row.sample) if row.sample else None,
                    'x_axis': row.x_axis,
                    'y_axis': row.y_axis,
                    'z_axis': row.z_axis,
                    'total_count': int(total),
                    'normal_count': int(normal),
                    'outlier_count': int(outlier),
                    'manual_outlier_count': int(manual_outlier),
                    'manual_normal_count': int(manual_normal)
                })
            
            return summary
        except Exception as e:
            print(f"Error getting iteration summary: {e}")
            return []

    def get_iteration_summary_by_brand_or_category(self, group_id, brand_ids, category_ids):
        """Get iterations where brand_id exists in brand column OR category_id exists in category column"""
        from sqlalchemy import func, text
        from models.pricing.product_iteration_item import PricingProductIterationItem
        from models.pricing.product import Product

        try:
            brand_parts = [f"FIND_IN_SET(:bid{i}, pricing_product_iteration.brand) > 0" for i in range(len(brand_ids))]
            cat_parts = [f"FIND_IN_SET(:cid{i}, pricing_product_iteration.category) > 0" for i in range(len(category_ids))]
            or_clause = " OR ".join(brand_parts + cat_parts)
            bind_params = {f"bid{i}": str(bid) for i, bid in enumerate(brand_ids)}
            bind_params.update({f"cid{i}": str(cid) for i, cid in enumerate(category_ids)})

            q = self.db.query(
                ProductIteration.iteration_id,
                ProductIteration.eps,
                ProductIteration.sample,
                ProductIteration.x_axis,
                ProductIteration.y_axis,
                ProductIteration.z_axis
            ).join(
                PricingProductIterationItem,
                ProductIteration.iteration_id == PricingProductIterationItem.iteration_id
            ).filter(
                ProductIteration.product_group_id == group_id,
                text(f"({or_clause})").bindparams(**bind_params)
            ).group_by(
                ProductIteration.iteration_id,
                ProductIteration.eps,
                ProductIteration.sample
            ).order_by(
                ProductIteration.iteration_id.asc()
            ).all()

            summary = []
            for row in q:
                iter_id = row.iteration_id
                total = self.db.query(func.count(PricingProductIterationItem.id)).join(
                    Product, PricingProductIterationItem.system_product_id == Product.system_product_id
                ).filter(
                    PricingProductIterationItem.iteration_id == iter_id,
                    (Product.skip_status.is_(None)) | (Product.skip_status != 1)
                ).scalar() or 0
                normal = self.db.query(func.count(PricingProductIterationItem.id)).join(
                    Product, PricingProductIterationItem.system_product_id == Product.system_product_id
                ).filter(
                    PricingProductIterationItem.iteration_id == iter_id,
                    PricingProductIterationItem.status == 1,
                    PricingProductIterationItem.final_status.is_(None),
                    (Product.skip_status.is_(None)) | (Product.skip_status != 1)
                ).scalar() or 0
                outlier = self.db.query(func.count(PricingProductIterationItem.id)).join(
                    Product, PricingProductIterationItem.system_product_id == Product.system_product_id
                ).filter(
                    PricingProductIterationItem.iteration_id == iter_id,
                    PricingProductIterationItem.status == 0,
                    PricingProductIterationItem.final_status.is_(None),
                    (Product.skip_status.is_(None)) | (Product.skip_status != 1)
                ).scalar() or 0
                manual_outlier = self.db.query(func.count(PricingProductIterationItem.id)).join(
                    Product, PricingProductIterationItem.system_product_id == Product.system_product_id
                ).filter(
                    PricingProductIterationItem.iteration_id == iter_id,
                    PricingProductIterationItem.final_status == 0,
                    (Product.skip_status.is_(None)) | (Product.skip_status != 1)
                ).scalar() or 0
                manual_normal = self.db.query(func.count(PricingProductIterationItem.id)).join(
                    Product, PricingProductIterationItem.system_product_id == Product.system_product_id
                ).filter(
                    PricingProductIterationItem.iteration_id == iter_id,
                    PricingProductIterationItem.final_status == 1,
                    (Product.skip_status.is_(None)) | (Product.skip_status != 1)
                ).scalar() or 0
                summary.append({
                    'iteration': iter_id,
                    'iteration_id': iter_id,
                    'eps': float(row.eps) if row.eps else None,
                    'sample': int(row.sample) if row.sample else None,
                    'x_axis': row.x_axis,
                    'y_axis': row.y_axis,
                    'z_axis': row.z_axis,
                    'total_count': int(total),
                    'normal_count': int(normal),
                    'outlier_count': int(outlier),
                    'manual_outlier_count': int(manual_outlier),
                    'manual_normal_count': int(manual_normal)
                })
            return summary
        except Exception as e:
            print(f"Error getting iteration summary by brand or category: {e}")
            return []
