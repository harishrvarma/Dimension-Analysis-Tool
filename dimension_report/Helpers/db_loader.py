import pandas as pd
from sqlalchemy.orm import Session
from Models.database import SessionLocal
from Helpers.product_group_helper import get_all_product_groups, get_product_group_by_id
from Helpers.product_helper import get_products_by_group_id

def get_product_groups_for_dropdown():
    db = SessionLocal()
    try:
        groups = get_all_product_groups(db)
        return [{
            "label": f"{group.name} ({group.product_count})",
            "value": group.group_id
        } for group in groups]
    finally:
        db.close()

def get_brands_by_group(group_id: int):
    """Get distinct brands for a group with product counts"""
    db = SessionLocal()
    try:
        from Models.models import Product
        from sqlalchemy import func
        brands = db.query(
            Product.brand, 
            func.count(Product.product_id)
        ).filter(
            Product.group_id == group_id,
            Product.brand.isnot(None),
            Product.brand != ''
        ).group_by(Product.brand).all()
        return sorted([(b[0], b[1]) for b in brands], key=lambda x: x[0])
    finally:
        db.close()

def get_categories_by_brands(group_id: int, brands: list):
    """Get distinct categories for selected brands with product counts"""
    db = SessionLocal()
    try:
        from Models.models import Product
        from sqlalchemy import func
        query = db.query(
            Product.category, 
            func.count(Product.product_id)
        ).filter(
            Product.group_id == group_id,
            Product.category.isnot(None),
            Product.category != ''
        )
        if brands:
            query = query.filter(Product.brand.in_(brands))
        categories = query.group_by(Product.category).all()
        return sorted([(c[0], c[1]) for c in categories], key=lambda x: x[0])
    finally:
        db.close()

def load_filtered_data(group_id: int, brands: list, category: str = None):
    """Load products with SQL filters applied"""
    db = SessionLocal()
    try:
        from Models.models import Product
        
        query = db.query(Product).filter(
            Product.group_id == group_id,
            Product.height.isnot(None),
            Product.width.isnot(None),
            Product.depth.isnot(None)
        )
        
        if brands:
            query = query.filter(Product.brand.in_(brands))
        if category:
            if isinstance(category, list):
                query = query.filter(Product.category.in_(category))
            else:
                query = query.filter(Product.category == category)
        
        products = query.all()
        
        if not products:
            return pd.DataFrame()
        
        data = [{
            'SKU': p.system_product_id or '',
            'Brand': p.brand or '',
            'Category': p.category or '',
            'Type': p.product_type or '',
            'H': float(p.height) if p.height else 0,
            'W': float(p.width) if p.width else 0,
            'D': float(p.depth) if p.depth else 0,
            'Name': p.name or '',
            'imageUrl': p.base_image_url or '',
            'productUrl': p.product_url or '',
            'qb_code': p.qb_code or '',
            'product_id': p.product_id,
            'skip_status': p.skip_status or ''
        } for p in products]
        
        df = pd.DataFrame(data)
        return df
    finally:
        db.close()
