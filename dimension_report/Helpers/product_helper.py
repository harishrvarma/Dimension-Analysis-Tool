from sqlalchemy.orm import Session
from Models.models import Product
from datetime import datetime

def get_products_by_group_id(db: Session, group_id: int):
    return db.query(Product).filter(Product.group_id == group_id).all()

def get_product_by_id(db: Session, product_id: int):
    return db.query(Product).filter(Product.product_id == product_id).first()

def create_product(db: Session, group_id: int, **kwargs):
    product = Product(group_id=group_id, created_date=datetime.utcnow(), **kwargs)
    db.add(product)
    db.commit()
    db.refresh(product)
    return product

def update_product(db: Session, product_id: int, **kwargs):
    product = get_product_by_id(db, product_id)
    if product:
        for key, value in kwargs.items():
            if hasattr(product, key):
                setattr(product, key, value)
        db.commit()
        db.refresh(product)
    return product

def delete_product(db: Session, product_id: int):
    product = get_product_by_id(db, product_id)
    if product:
        db.delete(product)
        db.commit()
        return True
    return False

def bulk_create_products(db: Session, group_id: int, products_data: list):
    products = [Product(group_id=group_id, created_date=datetime.utcnow(), **data) for data in products_data]
    db.bulk_save_objects(products)
    db.commit()
    return len(products)

def update_product_status(db: Session, product_id: int, status_field: str, status_value: str):
    product = get_product_by_id(db, product_id)
    if product and hasattr(product, status_field):
        setattr(product, status_field, status_value)
        db.commit()
    return product

def bulk_update_skip_status(db: Session, updates: list):
    """Bulk update skip_status for multiple products"""
    if not updates:
        return True
    
    product_ids = [u['product_id'] for u in updates]
    products = db.query(Product).filter(Product.product_id.in_(product_ids)).all()
    
    update_map = {u['product_id']: u['skip_status'] for u in updates}
    for product in products:
        product.skip_status = update_map.get(product.product_id)
    
    db.commit()
    return True
