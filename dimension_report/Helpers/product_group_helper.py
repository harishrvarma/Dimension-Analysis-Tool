from sqlalchemy.orm import Session
from Models.models import ProductGroup
from datetime import datetime

def get_all_product_groups(db: Session):
    return db.query(ProductGroup).all()

def get_product_group_by_id(db: Session, group_id: int):
    return db.query(ProductGroup).filter(ProductGroup.group_id == group_id).first()

def create_product_group(db: Session, name: str, product_count: int = 0):
    product_group = ProductGroup(name=name, product_count=product_count, created_date=datetime.utcnow())
    db.add(product_group)
    db.commit()
    db.refresh(product_group)
    return product_group

def update_product_group(db: Session, group_id: int, name: str = None, product_count: int = None):
    product_group = get_product_group_by_id(db, group_id)
    if product_group:
        if name:
            product_group.name = name
        if product_count is not None:
            product_group.product_count = product_count
        db.commit()
        db.refresh(product_group)
    return product_group

def delete_product_group(db: Session, group_id: int):
    product_group = get_product_group_by_id(db, group_id)
    if product_group:
        db.delete(product_group)
        db.commit()
        return True
    return False
