import pandas as pd
import os
from Models.database import SessionLocal
from Helpers.product_group_helper import create_product_group
from Helpers.product_helper import bulk_create_products

def import_excel_to_database(excel_filename: str, group_name: str = None):
    excel_path = os.path.join('Items', excel_filename)
    
    if not os.path.exists(excel_path):
        print(f"File not found: {excel_path}")
        return False
    
    df = pd.read_excel(excel_path)
    
    # Map columns
    column_mapping = {
        'product_id': 'system_product_id',
        'brand': 'brand',
        'category': 'category',
        'product_type': 'product_type',
        'web_id': 'qb_code',
        'name': 'name',
        'height': 'height',
        'width': 'width',
        'depth': 'depth',
        'weight': 'weight',
        'base_image_url': 'base_image_url',
        'url_key': 'product_url'
    }
    
    # Convert numeric columns
    for col in ['height', 'width', 'depth', 'weight']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Drop rows with missing dimensions
    df = df.dropna(subset=['height', 'width', 'depth'])
    
    db = SessionLocal()
    try:
        if not group_name:
            group_name = excel_filename.replace('.xlsx', '').replace('.xls', '')
        
        product_group = create_product_group(db, name=group_name, product_count=len(df))
        
        products_data = []
        for _, row in df.iterrows():
            product_data = {}
            for excel_col, db_col in column_mapping.items():
                if excel_col in df.columns:
                    value = row.get(excel_col)
                    if pd.notna(value):
                        product_data[db_col] = str(value) if db_col not in ['height', 'width', 'depth', 'weight'] else float(value)
        
            products_data.append(product_data)
        
        count = bulk_create_products(db, product_group.group_id, products_data)
        print(f"✅ Successfully imported {count} products into group '{group_name}' (ID: {product_group.group_id})")
        return True
        
    except Exception as e:
        print(f"❌ Error importing data: {str(e)}")
        db.rollback()
        return False
    finally:
        db.close()

if __name__ == "__main__":
    excel_files = [f for f in os.listdir('Items') if f.endswith(('.xlsx', '.xls'))]
    
    if not excel_files:
        print("No Excel files found in Items directory")
        exit()
    
    print("Available Excel files:")
    for i, file in enumerate(excel_files, 1):
        print(f"{i}. {file}")
    
    choice = input("\nEnter file number to import (or 'all' for all files): ")
    
    if choice.lower() == 'all':
        for excel_file in excel_files:
            print(f"\nImporting {excel_file}...")
            import_excel_to_database(excel_file)
    else:
        try:
            idx = int(choice) - 1
            if 0 <= idx < len(excel_files):
                excel_file = excel_files[idx]
                group_name = input(f"Enter group name (press Enter for '{excel_file.replace('.xlsx', '').replace('.xls', '')}'): ").strip()
                if not group_name:
                    group_name = None
                import_excel_to_database(excel_file, group_name)
            else:
                print("Invalid choice")
        except ValueError:
            print("Invalid input")
