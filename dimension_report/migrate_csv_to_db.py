import pandas as pd
import os
from Models.database import SessionLocal
from Helpers.product_group_helper import create_product_group, update_product_group
from Helpers.product_helper import bulk_create_products

def import_csv_to_database(csv_filename: str, group_name: str = None):
    csv_path = os.path.join('Items', csv_filename)
    
    if not os.path.exists(csv_path):
        print(f"File not found: {csv_path}")
        return False
    
    df = pd.read_csv(csv_path)
    
    column_mapping = {col.lower(): col for col in df.columns}
    standard_columns = {
        'imageurl': 'imageUrl',
        'sku': 'SKU',
        'brand': 'Brand',
        'category': 'Category',
        'type': 'Type',
        'h': 'H',
        'w': 'W',
        'd': 'D',
        'name': 'Name'
    }
    
    rename_dict = {}
    for lower_col, actual_col in column_mapping.items():
        if lower_col in standard_columns:
            rename_dict[actual_col] = standard_columns[lower_col]
    
    df = df.rename(columns=rename_dict)
    
    for col in ['H', 'W', 'D']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    df = df.dropna(subset=['H', 'W', 'D'])
    
    db = SessionLocal()
    try:
        if not group_name:
            group_name = csv_filename.replace('.csv', '')
        
        product_group = create_product_group(db, name=group_name, product_count=len(df))
        
        products_data = []
        for _, row in df.iterrows():
            product_data = {
                'system_product_id': str(row.get('SKU', '')),
                'brand': str(row.get('Brand', '')),
                'category': str(row.get('Category', '')),
                'product_type': str(row.get('Type', '')),
                'name': str(row.get('Name', '')),
                'height': float(row.get('H', 0)),
                'width': float(row.get('W', 0)),
                'depth': float(row.get('D', 0)),
                'base_image_url': str(row.get('imageUrl', ''))
            }
            products_data.append(product_data)
        
        count = bulk_create_products(db, product_group.group_id, products_data)
        print(f"Successfully imported {count} products into group '{group_name}' (ID: {product_group.group_id})")
        return True
        
    except Exception as e:
        print(f"Error importing data: {str(e)}")
        db.rollback()
        return False
    finally:
        db.close()

if __name__ == "__main__":
    csv_files = [f for f in os.listdir('Items') if f.endswith('.csv')]
    
    print("Available CSV files:")
    for i, file in enumerate(csv_files, 1):
        print(f"{i}. {file}")
    
    choice = input("\nEnter file number to import (or 'all' for all files): ")
    
    if choice.lower() == 'all':
        for csv_file in csv_files:
            print(f"\nImporting {csv_file}...")
            import_csv_to_database(csv_file)
    else:
        try:
            idx = int(choice) - 1
            if 0 <= idx < len(csv_files):
                csv_file = csv_files[idx]
                group_name = input(f"Enter group name (press Enter for '{csv_file.replace('.csv', '')}'): ").strip()
                if not group_name:
                    group_name = None
                import_csv_to_database(csv_file, group_name)
            else:
                print("Invalid choice")
        except ValueError:
            print("Invalid input")
