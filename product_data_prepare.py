import pandas as pd
from datetime import datetime

brand = pd.read_csv('var/brand_option.csv')
category = pd.read_csv('var/category_product.csv')
mfr = pd.read_csv('var/manufacturer-cost.csv')
attr = pd.read_csv('var/product_attribute.csv')
map_cols = pd.read_csv('var/map-columns.csv')

mfr = mfr[
    (mfr['status'] == 'Enabled') &
    (mfr['visibility'] == 'Catalog, Search')
]

output = mfr[[
    'product_id', 'entity_id', 'sku', 'web_id', 'product_name',
    'brand', 'mpn', 'part_number', 'mfr_cost',
    'freight_charge', 'wg_charge', 'map_price', 'price', 'msrp', 'map_violation', 'gtin'
]].copy()

brand_map = brand.set_index('option')['option_id']
output.insert(
    output.columns.get_loc('brand'),
    'brand_id',
    output['brand'].map(brand_map)
)

category = category.rename(columns={
    'Product Id': 'product_id_cat',
    'Category Id': 'category_id',
    'Category': 'category'
})

category = category.drop_duplicates(subset=['product_id_cat'])

category_map = category.set_index('product_id_cat')[['category_id', 'category']]
output = output.join(category_map, on='product_id')

attr['image'] = attr['image'].fillna('')

attr['image_url'] = (
    'https://cdn.1stopbedrooms.com/media/i/catalogxl_silouethe:keepframe/catalog/product'
    + attr['image']
)

attr['product_url'] = 'https://www.1stopbedrooms.com/' + attr['url_key']
attr['product_type'] = attr['ptype']

attr_map = attr[['entity_id', 'image_url', 'product_url', 'product_type']].copy()
attr_map = attr_map.rename(columns={'entity_id': 'product_id'})

output = output.merge(attr_map, on='product_id', how='inner')

# Add mor, map, map_suspended from map-columns.csv (join on web_id)
map_cols_map = map_cols.rename(columns={'Web ID': 'web_id', 'MOR': 'mor_raw', 'MAP': 'map', 'MAP suspended': 'map_suspended'})
output = output.merge(map_cols_map[['web_id', 'mor_raw', 'map', 'map_suspended']], on='web_id', how='left')
output['mor'] = output['mor_raw'].fillna('').str.strip().str.lower().eq('yes').astype(int)
output.drop(columns=['mor_raw'], inplace=True)

cols = [
    'product_id', 'product_name', 'product_type', 'web_id',
    'sku', 'mpn', 'part_number',
    'category_id', 'category',
    'brand_id', 'brand',
    'mfr_cost', 'freight_charge', 'wg_charge',
    'map_price', 'price', 'msrp',
    'map_violation', 'mor', 'map', 'map_suspended',
    'image_url', 'product_url'
]

num_cols = [
    'mfr_cost', 'freight_charge', 'wg_charge',
    'map_price', 'price', 'msrp'
]

output[num_cols] = output[num_cols].fillna(0)

timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
filename = f'var/product_data_{timestamp}.csv'

output[cols].to_csv(filename, index=False)

print(f"Done: {len(output)} rows written to {filename}")