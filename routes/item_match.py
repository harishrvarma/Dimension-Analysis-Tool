from flask import Blueprint, render_template, request, jsonify
from services.item_match.matcher import ItemMatchService
from services.item_match.attribute_service import AttributeService
import threading
import uuid

# Global dictionary to store job progress
job_progress = {}

item_match_bp = Blueprint('item_match', __name__, url_prefix='/item-match')

@item_match_bp.route('/')
def index():
    return render_template('item_match/index.html', active_page='item_match')

@item_match_bp.route('/api/attributes', methods=['GET'])
def get_attributes():
    attr_service = AttributeService()
    
    # Get scoring attributes (type='default')
    scoring_attrs = attr_service.get_attributes_by_type('default')
    
    # Get price config (type='price')
    price_attrs = attr_service.get_attributes_by_type('price')
    price_config = {attr.attribute_name: attr.default_weightage for attr in price_attrs}
    
    # Get status config (type='status')
    status_attrs = attr_service.get_attributes_by_type('status')
    status_config = {attr.attribute_name: attr.default_weightage for attr in status_attrs}
    
    return jsonify({
        'attributes': [
            {
                'value': attr.attribute_name,
                'label': attr.attribute_name.upper(),
                'default_weight': attr.default_weightage,
                'type': attr.attribute_type
            }
            for attr in scoring_attrs
        ],
        'price_config': price_config,
        'status_config': status_config
    })

@item_match_bp.route('/api/filters', methods=['POST'])
def get_filters():
    data = request.json or {}
    service = ItemMatchService()
    return jsonify(service.get_filter_options(
        brands=data.get('brands'),
        categories=data.get('categories')
    ))

@item_match_bp.route('/api/counts', methods=['POST'])
def get_counts():
    data = request.json
    service = ItemMatchService()
    return jsonify(service.get_counts(
        brands=data.get('brands'),
        categories=data.get('categories'),
        types=data.get('types')
    ))

@item_match_bp.route('/api/analyze', methods=['POST'])
def analyze():
    data = request.json
    service = ItemMatchService()
    return jsonify(service.run_analysis(
        brands=data.get('brands'),
        categories=data.get('categories'),
        types=data.get('types'),
        algorithms=data.get('algorithms', ['tfidf']),
        attributes=data.get('attributes', ['sku', 'url', 'price']),
        weights=data.get('weights', {'sku': 35, 'price': 25, 'url': 40}),
        thresholds=data.get('thresholds', {'matched': 85, 'review': 70}),
        price_config=data.get('price_config', {'margin': 25, 'margin_diff': 10}),
        save_score=data.get('save_score', False),
        save_top_most=data.get('save_top_most', False)
    ))

@item_match_bp.route('/api/item-details/<product_id>', methods=['POST'])
def get_item_details(product_id):
    data = request.json or {}
    service = ItemMatchService()
    return jsonify(service.get_item_comparison_details(
        product_id,
        algorithms=data.get('algorithms', ['tfidf']),
        attributes=data.get('attributes', ['sku', 'url', 'price']),
        weights=data.get('weights', {'sku': 35, 'price': 25, 'url': 40}),
        thresholds=data.get('thresholds', {'matched': 85, 'review': 70}),
        price_config=data.get('price_config', {'margin': 25, 'margin_diff': 10})
    ))

@item_match_bp.route('/api/save-match', methods=['POST'])
def save_match():
    data = request.json
    competitor_ref_id = data.get('competitor_ref_id')
    internal_product_id = data.get('internal_product_id')
    save_top_most = data.get('save_top_most', False)
    score = data.get('score')
    status = data.get('status')
    sku_score = data.get('sku_score')
    url_score = data.get('url_score')
    price_score = data.get('price_score')
    
    service = ItemMatchService()
    result = service.save_match(competitor_ref_id, internal_product_id, save_top_most, score, status, sku_score, url_score, price_score)
    return jsonify(result)

@item_match_bp.route('/api/update-action', methods=['POST'])
def update_action():
    data = request.json
    competitor_ref_id = data.get('competitor_ref_id')
    internal_product_id = data.get('internal_product_id')
    score = data.get('score')
    status = data.get('status')
    sku_score = data.get('sku_score')
    url_score = data.get('url_score')
    price_score = data.get('price_score')
    action = data.get('action')
    
    service = ItemMatchService()
    result = service.update_match_action(competitor_ref_id, internal_product_id, score, status, sku_score, url_score, price_score, action)
    return jsonify(result)

@item_match_bp.route('/api/analyze-multiple', methods=['POST'])
def analyze_multiple():
    data = request.json
    brands = data.get('brands', [])
    categories = data.get('categories', [])
    types = data.get('types', [])
    algorithms = data.get('algorithms', ['tfidf'])
    attributes = data.get('attributes', ['sku', 'url', 'price'])
    weights = data.get('weights', {'sku': 35, 'price': 25, 'url': 40})
    thresholds = data.get('thresholds', {'matched': 85, 'review': 70})
    price_config = data.get('price_config', {'margin': 25, 'margin_diff': 10})
    
    service = ItemMatchService()
    result = service.get_brand_category_list(
        brands=brands if brands else None,
        categories=categories if categories else None,
        types=types if types else None
    )
    return jsonify(result)

@item_match_bp.route('/api/analyze-brand-category', methods=['POST'])
def analyze_brand_category():
    data = request.json
    brand = data.get('brand')
    category = data.get('category')
    algorithms = data.get('algorithms', ['tfidf'])
    attributes = data.get('attributes', ['sku', 'url', 'price'])
    weights = data.get('weights', {'sku': 35, 'price': 25, 'url': 40})
    thresholds = data.get('thresholds', {'matched': 85, 'review': 70})
    price_config = data.get('price_config', {'margin': 25, 'margin_diff': 10})
    reset_scores = data.get('reset_scores', False)
    
    service = ItemMatchService()
    result = service.analyze_brand_category(brand, category, algorithms, attributes, weights, thresholds, price_config, reset_scores)
    
    if result.get('success'):
        stats = service.get_analysis_stats()
        result['stats'] = stats
        # Get progress for this brand
        result['brand_category_progress'] = service.get_brand_progress(brand)
    
    return jsonify(result)
