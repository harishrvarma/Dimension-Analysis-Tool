from flask import Blueprint, render_template, request, jsonify
from services.item_match.matcher import ItemMatchService
from services.item_match.attribute_service import AttributeService
from services.item_match.bulk_analyzer import bulk_analyze
import threading
import uuid

# Global dictionary to store job progress
job_progress = {}
job_results = {}

item_match_bp = Blueprint('item_match', __name__, url_prefix='/item-match')

@item_match_bp.route('/')
def index():
    return render_template('item_match/index.html', active_page='item_match')

@item_match_bp.route('/api/attributes', methods=['GET'])
def get_attributes():
    attr_service = AttributeService()
    
    # Get ALL attributes grouped by type
    all_attrs = attr_service.get_all_attributes()
    
    # Group attributes by type
    attrs_by_type = {}
    for attr in all_attrs:
        attr_type = attr.attribute_type
        if attr_type not in attrs_by_type:
            attrs_by_type[attr_type] = []
        attrs_by_type[attr_type].append({
            'value': attr.attribute_name,
            'label': attr.attribute_name.upper(),
            'default_weight': attr.default_weightage,
            'competitor_attribute': attr.competitor_attribute,
            'data_type': attr.data_type
        })
    
    # Debug: Print all attributes to console
    print(f"[DEBUG] Found attributes by type: {list(attrs_by_type.keys())}")
    for attr_type, attrs in attrs_by_type.items():
        print(f"  {attr_type}: {[a['value'] for a in attrs]}")
    
    return jsonify({
        'attributes_by_type': attrs_by_type,
        'attributes': attrs_by_type.get('default', []),  # Keep backward compatibility
        'price_config': {attr['value']: attr['default_weight'] for attr in attrs_by_type.get('price', [])},
        'status_config': {attr['value']: attr['default_weight'] for attr in attrs_by_type.get('status', [])}
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
    try:
        result = service.run_analysis(
            brands=data.get('brands'),
            categories=data.get('categories'),
            types=data.get('types'),
            algorithms=data.get('algorithms', ['tfidf']),
            attributes=data.get('attributes', ['sku', 'url', 'price']),
            weights=data.get('weights', {'sku': 35, 'price': 25, 'url': 40}),
            thresholds=data.get('thresholds', {'matched': 85, 'review': 70}),
            price_config=data.get('price_config', {'margin': 25, 'margin_diff': 10}),
            save_score=data.get('save_score', False),
            save_top_most=data.get('save_top_most', False),
            hard_refresh=data.get('hard_refresh', False),
            product_ids=data.get('product_ids')
        )
        print(f"[ANALYZE ENDPOINT] Returning {len(result.get('summary', []))} results")
        if result.get('summary'):
            sample_scores = [item.get('max_score', 0) for item in result['summary'][:5]]
            print(f"[ANALYZE ENDPOINT] Sample max_scores: {sample_scores}")
        return jsonify(result)
    except Exception as e:
        print(f"[ANALYZE ENDPOINT] Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)})

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

@item_match_bp.route('/api/recalculate-scores', methods=['POST'])
def recalculate_scores():
    data = request.json
    attributes = data.get('attributes', [])
    weights = data.get('weights', {})
    thresholds = data.get('thresholds', {'matched': 85, 'review': 70})
    price_config = data.get('price_config', {})
    algorithm = data.get('algorithm')
    product_ids = data.get('product_ids', [])
    brands = data.get('brands', [])
    categories = data.get('categories', [])
    types = data.get('types', [])
    
    service = ItemMatchService()
    result = service.recalculate_scores(attributes, weights, thresholds, algorithm, brands, categories, types, product_ids, price_config)
    return jsonify(result)

@item_match_bp.route('/api/bulk-analyze', methods=['POST'])
def run_bulk_analyze():
    data = request.json or {}
    brands = data.get('brands')
    categories = data.get('categories')
    types = data.get('types')
    algorithm_id = data.get('algorithm_id', 'tfidf')
    
    job_id = str(uuid.uuid4())
    job_progress[job_id] = {'status': 'running', 'message': 'Starting...'}
    
    def run_job():
        try:
            result = bulk_analyze(brands, categories, types, algorithm_id)
            job_progress[job_id] = {'status': 'completed', 'message': 'Completed'}
            job_results[job_id] = result
        except Exception as e:
            job_progress[job_id] = {'status': 'failed', 'message': str(e)}
            job_results[job_id] = {'success': False, 'error': str(e)}
    
    thread = threading.Thread(target=run_job)
    thread.daemon = True
    thread.start()
    
    return jsonify({'success': True, 'job_id': job_id})

@item_match_bp.route('/api/bulk-analyze-status/<job_id>', methods=['GET'])
def get_bulk_analyze_status(job_id):
    if job_id not in job_progress:
        return jsonify({'status': 'not_found'})
    
    progress = job_progress[job_id]
    response = {'status': progress['status'], 'message': progress['message']}
    
    if progress['status'] == 'completed' and job_id in job_results:
        response['result'] = job_results[job_id]
        del job_progress[job_id]
        del job_results[job_id]
    elif progress['status'] == 'failed' and job_id in job_results:
        response['result'] = job_results[job_id]
        del job_progress[job_id]
        del job_results[job_id]
    
    return jsonify(response)

@item_match_bp.route('/api/score-distribution', methods=['POST'])
def get_score_distribution():
    data = request.json
    service = ItemMatchService()
    return jsonify(service.get_score_distribution(
        brands=data.get('brands'),
        categories=data.get('categories'),
        types=data.get('types'),
        status_filter=data.get('status_filter'),
        product_ids=data.get('product_ids')
    ))
