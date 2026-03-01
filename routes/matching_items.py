from flask import Blueprint, render_template, request, jsonify
from services.item_match.matcher import ItemMatchService

matching_items_bp = Blueprint('matching_items', __name__, url_prefix='/matching-items')

@matching_items_bp.route('/')
def index():
    return render_template('matching_items/index.html', active_page='matching_items')

@matching_items_bp.route('/api/grid-data', methods=['POST'])
def get_grid_data():
    data = request.json or {}
    service = ItemMatchService()
    return jsonify(service.get_matching_items_grid(
        brands=data.get('brands'),
        categories=data.get('categories'),
        types=data.get('types'),
        status_filter=data.get('status_filter')
    ))

@matching_items_bp.route('/api/update-status', methods=['POST'])
def update_status():
    data = request.json
    comp_id = data.get('comp_id')
    review_status = data.get('review_status')
    
    service = ItemMatchService()
    result = service.update_review_status(comp_id, review_status)
    return jsonify(result)

@matching_items_bp.route('/api/comparison', methods=['POST'])
def get_comparison():
    data = request.json or {}
    product_id = data.get('product_id')
    attributes = data.get('attributes')  # Get from request, don't default to hardcoded list
    
    service = ItemMatchService()
    return jsonify(service.get_comparison_data(product_id, attributes))
