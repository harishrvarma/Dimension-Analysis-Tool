from flask import Blueprint, render_template, request, jsonify
from services.item_match.matcher import ItemMatchService
from models.base.base import SessionLocal

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

@matching_items_bp.route('/api/latest-config', methods=['GET'])
def get_latest_config():
    """Get the latest algorithm and its configuration used in scoring"""
    try:
        from sqlalchemy import text
        
        session = SessionLocal()
        
        # Get the most recent algorithm used from matching_scores table
        latest_algo_query = text("""
            SELECT algorithm_id 
            FROM matching_scores 
            WHERE algorithm_id IS NOT NULL 
            ORDER BY created_at DESC 
            LIMIT 1
        """)
        
        try:
            result = session.execute(latest_algo_query).fetchone()
            latest_algorithm = result[0] if result else 'custom'
        except Exception:
            # If matching_scores doesn't have algorithm_id or created_at, use default
            latest_algorithm = 'custom'
        
        # Try to get algorithm configuration - use fallback if table doesn't exist
        algorithm_weights = {}
        try:
            config_query = text("""
                SELECT attribute_name, weight_value 
                FROM item_match_configuration 
                WHERE algorithm_id = :algorithm_id
            """)
            config_results = session.execute(config_query, {'algorithm_id': latest_algorithm}).fetchall()
            algorithm_weights = {row[0]: row[1] for row in config_results}
        except Exception:
            # Fallback to default weights from matching_attribute table
            try:
                attr_query = text("""
                    SELECT attribute_name, default_weightage 
                    FROM matching_attribute 
                    WHERE attribute_type = 'default'
                """)
                attr_results = session.execute(attr_query).fetchall()
                for row in attr_results:
                    attr_name = row[0]
                    comp_attr = row[0]  # Use attribute name as competitor attribute
                    algorithm_weights[comp_attr] = row[1] or 33
            except Exception:
                # Final fallback - hardcoded weights
                algorithm_weights = {'sku': 35, 'url': 40, 'price': 25}
        
        # Get attribute mapping from matching_attribute table
        attribute_mapping = {}
        try:
            attr_query = text("""
                SELECT attribute_name, competitor_attribute 
                FROM matching_attribute 
                WHERE competitor_attribute IS NOT NULL
            """)
            attr_results = session.execute(attr_query).fetchall()
            attribute_mapping = {row[0]: row[1] for row in attr_results}
        except Exception:
            # Fallback mapping
            attribute_mapping = {'sku': 'sku', 'part_number': 'sku', 'url': 'url', 'price': 'price'}
        
        session.close()
        
        return jsonify({
            'success': True,
            'latest_algorithm': latest_algorithm,
            'algorithm_weights': algorithm_weights,
            'attribute_mapping': attribute_mapping
        })
        
    except Exception as e:
        if 'session' in locals():
            session.close()
        return jsonify({'success': False, 'error': str(e)}), 500
