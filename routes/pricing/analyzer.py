from flask import Blueprint, jsonify, render_template, request, make_response
from services.pricing import analyzer
import csv
from io import StringIO
from models.base.base import SessionLocal
from repositories.pricing.product_column_repository import ProductColumnRepository
from repositories.pricing.product_insight_config_repository import ProductInsightConfigRepository


pricing_analyzer_bp = Blueprint("pricing_analyzer_bp", __name__, url_prefix="/pricing/analyzer")


@pricing_analyzer_bp.get("")
@pricing_analyzer_bp.get("/")
def analyzer_page():
    return render_template("pricing/analyzer/index.html", active_page="pricing_analyzer")


@pricing_analyzer_bp.get("/api/insight-config")
def api_insight_config():
    session = SessionLocal()
    try:
        configs = ProductInsightConfigRepository(session).get_all()
        columns = ProductColumnRepository(session).get_all()
        return jsonify({
            "ok": True,
            "configs": [
                {
                    "id": c.insight_config_id,
                    "name": c.name,
                    "x_axis": c.x_axis,
                    "y_axis": c.y_axis,
                    "z_axis": c.z_axis,
                    "x_axis_com": c.x_axis_com,
                    "y_axis_com": c.y_axis_com,
                    "z_axis_com": c.z_axis_com,
                    "is_default": c.is_default
                } for c in configs
            ],
            "columns": [
                {
                    "id": col.column_id,
                    "name": col.name,
                    "code": col.code,
                    "symbol": col.symbol
                } for col in columns
            ]
        })
    finally:
        session.close()


@pricing_analyzer_bp.get("/api/product-groups")
def api_product_groups():
    """Get all product groups with default selection"""
    groups, default_group_id = analyzer.get_product_groups()
    return jsonify({"groups": groups, "default_group_id": default_group_id})


@pricing_analyzer_bp.post("/api/brands")
def api_brands():
    """Get brands for selected group"""
    payload = request.get_json(silent=True) or {}
    group_id = payload.get("group_id")
    
    if not group_id:
        return jsonify({"ok": False, "brands": []})
    
    brands = analyzer.get_brands_for_group(group_id)
    return jsonify({"ok": True, "brands": brands})


@pricing_analyzer_bp.post("/api/options")
def api_options():
    """Get all filter options (brands, categories, types) based on current selections"""
    payload = request.get_json(silent=True) or {}
    group_id = payload.get("group_id")
    brands = payload.get("brands") or []
    categories = payload.get("categories") or []
    category = payload.get("category") or (categories[0] if len(categories) == 1 else None)
    
    if not group_id:
        return jsonify({"ok": False, "message": "Group ID required"})
    
    brand_options = analyzer.get_brands_for_group(group_id)
    category_options = analyzer.get_categories_for_group(group_id, brands if brands else None)
    type_options = analyzer.get_types_for_group(group_id, brands if brands else None, categories or category)
    
    return jsonify({
        "ok": True,
        "brand_options": brand_options,
        "category_options": category_options,
        "type_options": type_options
    })


@pricing_analyzer_bp.post("/api/categories")
def api_categories():
    """Get categories for selected group and brands"""
    payload = request.get_json(silent=True) or {}
    group_id = payload.get("group_id")
    brands = payload.get("brands") or []
    
    if not group_id:
        return jsonify({"ok": False, "categories": []})
    
    categories = analyzer.get_categories_for_group(group_id, brands if brands else None)
    return jsonify({"ok": True, "categories": categories})


@pricing_analyzer_bp.post("/api/types")
def api_types():
    """Get types for selected group, brands, and categories"""
    payload = request.get_json(silent=True) or {}
    group_id = payload.get("group_id")
    brands = payload.get("brands") or []
    categories = payload.get("categories") or []
    category = payload.get("category") or (categories[0] if len(categories) == 1 else None)
    
    if not group_id:
        return jsonify({"ok": False, "types": []})
    
    types = analyzer.get_types_for_group(group_id, brands if brands else None, categories or category)
    return jsonify({"ok": True, "types": types})


@pricing_analyzer_bp.post("/api/analyze")
def api_analyze():
    """Run analysis"""
    payload = request.get_json(silent=True) or {}
    
    group_id = payload.get("group_id")
    brands = payload.get("brands") or []
    categories = payload.get("categories") or []
    category_ids = payload.get("category_ids") or []
    brand_ids = payload.get("brand_ids") or []
    category = payload.get("category") or (categories[0] if len(categories) == 1 else None)
    types = payload.get("types") or []
    algorithms = payload.get("algorithms") or []
    algorithm_settings = payload.get("algorithm_settings") or ["shape", "size", "volume"]
    h_mult = float(payload.get("mfr_cost_mult", 1.5))
    w_mult = float(payload.get("shipping_cost_mult", 1.5))
    d_mult = float(payload.get("price_mult", 1.5))
    dbscan_eps = float(payload.get("dbscan_eps", 1.0))
    dbscan_min_samples = int(payload.get("dbscan_min_samples", 4))
    analysis_mode = payload.get("analysis_mode", "all")
    save_to_db = payload.get("save_to_db", False)
    selected_iteration_id = payload.get("selected_iteration_id")
    axis_cols = payload.get("axis_cols") or None
    axis_meta = payload.get("axis_meta") or {}
    axis_col_ids = payload.get("axis_col_ids") or None
    axis_col_com_ids = payload.get("axis_col_com_ids") or None
    
    if not group_id or not algorithms or (not categories and not brands):
        return jsonify({"ok": False, "message": "Missing required fields"})
    
    result = analyzer.analyze_and_save(
        group_id, brands, categories, types, algorithms,
        h_mult, w_mult, d_mult, dbscan_eps, dbscan_min_samples,
        analysis_mode, save_to_db, selected_iteration_id,
        algorithm_settings=algorithm_settings,
        axis_cols=axis_cols,
        axis_col_ids=axis_col_ids,
        axis_col_com_ids=axis_col_com_ids,
        category_ids=category_ids,
        brand_ids=brand_ids
    )
    
    # Attach axis meta to result so frontend can use it
    if isinstance(result, dict):
        result['axis_meta'] = axis_meta
        result['axis_cols'] = axis_cols
    
    return jsonify(result)


@pricing_analyzer_bp.post("/api/iteration-history")
def api_iteration_history():
    """Get iteration history for categories - by group_id and one or more categories"""
    payload = request.get_json(silent=True) or {}
    group_id = payload.get("group_id")
    categories = payload.get("categories") or []
    category_ids = payload.get("category_ids") or []
    brand_ids = payload.get("brand_ids") or []
    # backward compat: single category string
    if not categories and payload.get("category"):
        categories = [payload.get("category")]
    
    if not group_id or (not categories and not brand_ids):
        return jsonify({"ok": False, "history": []})
    
    history = analyzer.get_iteration_history(group_id, categories, category_ids, brand_ids=brand_ids)
    return jsonify({"ok": True, "history": history})


@pricing_analyzer_bp.post("/api/reset-iterations")
def api_reset_iterations():
    """Reset all iterations for a product group, optionally filtered by category"""
    payload = request.get_json(silent=True) or {}
    group_id = payload.get("group_id")
    category = payload.get("category")  # optional — None means reset all categories
    
    if not group_id:
        return jsonify({"ok": False, "message": "Product Group is required"})
    
    success = analyzer.reset_iterations(group_id, category)
    return jsonify({"ok": success})


@pricing_analyzer_bp.post("/api/get-all-outliers")
def api_get_all_outliers():
    """Get all outliers from previous iterations"""
    payload = request.get_json(silent=True) or {}
    group_id = payload.get("group_id")
    brands = payload.get("brands") or []
    category = payload.get("category")
    types = payload.get("types") or []
    current_iteration = payload.get("current_iteration", 1)
    algorithms = payload.get("algorithms") or []
    
    if not group_id or not category:
        return jsonify({"ok": False, "outliers": []})
    
    outliers = analyzer.get_all_previous_outliers(group_id, brands, category, types, current_iteration, algorithms)
    return jsonify({"ok": True, "outliers": outliers})


@pricing_analyzer_bp.post("/api/get-global-aggregate")
def api_get_global_aggregate():
    """Get global aggregate data from product table"""
    payload = request.get_json(silent=True) or {}
    group_id = payload.get("group_id")
    brands = payload.get("brands") or []
    category = payload.get("category")
    types = payload.get("types") or []
    algorithms = payload.get("algorithms") or []
    
    if not group_id or not category:
        return jsonify({"ok": False, "data": []})
    
    data = analyzer.get_global_aggregate_data(group_id, brands, category, types, algorithms)
    return jsonify({"ok": True, "data": data})


@pricing_analyzer_bp.post("/api/export")
def api_export():
    """Export analysis results to CSV"""
    payload = request.get_json(silent=True) or {}
    data = payload.get("data") or []
    export_type = payload.get("export_type", "all")
    
    if not data:
        return jsonify({"ok": False, "message": "No data to export"}), 400
    
    # Filter data based on export type
    if export_type == "normal":
        filtered_data = [row for row in data if not row.get('is_outlier_combined', False)]
    elif export_type == "outlier":
        filtered_data = [row for row in data if row.get('is_outlier_combined', False)]
    else:
        filtered_data = data
    
    if not filtered_data:
        return jsonify({"ok": False, "message": "No data to export"}), 400
    
    # Create CSV
    si = StringIO()
    writer = csv.writer(si)
    
    # Headers
    headers = ['SKU', 'Brand', 'Category', 'Type', 'Name', 'Height', 'Width', 'Depth', 'Status']
    writer.writerow(headers)
    
    # Data rows
    for row in filtered_data:
        writer.writerow([
            row.get('SKU', ''),
            row.get('Brand', ''),
            row.get('Category', ''),
            row.get('Type', ''),
            row.get('Name', ''),
            row.get('H', ''),
            row.get('W', ''),
            row.get('D', ''),
            'Outlier' if row.get('is_outlier_combined', False) else 'Normal'
        ])
    
    output = make_response(si.getvalue())
    output.headers["Content-Disposition"] = f"attachment; filename=analyzer_export_{export_type}.csv"
    output.headers["Content-type"] = "text/csv"
    return output


@pricing_analyzer_bp.post("/api/set-cluster-normal")
def api_set_cluster_normal():
    """Mark all products in a cluster as normal"""
    payload = request.get_json(silent=True) or {}
    skus = payload.get("skus") or []
    iteration = payload.get("iteration")
    brands = payload.get("brands") or []
    category = payload.get("category")
    eps = payload.get("eps")
    sample = payload.get("sample")
    group_id = payload.get("group_id")
    
    if not skus or not iteration:
        return jsonify({"ok": False, "message": "SKUs and iteration are required"})
    
    success, error = analyzer.set_cluster_as_normal(
        skus, iteration, brands, category, eps, sample, group_id
    )
    
    if success:
        return jsonify({"ok": True, "message": f"Updated {len(skus)} products"})
    else:
        return jsonify({"ok": False, "message": error or "Failed to update products"})


@pricing_analyzer_bp.post("/api/set-cluster-outlier")
def api_set_cluster_outlier():
    """Mark all products in a cluster as outliers"""
    payload = request.get_json(silent=True) or {}
    skus = payload.get("skus") or []
    iteration = payload.get("iteration")
    brands = payload.get("brands") or []
    category = payload.get("category")
    eps = payload.get("eps")
    sample = payload.get("sample")
    group_id = payload.get("group_id")
    
    if not skus or not iteration:
        return jsonify({"ok": False, "message": "SKUs and iteration are required"})
    
    success, error = analyzer.set_cluster_as_outlier(
        skus, iteration, brands, category, eps, sample, group_id
    )
    
    if success:
        return jsonify({"ok": True, "message": f"Updated {len(skus)} products"})
    else:
        return jsonify({"ok": False, "message": error or "Failed to update products"})


@pricing_analyzer_bp.post("/api/remove-cluster-outlier")
def api_remove_cluster_outlier():
    """Remove outlier status from all products in a cluster"""
    payload = request.get_json(silent=True) or {}
    skus = payload.get("skus") or []
    iteration = payload.get("iteration")
    brands = payload.get("brands") or []
    category = payload.get("category")
    group_id = payload.get("group_id")
    
    if not skus or not iteration:
        return jsonify({"ok": False, "message": "SKUs and iteration are required"})
    
    success, error = analyzer.remove_cluster_outlier(
        skus, iteration, brands, category, group_id
    )
    
    if success:
        return jsonify({"ok": True, "message": f"Updated {len(skus)} products"})
    else:
        return jsonify({"ok": False, "message": error or "Failed to update products"})



@pricing_analyzer_bp.post("/api/load-iteration")
def api_load_iteration():
    """Load saved iteration and return filters and analysis result"""
    payload = request.get_json(silent=True) or {}
    iteration_id = payload.get("iteration_id")
    
    if not iteration_id:
        return jsonify({"ok": False, "message": "Iteration ID required"})
    
    result = analyzer.load_saved_iteration(iteration_id)
    return jsonify(result)


@pricing_analyzer_bp.post("/api/delete-iteration")
def api_delete_iteration():
    """Delete iteration and recalculate aggregate data"""
    payload = request.get_json(silent=True) or {}
    iteration_id = payload.get("iteration_id")
    
    if not iteration_id:
        return jsonify({"ok": False, "message": "Iteration ID required"})
    
    success, message = analyzer.delete_iteration(iteration_id)
    return jsonify({"ok": success, "message": message})


@pricing_analyzer_bp.post("/api/update-item-status")
def api_update_item_status():
    """Update final_status for a specific iteration item"""
    payload = request.get_json(silent=True) or {}
    sku = payload.get("sku")
    final_status = payload.get("final_status")  # Can be 0, 1, or None
    iteration_id = payload.get("iteration_id")
    group_id = payload.get("group_id")
    category = payload.get("category")
    eps = payload.get("eps")
    sample = payload.get("sample")
    
    if sku is None or not iteration_id:
        return jsonify({"ok": False, "message": "SKU and iteration_id are required"})
    
    success, error = analyzer.update_item_status(
        sku, final_status, iteration_id, group_id, category, eps, sample
    )
    
    if success:
        return jsonify({"ok": True, "message": f"Updated {sku}"})
    else:
        return jsonify({"ok": False, "message": error or "Failed to update item"})


@pricing_analyzer_bp.get("/api/analyze-all-export")
def api_analyze_all_export_get():
    """Analyze all products and export results - GET with algorithm parameter (legacy)"""
    from services.pricing.analyze_all_export import analyze_all_and_export
    from flask import Response
    import time
    
    algorithm = request.args.get('algorithm', 'DBSCAN')
    
    print(f"Starting export with algorithm: {algorithm}")
    start = time.time()
    
    csv_data, error = analyze_all_and_export(algorithm=algorithm)
    
    print(f"Export completed in {time.time()-start:.1f}s")
    
    if error:
        return jsonify({"ok": False, "message": error}), 400
    
    response = Response(
        csv_data,
        mimetype='text/csv',
        headers={
            'Content-Disposition': f'attachment; filename=analyze_all_export_{algorithm}.csv',
            'Content-Type': 'text/csv; charset=utf-8'
        }
    )
    return response


@pricing_analyzer_bp.post("/api/analyze-all-export")
def api_analyze_all_export_post():
    """Analyze all products and export results - POST with full configuration"""
    from services.pricing.analyze_all_export import analyze_all_and_export
    from flask import Response
    import time
    
    payload = request.get_json(silent=True) or {}
    
    product_group_id = payload.get('product_group_id')
    algorithm = payload.get('algorithm', 'DBSCAN')
    record_type = payload.get('record_type', 'all')
    configurations = payload.get('configurations', [])
    algorithm_settings = payload.get('algorithm_settings', ['shape', 'size', 'volume'])
    axis_cols = payload.get('axis_cols') or None
    
    if not product_group_id:
        return jsonify({"ok": False, "message": "Product Group ID is required"}), 400
    
    configs = [(c['eps'], c['min_samples']) for c in configurations] if configurations else None
    filters = {'product_group_id': product_group_id}
    
    print(f"Starting export with product_group_id: {product_group_id}, algorithm: {algorithm}, record_type: {record_type}, settings: {algorithm_settings}")
    start = time.time()
    
    csv_data, error = analyze_all_and_export(
        product_group_id=product_group_id,
        algorithm=algorithm,
        record_type=record_type,
        configs=configs,
        filters=filters,
        algorithm_settings=algorithm_settings,
        axis_cols=axis_cols
    )
    
    print(f"Export completed in {time.time()-start:.1f}s")
    
    if error:
        return jsonify({"ok": False, "message": error}), 400
    
    response = Response(
        csv_data,
        mimetype='text/csv',
        headers={
            'Content-Disposition': f'attachment; filename=analyze_all_export_{algorithm}_category_{record_type}.csv',
            'Content-Type': 'text/csv; charset=utf-8'
        }
    )
    return response


