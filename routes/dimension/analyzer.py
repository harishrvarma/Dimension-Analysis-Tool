from flask import Blueprint, jsonify, render_template, request, make_response
from services.dimension import analyzer
import csv
from io import StringIO


analyzer_bp = Blueprint("analyzer_bp", __name__, url_prefix="/dimension/analyzer")


@analyzer_bp.get("")
@analyzer_bp.get("/")
def analyzer_page():
    return render_template("dimension/analyzer/index.html", active_page="analyzer")


@analyzer_bp.get("/api/product-groups")
def api_product_groups():
    """Get all product groups with default selection"""
    groups, default_group_id = analyzer.get_product_groups()
    return jsonify({"groups": groups, "default_group_id": default_group_id})


@analyzer_bp.post("/api/brands")
def api_brands():
    """Get brands for selected group"""
    payload = request.get_json(silent=True) or {}
    group_id = payload.get("group_id")
    
    if not group_id:
        return jsonify({"ok": False, "brands": []})
    
    brands = analyzer.get_brands_for_group(group_id)
    return jsonify({"ok": True, "brands": brands})


@analyzer_bp.post("/api/options")
def api_options():
    """Get all filter options (brands, categories, types) based on current selections"""
    payload = request.get_json(silent=True) or {}
    group_id = payload.get("group_id")
    brands = payload.get("brands") or []
    category = payload.get("category")
    
    if not group_id:
        return jsonify({"ok": False, "message": "Group ID required"})
    
    brand_options = analyzer.get_brands_for_group(group_id)
    category_options = analyzer.get_categories_for_group(group_id, brands if brands else None)
    type_options = analyzer.get_types_for_group(group_id, brands if brands else None, category)
    
    return jsonify({
        "ok": True,
        "brand_options": brand_options,
        "category_options": category_options,
        "type_options": type_options
    })


@analyzer_bp.post("/api/categories")
def api_categories():
    """Get categories for selected group and brands"""
    payload = request.get_json(silent=True) or {}
    group_id = payload.get("group_id")
    brands = payload.get("brands") or []
    
    if not group_id:
        return jsonify({"ok": False, "categories": []})
    
    categories = analyzer.get_categories_for_group(group_id, brands if brands else None)
    return jsonify({"ok": True, "categories": categories})


@analyzer_bp.post("/api/types")
def api_types():
    """Get types for selected group, brands, and category"""
    payload = request.get_json(silent=True) or {}
    group_id = payload.get("group_id")
    brands = payload.get("brands") or []
    category = payload.get("category")
    
    if not group_id:
        return jsonify({"ok": False, "types": []})
    
    types = analyzer.get_types_for_group(group_id, brands if brands else None, category)
    return jsonify({"ok": True, "types": types})


@analyzer_bp.post("/api/analyze")
def api_analyze():
    """Run analysis"""
    payload = request.get_json(silent=True) or {}
    
    group_id = payload.get("group_id")
    brands = payload.get("brands") or []
    category = payload.get("category")
    types = payload.get("types") or []
    algorithms = payload.get("algorithms") or []
    h_mult = float(payload.get("h_mult", 1.5))
    w_mult = float(payload.get("w_mult", 1.5))
    d_mult = float(payload.get("d_mult", 1.5))
    dbscan_eps = float(payload.get("dbscan_eps", 1.0))
    dbscan_min_samples = int(payload.get("dbscan_min_samples", 4))
    analysis_mode = payload.get("analysis_mode", "all")
    save_to_db = payload.get("save_to_db", False)
    
    if not group_id or not category or not algorithms:
        return jsonify({"ok": False, "message": "Missing required fields"})
    
    result = analyzer.analyze_and_save(
        group_id, brands, category, types, algorithms,
        h_mult, w_mult, d_mult, dbscan_eps, dbscan_min_samples,
        analysis_mode, save_to_db
    )
    
    return jsonify(result)


@analyzer_bp.post("/api/iteration-history")
def api_iteration_history():
    """Get iteration history for a category"""
    payload = request.get_json(silent=True) or {}
    group_id = payload.get("group_id")
    brands = payload.get("brands") or []
    category = payload.get("category")
    types = payload.get("types") or []
    
    if not group_id or not category:
        return jsonify({"ok": False, "history": []})
    
    history = analyzer.get_iteration_history(group_id, brands, category, types)
    return jsonify({"ok": True, "history": history})


@analyzer_bp.post("/api/reset-iterations")
def api_reset_iterations():
    """Reset iterations for a category"""
    payload = request.get_json(silent=True) or {}
    group_id = payload.get("group_id")
    brands = payload.get("brands") or []
    category = payload.get("category")
    types = payload.get("types") or []
    
    if not group_id:
        return jsonify({"ok": False, "message": "Product Group is required"})
    
    success = analyzer.reset_iterations(group_id, brands, category, types)
    return jsonify({"ok": success})


@analyzer_bp.post("/api/get-all-outliers")
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


@analyzer_bp.post("/api/get-global-aggregate")
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


@analyzer_bp.post("/api/export")
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


@analyzer_bp.post("/api/set-cluster-outlier")
def api_set_cluster_outlier():
    """Mark all products in a cluster as outliers"""
    payload = request.get_json(silent=True) or {}
    skus = payload.get("skus") or []
    iteration = payload.get("iteration")
    brands = payload.get("brands") or []
    category = payload.get("category")
    
    if not skus or not iteration:
        return jsonify({"ok": False, "message": "SKUs and iteration are required"})
    
    success, error = analyzer.set_cluster_as_outlier(
        skus, iteration, brands, category
    )
    
    if success:
        return jsonify({"ok": True, "message": f"Updated {len(skus)} products"})
    else:
        return jsonify({"ok": False, "message": error or "Failed to update products"})


@analyzer_bp.post("/api/remove-cluster-outlier")
def api_remove_cluster_outlier():
    """Remove outlier status from all products in a cluster"""
    payload = request.get_json(silent=True) or {}
    skus = payload.get("skus") or []
    iteration = payload.get("iteration")
    brands = payload.get("brands") or []
    category = payload.get("category")
    
    if not skus or not iteration:
        return jsonify({"ok": False, "message": "SKUs and iteration are required"})
    
    success, error = analyzer.remove_cluster_outlier(
        skus, iteration, brands, category
    )
    
    if success:
        return jsonify({"ok": True, "message": f"Updated {len(skus)} products"})
    else:
        return jsonify({"ok": False, "message": error or "Failed to update products"})


@analyzer_bp.post("/api/analyze-multiple")
def api_analyze_multiple():
    """Analyze multiple combinations based on filter hierarchy"""
    payload = request.get_json(silent=True) or {}
    
    group_id = payload.get("group_id")
    brands = payload.get("brands") or []
    category = payload.get("category")
    types = payload.get("types") or []
    algorithms = payload.get("algorithms") or []
    h_mult = float(payload.get("h_mult", 1.5))
    w_mult = float(payload.get("w_mult", 1.5))
    d_mult = float(payload.get("d_mult", 1.5))
    dbscan_eps = float(payload.get("dbscan_eps", 1.0))
    dbscan_min_samples = int(payload.get("dbscan_min_samples", 4))
    save_to_db = payload.get("save_to_db", False)
    
    if not group_id or not algorithms:
        return jsonify({"ok": False, "message": "Missing required fields"})
    
    result = analyzer.analyze_multiple_combinations(
        group_id, brands, category, types, algorithms,
        h_mult, w_mult, d_mult, dbscan_eps, dbscan_min_samples, save_to_db
    )
    
    return jsonify(result)


@analyzer_bp.post("/api/process-combination")
def api_process_combination():
    """Process a single combination"""
    payload = request.get_json(silent=True) or {}
    
    group_id = payload.get("group_id")
    combination = payload.get("combination")
    algorithms = payload.get("algorithms") or []
    h_mult = float(payload.get("h_mult", 1.5))
    w_mult = float(payload.get("w_mult", 1.5))
    d_mult = float(payload.get("d_mult", 1.5))
    dbscan_eps = float(payload.get("dbscan_eps", 1.0))
    dbscan_min_samples = int(payload.get("dbscan_min_samples", 4))
    save_to_db = payload.get("save_to_db", False)
    
    if not group_id or not combination or not algorithms:
        return jsonify({"ok": False, "message": "Missing required fields"})
    
    result = analyzer.process_single_combination_v2(
        group_id, combination, algorithms,
        h_mult, w_mult, d_mult, dbscan_eps, dbscan_min_samples, save_to_db
    )
    
    return jsonify(result)


@analyzer_bp.post("/api/load-iteration")
def api_load_iteration():
    """Load saved iteration and return filters and analysis result"""
    payload = request.get_json(silent=True) or {}
    iteration_id = payload.get("iteration_id")
    
    if not iteration_id:
        return jsonify({"ok": False, "message": "Iteration ID required"})
    
    result = analyzer.load_saved_iteration(iteration_id)
    return jsonify(result)


@analyzer_bp.post("/api/delete-iteration")
def api_delete_iteration():
    """Delete iteration and recalculate aggregate data"""
    payload = request.get_json(silent=True) or {}
    iteration_id = payload.get("iteration_id")
    
    if not iteration_id:
        return jsonify({"ok": False, "message": "Iteration ID required"})
    
    success, message = analyzer.delete_iteration(iteration_id)
    return jsonify({"ok": success, "message": message})
