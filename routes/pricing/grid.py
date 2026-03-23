from flask import Blueprint, jsonify, render_template, request
from services.pricing import grid


pricing_grid_bp = Blueprint("pricing_grid_bp", __name__, url_prefix="/pricing/grid")


@pricing_grid_bp.get("")
@pricing_grid_bp.get("/")
def grid_page():
    return render_template("pricing/grid/index.html", active_page="pricing_report")


@pricing_grid_bp.get("/api/product-groups")
def api_product_groups():
    """Get all product groups with default selection"""
    groups, default_group_id = grid.get_product_groups()
    return jsonify({"groups": groups, "default_group_id": default_group_id})


@pricing_grid_bp.post("/api/options")
def api_options():
    """Get brands and categories based on filters"""
    payload = request.get_json(silent=True) or {}
    group_id = payload.get("group_id")
    brands = payload.get("brands") or []
    categories = payload.get("categories") or []
    types = payload.get("types") or []
    final_status = payload.get("final_status") or []
    skip_status = payload.get("skip_status") or []

    if not group_id:
        return jsonify({
            "ok": False,
            "message": "No product group selected.",
            "brand_options": [],
            "category_options": [],
            "type_options": [],
            "analyzed_status": {}
        })

    brand_options = grid.get_brands_with_counts(group_id, final_status if final_status else None)
    category_options = grid.get_categories_with_counts(group_id, brands if brands else None, final_status if final_status else None)
    type_options = grid.get_types_with_counts(group_id, brands if brands else None, categories if categories else None, final_status if final_status else None)
    analyzed_status = grid.get_analyzed_status(group_id, brands if brands else None, categories if categories else None)
    return jsonify({
        "ok": True,
        "brand_options": brand_options,
        "category_options": category_options,
        "type_options": type_options,
        "analyzed_status": analyzed_status,
        "message": f"Loaded options for group {group_id}"
    })


@pricing_grid_bp.post("/api/iteration-filters")
def api_iteration_filters():
    """Get brands and categories from iteration"""
    payload = request.get_json(silent=True) or {}
    iteration_id = payload.get("iteration_id")
    
    if not iteration_id:
        return jsonify({"ok": False, "message": "Iteration ID required"})
    
    filters = grid.get_iteration_filters(iteration_id)
    
    if not filters:
        return jsonify({"ok": False, "message": "Iteration not found"})
    
    return jsonify({"ok": True, "filters": filters})


@pricing_grid_bp.post("/api/grid-data")
def api_grid_data():
    """Load grid data based on filters"""
    payload = request.get_json(silent=True) or {}
    group_id = payload.get("group_id")
    brands = payload.get("brands") or []
    categories = payload.get("categories") or []
    types = payload.get("types") or []
    final_status = payload.get("final_status") or []
    skip_status = payload.get("skip_status") or []
    clusters = payload.get("clusters") or []
    iteration_id = payload.get("iteration_id")
    page = payload.get("page", 1)
    per_page = payload.get("per_page", 50)
    sort_column = payload.get("sort_column")
    sort_direction = payload.get("sort_direction", "asc")

    if not group_id:
        return jsonify({"ok": False, "message": "No product group selected.", "data": [], "total": 0})

    col_filters = payload.get("col_filters") or {}
    data, total = grid.load_grid_data(group_id, brands, categories, types, final_status if final_status else None, skip_status if skip_status else None, clusters if clusters else None, iteration_id, page, per_page, sort_column, sort_direction, col_filters=col_filters)

    return jsonify({
        "ok": True,
        "data": data,
        "total": total,
        "message": f"Loaded {len(data)} products"
    })


@pricing_grid_bp.post("/api/update-skip-status")
def api_update_skip_status():
    """Update skip status for a single product"""
    payload = request.get_json(silent=True) or {}
    product_id = payload.get("product_id")
    skip_status = payload.get("skip_status")

    if product_id is None:
        return jsonify({"ok": False, "message": "Product ID is required."})

    from models.base.base import SessionLocal
    from repositories.pricing.product_repository import ProductRepository

    db = SessionLocal()
    try:
        repo = ProductRepository(db)
        repo.update_skip_status(product_id, skip_status)
        db.commit()
        return jsonify({"ok": True, "message": "Skip status updated successfully"})
    except Exception as e:
        db.rollback()
        return jsonify({"ok": False, "message": f"Error: {str(e)}"})
    finally:
        db.close()


@pricing_grid_bp.get("/api/columns")
def api_columns():
    """Return all available grid columns: pricing_product model columns + virtual computed columns.
    Labels come from pricing_product_column table where available, otherwise auto-generated.
    Adding a new column to pricing_product automatically appears here.
    """
    from models.base.base import SessionLocal
    from models.pricing.product import Product
    from repositories.pricing.product_column_repository import ProductColumnRepository
    from sqlalchemy import inspect as sa_inspect, Integer, Float, Numeric, String, DateTime
    from sqlalchemy.dialects.mysql import TINYINT, DECIMAL, INTEGER

    EXCLUDE_KEYS = {'product_id', 'group_id', 'brand_id', 'category_id',
                    'base_image_url', 'product_url', 'created_date',
                    'skip_status_updated_date', 'iteration_closed', 'dimension_failed',
                    'skip_status'}

    db = SessionLocal()
    try:
        label_map = {c.code: c.name for c in ProductColumnRepository(db).get_all() if c.code}
    finally:
        db.close()

    def _auto_label(key):
        return key.replace('_', ' ').title()

    # Columns that are tinyint but NOT boolean Yes/No flags
    NON_BOOLEAN_TINYINT = {'dbs_status', 'final_status', 'outlier_mode', 'skip_status'}

    def _col_type(sa_col, key):
        t = sa_col.type
        if isinstance(t, TINYINT) and key not in NON_BOOLEAN_TINYINT:
            return 'boolean'
        if isinstance(t, (Integer, Float, Numeric, TINYINT, DECIMAL, INTEGER)):
            return 'number'
        return 'string'

    model_cols = [
        {
            "key": col.key,
            "label": label_map.get(col.key, _auto_label(col.key)),
            "type": _col_type(col.columns[0], col.key)
        }
        for col in sa_inspect(Product).mapper.column_attrs
        if col.key not in EXCLUDE_KEYS
    ]

    VIRTUAL_COLS = [
        {"key": "eps",               "label": label_map.get("eps",               "EPS"),                  "type": "number"},
        {"key": "sample",            "label": label_map.get("sample",            "Sample"),               "type": "number"},
        {"key": "total_items",       "label": label_map.get("total_items",       "Total Items"),          "type": "number"},
        {"key": "analyzed_items",    "label": label_map.get("analyzed_items",    "Analyzed Items"),       "type": "number"},
        {"key": "pending_items",     "label": label_map.get("pending_items",     "Pending Items"),        "type": "number"},
        {"key": "outlier_items",     "label": label_map.get("outlier_items",     "Outlier Items"),        "type": "number"},
        {"key": "cluster",           "label": label_map.get("cluster",           "Cluster"),              "type": "string"},
        {"key": "cluster_items",     "label": label_map.get("cluster_items",     "Cluster Items"),        "type": "number"},
        {"key": "cluster_items_per", "label": label_map.get("cluster_items_per", "Cluster Items (%)"),   "type": "number"},
        {"key": "skip_status",       "label": label_map.get("skip_status",       "Skip Status"),          "type": "select"},
    ]

    seen = {c["key"] for c in model_cols}
    result = model_cols + [c for c in VIRTUAL_COLS if c["key"] not in seen]
    return jsonify(result)


@pricing_grid_bp.post("/api/export-data")
def api_export_data():
    """Export grid data to CSV using only the columns selected in the column selector"""
    payload = request.get_json(silent=True) or {}
    group_id = payload.get("group_id")
    if not group_id:
        return jsonify({"ok": False, "message": "No product group selected."}), 400

    import csv
    from io import StringIO
    from flask import make_response
    from models.base.base import SessionLocal
    from repositories.pricing.product_column_repository import ProductColumnRepository

    col_filters = payload.get("col_filters") or {}
    brands = payload.get("brands") or []
    categories = payload.get("categories") or []
    types = payload.get("types") or []
    final_status = payload.get("final_status") or []
    skip_status = payload.get("skip_status") or []
    clusters = payload.get("clusters") or []
    iteration_id = payload.get("iteration_id")
    selected_columns = payload.get("selected_columns") or []

    # Build ordered column map from DB; fall back to all if none selected
    db = SessionLocal()
    try:
        all_cols = ProductColumnRepository(db).get_all()
    finally:
        db.close()
    all_col_map = {c.code: c.name for c in all_cols if c.code}

    # All virtual columns with fallback labels
    virtual_labels = {
        'skip_status': 'Skip Status',
        'iteration_history': 'Final Status History',
        'eps': 'EPS',
        'sample': 'Sample',
        'total_items': 'Total Items',
        'analyzed_items': 'Analyzed Items',
        'pending_items': 'Pending Items',
        'outlier_items': 'Outlier Items',
        'cluster': 'Cluster',
        'cluster_items': 'Cluster Items',
        'cluster_items_per': 'Cluster Items (%)',
    }
    for k, v in virtual_labels.items():
        if k not in all_col_map:
            all_col_map[k] = v

    if selected_columns:
        export_cols = [(k, all_col_map.get(k, k)) for k in selected_columns]
    else:
        export_cols = list(all_col_map.items())

    si = StringIO()
    writer = csv.writer(si)
    writer.writerow([label for _, label in export_cols])

    page = 1
    chunk_size = 5000
    while True:
        data, _ = grid.load_grid_data(group_id, brands, categories, types,
                                      final_status or None, skip_status or None, clusters or None,
                                      iteration_id, page, chunk_size, None, 'asc', skip_count=True, col_filters=col_filters)
        if not data:
            break
        for row in data:
            csv_row = []
            for key, _ in export_cols:
                if key == 'skip_status':
                    val = 'Yes' if row.get('skip_status') == 1 else ('No' if row.get('skip_status') == 0 else '-')
                elif key in ('mor', 'is_map_violation', 'is_loss_item', 'is_underpriced', 'is_overpriced', 'is_overpriced_above_map'):
                    val = 'Yes' if row.get(key) == 1 else 'No'
                elif key == 'iteration_history':
                    val = ' | '.join([f"EPS: {h['eps']}, Sample: {h['sample']}, Status: {h['status']}, {h['date']}" for h in row['iteration_history']]) if row.get('iteration_history') else ''
                elif key == 'cluster_items_per':
                    val = f"{row.get('cluster_items_per', 0):.2f}%"
                else:
                    val = row.get(key, '')
                csv_row.append(val)
            writer.writerow(csv_row)
        if len(data) < chunk_size:
            break
        page += 1

    output = make_response(si.getvalue())
    output.headers["Content-Disposition"] = "attachment; filename=grid_export.csv"
    output.headers["Content-type"] = "text/csv"
    return output


@pricing_grid_bp.post("/api/mark-status")
def api_mark_status():
    """Mark selected products as Normal / Outlier / Pending with optional issue note."""
    payload = request.get_json(silent=True) or {}
    product_ids = payload.get("product_ids") or []
    status = payload.get("status")          # 'Normal' | 'Outlier' | 'Pending'
    issue_note = (payload.get("issue_note") or "").strip()

    if not product_ids:
        return jsonify({"ok": False, "message": "No products selected."})
    if status not in ('Normal', 'Outlier', 'Pending'):
        return jsonify({"ok": False, "message": "Invalid status."})

    from models.base.base import SessionLocal
    from models.pricing.product import Product
    from datetime import datetime

    db = SessionLocal()
    try:
        products = db.query(Product).filter(Product.product_id.in_(product_ids)).all()
        now = datetime.utcnow()

        for p in products:
            if status == 'Normal':
                p.final_status = 1
                p.outlier_mode = None
                p.analyzed_date = now
                p.issue_note = None
            elif status == 'Outlier':
                p.final_status = 0
                p.outlier_mode = 1
                p.analyzed_date = now
                if issue_note:
                    existing = (p.issue_note or '').strip()
                    p.issue_note = (existing + ', ' + issue_note) if existing else issue_note
            elif status == 'Pending':
                p.final_status = None
                p.outlier_mode = None
                p.analyzed_date = None
                p.issue_note = None

        db.commit()
        return jsonify({"ok": True, "message": f"Updated {len(products)} product(s) to {status}."})
    except Exception as e:
        db.rollback()
        return jsonify({"ok": False, "message": f"Error: {str(e)}"})
    finally:
        db.close()


@pricing_grid_bp.post("/api/save-skip-status")
def api_save_skip_status():
    """Save skip status for selected products"""
    payload = request.get_json(silent=True) or {}
    skip_items = payload.get("skip_items") or []

    if not skip_items:
        return jsonify({"ok": False, "message": "No items to save."})

    from models.base.base import SessionLocal
    from models.pricing.product import Product

    db = SessionLocal()
    try:
        product_ids = [item["product_id"] for item in skip_items]
        products = db.query(Product).filter(Product.product_id.in_(product_ids)).all()

        skip_map = {item["product_id"]: item["skip_status"] for item in skip_items}

        for product in products:
            if product.product_id in skip_map:
                product.skip_status = skip_map[product.product_id]

        db.commit()
        return jsonify({"ok": True, "message": f"Saved skip status for {len(skip_items)} products"})
    except Exception as e:
        db.rollback()
        return jsonify({"ok": False, "message": f"Error: {str(e)}"})
    finally:
        db.close()
