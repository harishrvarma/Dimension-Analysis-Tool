from flask import Blueprint, jsonify, render_template, request
from services.dimension import grid


grid_bp = Blueprint("grid_bp", __name__, url_prefix="/grid")


@grid_bp.get("")
@grid_bp.get("/")
def grid_page():
    return render_template("dimension/grid/index.html", active_page="grid")


@grid_bp.get("/api/product-groups")
def api_product_groups():
    """Get all product groups with default selection"""
    groups, default_group_id = grid.get_product_groups()
    return jsonify({"groups": groups, "default_group_id": default_group_id})


@grid_bp.post("/api/options")
def api_options():
    """Get brands and categories based on filters"""
    payload = request.get_json(silent=True) or {}
    group_id = payload.get("group_id")
    brands = payload.get("brands") or []
    categories = payload.get("categories") or []
    final_status = payload.get("final_status") or []
    
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


@grid_bp.post("/api/grid-data")
def api_grid_data():
    """Load grid data based on filters"""
    payload = request.get_json(silent=True) or {}
    group_id = payload.get("group_id")
    brands = payload.get("brands") or []
    categories = payload.get("categories") or []
    types = payload.get("types") or []
    final_status = payload.get("final_status") or []
    skip_status = payload.get("skip_status") or []
    iteration = payload.get("iteration")
    page = payload.get("page", 1)
    per_page = payload.get("per_page", 50)
    sort_column = payload.get("sort_column")
    sort_direction = payload.get("sort_direction", "asc")
    
    if not group_id:
        return jsonify({"ok": False, "message": "No product group selected.", "data": [], "total": 0})
    
    data, total = grid.load_grid_data(group_id, brands, categories, types, final_status if final_status else None, skip_status if skip_status else None, iteration, page, per_page, sort_column, sort_direction)
    
    return jsonify({
        "ok": True,
        "data": data,
        "total": total,
        "message": f"Loaded {len(data)} products"
    })


@grid_bp.post("/api/update-skip-status")
def api_update_skip_status():
    """Update skip status for a single product"""
    payload = request.get_json(silent=True) or {}
    product_id = payload.get("product_id")
    skip_status = payload.get("skip_status")
    
    if product_id is None:
        return jsonify({"ok": False, "message": "Product ID is required."})
    
    from models.base.base import SessionLocal
    from repositories.product_repository import ProductRepository
    
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


@grid_bp.post("/api/export-data")
def api_export_data():
    """Export grid data to CSV"""
    payload = request.get_json(silent=True) or {}
    group_id = payload.get("group_id")
    brands = payload.get("brands") or []
    categories = payload.get("categories") or []
    types = payload.get("types") or []
    final_status = payload.get("final_status") or []
    iteration = payload.get("iteration")
    
    if not group_id:
        return jsonify({"ok": False, "message": "No product group selected."}), 400
    
    import csv
    from io import StringIO
    from flask import make_response
    
    data, total = grid.load_grid_data(group_id, brands, categories, types, final_status if final_status else None, None, iteration, 1, 999999, None, 'asc')
    
    si = StringIO()
    writer = csv.writer(si)
    
    writer.writerow(['Product ID', 'QB Code', 'Brand', 'Category', 'Type', 'Name', 'Height', 'Width', 'Depth', 
                     'IQR Height Status', 'IQR Width Status', 'IQR Depth Status', 'IQR Status', 
                     'DBSCAN Status', 'Final Status', 'Skip Status'])
    
    for row in data:
        writer.writerow([
            row['sku'], row['qb_code'], row['brand'], row['category'], row['product_type'], row['name'],
            row['height'], row['width'], row['depth'], row['iqr_height_status'], row['iqr_width_status'],
            row['iqr_depth_status'], row['iqr_status'], row['dbs_status'], row['final_status'],
            'Ignored' if row['skip_status'] == 1 else 'Not Ignored' if row['skip_status'] == 0 else ''
        ])
    
    output = make_response(si.getvalue())
    output.headers["Content-Disposition"] = "attachment; filename=grid_export.csv"
    output.headers["Content-type"] = "text/csv"
    return output


@grid_bp.post("/api/export-xls")
def api_export_xls():
    """Export grid data to XLS with red background for outlier dimensions"""
    payload = request.get_json(silent=True) or {}
    group_id = payload.get("group_id")
    brands = payload.get("brands") or []
    categories = payload.get("categories") or []
    types = payload.get("types") or []
    final_status = payload.get("final_status") or []
    iteration = payload.get("iteration")
    
    if not group_id:
        return jsonify({"ok": False, "message": "No product group selected."}), 400
    
    from io import BytesIO
    from flask import make_response
    try:
        from openpyxl import Workbook
        from openpyxl.styles import PatternFill
    except ImportError:
        return jsonify({"ok": False, "message": "openpyxl not installed"}), 500
    
    data, total = grid.load_grid_data(group_id, brands, categories, types, final_status if final_status else None, None, iteration, 1, 999999, None, 'asc')
    
    wb = Workbook()
    ws = wb.active
    ws.title = "Grid Export"
    
    headers = ['Product ID', 'QB Code', 'Brand', 'Category', 'Type', 'Name', 'Product URL', 'Image URL', 'Height', 'Width', 'Depth', 
               'DBSCAN Status', 'Final Status', 'Skip Status']
    ws.append(headers)
    
    red_fill = PatternFill(start_color="FFCCCB", end_color="FFCCCB", fill_type="solid")
    
    for row in data:
        row_data = [
            row['sku'], row['qb_code'], row['brand'], row['category'], row['product_type'], row['name'],
            row.get('product_url', ''), row.get('base_image_url', ''),
            row['height'], row['width'], row['depth'], row['dbs_status'], row['final_status'],
            'Ignored' if row['skip_status'] == 1 else 'Not Ignored' if row['skip_status'] == 0 else ''
        ]
        ws.append(row_data)
        
        current_row = ws.max_row
        if row['iqr_height_status'] == 'Outlier':
            ws.cell(row=current_row, column=9).fill = red_fill
        if row['iqr_width_status'] == 'Outlier':
            ws.cell(row=current_row, column=10).fill = red_fill
        if row['iqr_depth_status'] == 'Outlier':
            ws.cell(row=current_row, column=11).fill = red_fill
    
    output = BytesIO()
    wb.save(output)
    output.seek(0)
    
    response = make_response(output.getvalue())
    response.headers["Content-Disposition"] = "attachment; filename=grid_export.xlsx"
    response.headers["Content-type"] = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    return response


@grid_bp.post("/api/save-skip-status")
def api_save_skip_status():
    """Save skip status for selected products"""
    payload = request.get_json(silent=True) or {}
    skip_items = payload.get("skip_items") or []
    
    if not skip_items:
        return jsonify({"ok": False, "message": "No items to save."})
    
    from models.base.base import SessionLocal
    from models.product import Product
    
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
