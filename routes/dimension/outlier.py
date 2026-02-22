import io
import os
import uuid
from io import StringIO

import pandas as pd
from flask import Blueprint, jsonify, make_response, render_template, request

from services.dimension import outlier


outlier_bp = Blueprint("outlier_bp", __name__, url_prefix="/outlier")
OUTLIER_STATE = {}


def _state_set(payload):
    token = uuid.uuid4().hex
    OUTLIER_STATE[token] = payload
    if len(OUTLIER_STATE) > 30:
        oldest = list(OUTLIER_STATE.keys())[0]
        OUTLIER_STATE.pop(oldest, None)
    return token


def _column_rename_map():
    return {
        "SKU": "web_id",
        "Brand": "brand",
        "Category": "category",
        "Type": "product_type",
        "Name": "name",
        "H": "height",
        "W": "width",
        "D": "depth",
        "imageUrl": "base_image_url",
    }


@outlier_bp.get("")
@outlier_bp.get("/")
def outlier_page():
    return render_template("dimension/outlier/index.html", active_page="outlier")


@outlier_bp.get("/api/files")
def api_files():
    files = outlier.get_csv_files_from_items_folder()
    return jsonify({"files": files, "default_file": files[0] if files else None})


@outlier_bp.get("/api/info")
def api_info():
    filename = request.args.get("filename")
    if not filename:
        return jsonify({"ok": False, "message": "No file selected."})
    df = outlier.load_data_from_file(filename)
    if df.empty:
        return jsonify({"ok": False, "message": f"Failed to load file: {filename}"})
    return jsonify(
        {
            "ok": True,
            "message": f"Loaded: {filename} | Total Products: {len(df)} | Columns: {', '.join(df.columns.tolist())}",
        }
    )


@outlier_bp.get("/api/brands")
def api_brands():
    filename = request.args.get("filename")
    if not filename:
        return jsonify({"ok": False, "options": []})
    df = outlier.load_data_from_file(filename)
    if df.empty or "Brand" not in df.columns:
        return jsonify({"ok": False, "options": []})
    brand_counts = df["Brand"].value_counts().to_dict()
    options = [{"label": f"{b} ({brand_counts[b]})", "value": b} for b in sorted(df["Brand"].dropna().unique())]
    return jsonify({"ok": True, "options": options})


@outlier_bp.post("/api/categories")
def api_categories():
    payload = request.get_json(silent=True) or {}
    filename = payload.get("filename")
    brands = payload.get("brands") or []
    if not filename:
        return jsonify({"ok": False, "options": []})

    df = outlier.load_data_from_file(filename)
    if df.empty or "Category" not in df.columns:
        return jsonify({"ok": False, "options": []})

    if brands:
        filtered_df = df[df["Brand"].isin(brands)]
    else:
        filtered_df = df
    category_counts = filtered_df["Category"].value_counts().to_dict()
    options = [{"label": f"{c} ({category_counts[c]})", "value": c} for c in sorted(filtered_df["Category"].dropna().unique())]
    return jsonify({"ok": True, "options": options})


@outlier_bp.post("/api/analyze")
def api_analyze():
    payload = request.get_json(silent=True) or {}
    filename = payload.get("filename")
    selected_algorithms = payload.get("algorithms") or []
    brands = payload.get("brands") or []
    categories = payload.get("categories") or []

    if not filename:
        return jsonify({"ok": False, "message": "No file selected."})
    if not selected_algorithms:
        return jsonify({"ok": False, "message": "Please select at least one detection algorithm."})
    if not categories:
        return jsonify({"ok": False, "message": "Category selection is required to proceed with analysis."})

    df = outlier.load_data_from_file(filename)
    if df.empty:
        return jsonify({"ok": False, "message": f"Failed to load file: {filename}"})

    try:
        log_filename, output_path, total_analyzed, final_valid, final_invalid, summary_data, processed_df = outlier.analyze_and_export(
            df, brands, categories, selected_algorithms, filename
        )
        normal_path, outlier_path = outlier.generate_filtered_csvs(processed_df)

        # Convert numpy/pandas int64 to Python int
        summary_data_sorted = sorted(summary_data, key=lambda x: (x["brand"], -x["outlier"]))
        for row in summary_data_sorted:
            row["total"] = int(row["total"])
            row["normal"] = int(row["normal"])
            row["outlier"] = int(row["outlier"])
            row["normal_pct"] = float(row["normal_pct"])
            row["outlier_pct"] = float(row["outlier_pct"])
        
        show_detailed_summary = total_analyzed < 20000

        token = _state_set(
            {
                "processed_df_json": processed_df.to_json(date_format="iso", orient="split"),
                "log_filename": log_filename,
                "output_path": output_path,
                "normal_path": normal_path,
                "outlier_path": outlier_path,
            }
        )

        return jsonify(
            {
                "ok": True,
                "token": token,
                "summary": {
                    "filename": filename,
                    "algorithms": selected_algorithms,
                    "brands": brands,
                    "categories": categories,
                    "total_analyzed": int(total_analyzed),
                    "final_valid": int(final_valid),
                    "final_invalid": int(final_invalid),
                    "show_detailed_summary": show_detailed_summary,
                    "rows": summary_data_sorted,
                },
            }
        )
    except Exception as e:
        return jsonify({"ok": False, "message": str(e)})


@outlier_bp.get("/download/log")
def download_log():
    token = request.args.get("token")
    state = OUTLIER_STATE.get(token)
    if not state:
        return make_response("Session expired. Please re-run analysis.", 400)
    path = state.get("log_filename")
    if not path or not os.path.exists(path):
        return make_response("Log file not found.", 404)
    with open(path, "rb") as f:
        data = f.read()
    resp = make_response(data)
    resp.headers["Content-Type"] = "text/plain; charset=utf-8"
    resp.headers["Content-Disposition"] = "attachment; filename=analysis.log"
    return resp


@outlier_bp.get("/download/all")
def download_all():
    token = request.args.get("token")
    state = OUTLIER_STATE.get(token)
    if not state:
        return make_response("Session expired. Please re-run analysis.", 400)
    path = state.get("output_path")
    if not path or not os.path.exists(path):
        return make_response("Results file not found.", 404)
    with open(path, "rb") as f:
        data = f.read()
    resp = make_response(data)
    resp.headers["Content-Type"] = "text/csv"
    resp.headers["Content-Disposition"] = "attachment; filename=analysis_results.csv"
    return resp


@outlier_bp.get("/download/normal-items")
def download_normal_items():
    token = request.args.get("token")
    state = OUTLIER_STATE.get(token)
    if not state:
        return make_response("Session expired. Please re-run analysis.", 400)
    path = state.get("normal_path")
    if not path or not os.path.exists(path):
        return make_response("Normal items file not found.", 404)
    with open(path, "rb") as f:
        data = f.read()
    resp = make_response(data)
    resp.headers["Content-Type"] = "text/csv"
    resp.headers["Content-Disposition"] = "attachment; filename=normal_items.csv"
    return resp


@outlier_bp.get("/download/outlier-items")
def download_outlier_items():
    token = request.args.get("token")
    state = OUTLIER_STATE.get(token)
    if not state:
        return make_response("Session expired. Please re-run analysis.", 400)
    path = state.get("outlier_path")
    if not path or not os.path.exists(path):
        return make_response("Outlier items file not found.", 404)
    with open(path, "rb") as f:
        data = f.read()
    resp = make_response(data)
    resp.headers["Content-Type"] = "text/csv"
    resp.headers["Content-Disposition"] = "attachment; filename=outlier_items.csv"
    return resp


@outlier_bp.get("/download/normal")
def download_normal_by_brand_category():
    token = request.args.get("token")
    brand = request.args.get("brand")
    category = request.args.get("category")
    state = OUTLIER_STATE.get(token)
    if not state:
        return make_response("Session expired. Please re-run analysis.", 400)
    if not brand or not category:
        return make_response("brand and category are required.", 400)

    processed_df = pd.read_json(StringIO(state["processed_df_json"]), orient="split")
    filtered = processed_df[
        (processed_df["Brand"].astype(str) == str(brand))
        & (processed_df["Category"].astype(str) == str(category))
        & (processed_df["final_status"] == "Yes")
    ].copy()
    filtered = filtered.rename(columns=_column_rename_map())

    filename_safe_brand = str(brand).replace(" ", "_").replace("/", "-")
    filename_safe_category = str(category).replace(" ", "_").replace("/", "-")
    filename = f"normal_{filename_safe_brand}_{filename_safe_category}.csv"
    buffer = io.StringIO()
    filtered.to_csv(buffer, index=False)
    resp = make_response(buffer.getvalue())
    resp.headers["Content-Type"] = "text/csv"
    resp.headers["Content-Disposition"] = f"attachment; filename={filename}"
    return resp


@outlier_bp.get("/download/outlier")
def download_outlier_by_brand_category():
    token = request.args.get("token")
    brand = request.args.get("brand")
    category = request.args.get("category")
    state = OUTLIER_STATE.get(token)
    if not state:
        return make_response("Session expired. Please re-run analysis.", 400)
    if not brand or not category:
        return make_response("brand and category are required.", 400)

    processed_df = pd.read_json(StringIO(state["processed_df_json"]), orient="split")
    filtered = processed_df[
        (processed_df["Brand"].astype(str) == str(brand))
        & (processed_df["Category"].astype(str) == str(category))
        & (processed_df["final_status"] == "No")
    ].copy()
    filtered = filtered.rename(columns=_column_rename_map())

    filename_safe_brand = str(brand).replace(" ", "_").replace("/", "-")
    filename_safe_category = str(category).replace(" ", "_").replace("/", "-")
    filename = f"outlier_{filename_safe_brand}_{filename_safe_category}.csv"
    buffer = io.StringIO()
    filtered.to_csv(buffer, index=False)
    resp = make_response(buffer.getvalue())
    resp.headers["Content-Type"] = "text/csv"
    resp.headers["Content-Disposition"] = f"attachment; filename={filename}"
    return resp
