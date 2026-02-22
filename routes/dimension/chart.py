import io
import json
from io import StringIO

import numpy as np
import pandas as pd
from flask import Blueprint, jsonify, make_response, render_template, request

from services.dimension import chart


chart_bp = Blueprint("chart_bp", __name__, url_prefix="/chart")


@chart_bp.get("")
@chart_bp.get("/")
def chart_page():
    return render_template("dimension/chart/index.html", active_page="chart")


@chart_bp.get("/api/files")
def api_files():
    files = chart.get_csv_files_from_items_folder()
    return jsonify({"files": files, "default_file": files[0] if files else None})


@chart_bp.post("/api/options")
def api_options():
    payload = request.get_json(silent=True) or {}
    filename = payload.get("filename")
    brands = payload.get("brands") or []
    category = payload.get("category")

    if not filename:
        return jsonify(
            {
                "ok": False,
                "message": "No file selected.",
                "brand_options": [],
                "category_options": [],
                "type_options": [],
            }
        )

    df = chart.load_data_from_file(filename)
    if df.empty:
        return jsonify(
            {
                "ok": False,
                "message": f"Failed to load file: {filename}",
                "brand_options": [],
                "category_options": [],
                "type_options": [],
            }
        )

    brand_options = chart.build_counts(df["Brand"])

    if brands:
        df_for_category = df[df["Brand"].isin(brands)]
    else:
        df_for_category = df
    category_options = chart.build_counts(df_for_category["Category"])

    df_for_type = df.copy()
    if brands:
        df_for_type = df_for_type[df_for_type["Brand"].isin(brands)]
    if category:
        df_for_type = df_for_type[df_for_type["Category"] == category]
    type_options = chart.build_counts(df_for_type["Type"]) if not df_for_type.empty else []

    return jsonify(
        {
            "ok": True,
            "message": f"Loaded: {filename} | Total Products: {len(df)} | Columns: {', '.join(df.columns.tolist())}",
            "brand_options": brand_options,
            "category_options": category_options,
            "type_options": type_options,
        }
    )


@chart_bp.post("/api/analyze")
def api_analyze():
    payload = request.get_json(silent=True) or {}
    filename = payload.get("filename")
    brands = payload.get("brands") or []
    category = payload.get("category")
    types = payload.get("types") or []
    selected_algorithms = payload.get("algorithms") or []
    top_outliers_filter = payload.get("top_outliers_filter", "all")
    h_mult = float(payload.get("h_mult", 1.5))
    w_mult = float(payload.get("w_mult", 1.5))
    d_mult = float(payload.get("d_mult", 1.5))

    if not filename:
        return jsonify({"ok": False, "message": "No file selected."})
    df = chart.load_data_from_file(filename)
    if df.empty:
        return jsonify({"ok": False, "message": f"Failed to load file: {filename}"})
    if not selected_algorithms:
        return jsonify({"ok": False, "message": "Please select at least one detection algorithm."})
    if not category:
        return jsonify({"ok": False, "message": "Please select Category (required)."})

    filtered = df.copy()
    if brands:
        filtered = filtered[filtered["Brand"].isin(brands)]
    filtered = filtered[filtered["Category"] == category]
    if types:
        filtered = filtered[filtered["Type"].isin(types)]

    if len(filtered) < 4:
        return jsonify({"ok": False, "message": f"Need at least 4 products for analysis. Currently showing: {len(filtered)}"})

    multipliers = {"H": h_mult, "W": w_mult, "D": d_mult}
    filtered, _ = chart.combine_algorithm_results(filtered, selected_algorithms, multipliers)
    filtered["is_outlier"] = filtered["is_outlier_combined"]
    filtered["Label"] = filtered["is_outlier"].map({True: "Outlier", False: "Normal"})

    normals = filtered[filtered["Label"] == "Normal"]
    outliers = filtered[filtered["Label"] == "Outlier"]
    outliers_to_display = outliers.copy()
    normals_to_display = normals.copy()

    if top_outliers_filter != "all" and top_outliers_filter is not None and len(outliers) > 0:
        normals_to_display = pd.DataFrame()
        outliers_scored = outliers.copy()
        if "H_lower_bound" in outliers.columns and "H_upper_bound" in outliers.columns:
            outliers_scored["outlier_score"] = 0.0
            for dim in ["H", "W", "D"]:
                lower_col = f"{dim}_lower_bound"
                upper_col = f"{dim}_upper_bound"
                if lower_col in outliers_scored.columns and upper_col in outliers_scored.columns:
                    below_distance = (outliers_scored[lower_col] - outliers_scored[dim]).clip(lower=0)
                    above_distance = (outliers_scored[dim] - outliers_scored[upper_col]).clip(lower=0)
                    outliers_scored["outlier_score"] += below_distance + above_distance
        else:
            centroid_h = filtered["H"].mean()
            centroid_w = filtered["W"].mean()
            centroid_d = filtered["D"].mean()
            outliers_scored["outlier_score"] = np.sqrt(
                (outliers_scored["H"] - centroid_h) ** 2
                + (outliers_scored["W"] - centroid_w) ** 2
                + (outliers_scored["D"] - centroid_d) ** 2
            )
        top_n = int(top_outliers_filter)
        outliers_to_display = outliers_scored.nlargest(top_n, "outlier_score") if len(outliers_scored) >= top_n else outliers_scored

    fig = chart.create_figure(filtered, outliers_to_display, normals_to_display, selected_algorithms)
    token = chart.state_set(
        {
            "filtered_data_json": filtered.to_json(date_format="iso", orient="split"),
            "multipliers": multipliers,
            "selected_algorithms": selected_algorithms,
        }
    )

    total = len(filtered)
    outlier_count = len(outliers)
    displayed_outliers = len(outliers_to_display)
    outlier_pct = (outlier_count / total * 100) if total > 0 else 0
    top_options = [{"label": "All Outliers", "value": "all", "disabled": False}]
    for threshold, label in [
        (5, "Top 5 Outliers"),
        (10, "Top 10 Outliers"),
        (20, "Top 20 Outliers"),
        (30, "Top 30 Outliers"),
        (50, "Top 50 Outliers"),
        (100, "Top 50+ Outliers"),
    ]:
        top_options.append({"label": label, "value": threshold, "disabled": outlier_count < threshold})

    return jsonify(
        {
            "ok": True,
            "token": token,
            "figure": json.loads(fig.to_json()),
            "stats": {
                "total": total,
                "normal_count": len(normals),
                "outlier_count": outlier_count,
                "displayed_outliers": displayed_outliers,
                "outlier_pct": round(outlier_pct, 1),
            },
            "top_options": top_options,
            "message": "Analysis complete.",
        }
    )


@chart_bp.get("/api/preview")
def api_preview():
    token = request.args.get("token")
    sku = request.args.get("sku")
    state = chart.CHART_STATE.get(token)
    if not state:
        return jsonify({"ok": False, "message": "Session expired. Please re-run analysis."})
    filtered = pd.read_json(StringIO(state["filtered_data_json"]), orient="split")
    row = filtered[filtered["SKU"].astype(str) == str(sku)]
    if row.empty:
        return jsonify({"ok": False, "message": "Product not found."})

    product = row.iloc[0]
    data = {
        "sku": product.get("SKU", ""),
        "brand": product.get("Brand", ""),
        "category": product.get("Category", ""),
        "type": product.get("Type", ""),
        "imageUrl": product.get("imageUrl", ""),
        "H": product.get("H", ""),
        "W": product.get("W", ""),
        "D": product.get("D", ""),
        "is_outlier": bool(product.get("is_outlier", False)),
        "algorithms": state.get("selected_algorithms", []),
    }
    for key in [
        "H_IQR",
        "W_IQR",
        "D_IQR",
        "dbscan_cluster",
        "H_lower_bound",
        "H_upper_bound",
        "W_lower_bound",
        "W_upper_bound",
        "D_lower_bound",
        "D_upper_bound",
    ]:
        if key in product.index:
            val = product.get(key)
            data[key] = float(val) if pd.notna(val) else None

    return jsonify({"ok": True, "product": data})


@chart_bp.get("/download/<kind>")
def download_csv(kind):
    token = request.args.get("token")
    state = chart.CHART_STATE.get(token)
    if not state:
        return make_response("Session expired. Please re-run analysis.", 400)

    filtered = pd.read_json(StringIO(state["filtered_data_json"]), orient="split")
    multipliers = state["multipliers"]
    selected_algorithms = state["selected_algorithms"]
    export_df = chart.prepare_export_dataframe(filtered, multipliers, selected_algorithms)

    if kind == "normal":
        if "iqr_status" in export_df.columns and "dbscan_status" in export_df.columns:
            export_df = export_df[(export_df["iqr_status"] == "Yes") | (export_df["dbscan_status"] == "Yes")]
        elif "iqr_status" in export_df.columns:
            export_df = export_df[export_df["iqr_status"] == "Yes"]
        elif "dbscan_status" in export_df.columns:
            export_df = export_df[export_df["dbscan_status"] == "Yes"]
        filename = f"normal_products_{'_'.join(selected_algorithms)}.csv"
    elif kind == "outliers":
        if "iqr_status" in export_df.columns and "dbscan_status" in export_df.columns:
            export_df = export_df[(export_df["iqr_status"] == "No") & (export_df["dbscan_status"] == "No")]
        elif "iqr_status" in export_df.columns:
            export_df = export_df[export_df["iqr_status"] == "No"]
        elif "dbscan_status" in export_df.columns:
            export_df = export_df[export_df["dbscan_status"] == "No"]
        filename = f"outlier_products_{'_'.join(selected_algorithms)}.csv"
    else:
        filename = f"all_products_{'_'.join(selected_algorithms)}.csv"

    buffer = io.StringIO()
    export_df.to_csv(buffer, index=False)
    resp = make_response(buffer.getvalue())
    resp.headers["Content-Type"] = "text/csv"
    resp.headers["Content-Disposition"] = f"attachment; filename={filename}"
    return resp
