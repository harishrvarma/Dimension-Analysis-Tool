import io
import json
import os
import uuid
from io import StringIO

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler


def get_csv_files_from_items_folder():
    items_folder = "var/items"
    if not os.path.exists(items_folder):
        os.makedirs(items_folder)
        return []
    csv_files = [f for f in os.listdir(items_folder) if f.endswith(".csv")]
    return sorted(csv_files)


def load_data_from_file(filename):
    if not filename:
        return pd.DataFrame()

    csv_file = os.path.join("var/items", filename)
    if not os.path.exists(csv_file):
        return pd.DataFrame()

    df = pd.read_csv(csv_file, low_memory=False)

    column_mapping = {col.lower(): col for col in df.columns}
    standard_columns = {
        "web_id": "SKU",
        "product_id": "product_id",
        "brand": "Brand",
        "category": "Category",
        "product_type": "Type",
        "name": "Name",
        "height": "H",
        "width": "W",
        "depth": "D",
        "weight": "weight",
        "base_image_url": "imageUrl",
        "url_key": "url_key",
        "imageurl": "imageUrl",
        "sku": "SKU",
        "type": "Type",
        "h": "H",
        "w": "W",
        "d": "D",
    }

    rename_dict = {}
    for lower_col, actual_col in column_mapping.items():
        if lower_col in standard_columns:
            rename_dict[actual_col] = standard_columns[lower_col]
    df = df.rename(columns=rename_dict)

    for col in ["H", "W", "D"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "weight" in df.columns:
        df["weight"] = pd.to_numeric(df["weight"], errors="coerce")

    required_cols = ["H", "W", "D"]
    existing_required = [col for col in required_cols if col in df.columns]
    if len(existing_required) != len(required_cols):
        return pd.DataFrame()

    df = df.dropna(subset=existing_required)
    return df


def calculate_iqr_bounds(data_subset, dimensions=None, multipliers=None):
    if dimensions is None:
        dimensions = ["H", "W", "D"]
    if multipliers is None:
        multipliers = {"H": 1.5, "W": 1.5, "D": 1.5}

    iqr_stats = {}
    for dim in dimensions:
        q1 = data_subset[dim].quantile(0.25)
        q3 = data_subset[dim].quantile(0.75)
        iqr = q3 - q1
        mult = multipliers.get(dim, 1.5)
        iqr_stats[dim] = {
            "Q1": q1,
            "Q3": q3,
            "IQR": iqr,
            "lower_bound": q1 - (mult * iqr),
            "upper_bound": q3 + (mult * iqr),
            "multiplier": mult,
        }
    return iqr_stats


def detect_outliers_iqr(df_subset, iqr_stats):
    is_outlier = pd.Series([False] * len(df_subset), index=df_subset.index)
    for dim in ["H", "W", "D"]:
        lower = iqr_stats[dim]["lower_bound"]
        upper = iqr_stats[dim]["upper_bound"]
        is_outlier |= (df_subset[dim] < lower) | (df_subset[dim] > upper)
    return is_outlier


def detect_outliers_per_dimension_iqr(df_subset, iqr_stats):
    outlier_flags = {}
    for dim in ["H", "W", "D"]:
        lower = iqr_stats[dim]["lower_bound"]
        upper = iqr_stats[dim]["upper_bound"]
        if dim == "H":
            col_name = "iqr_o_height"
        elif dim == "W":
            col_name = "iqr_o_width"
        else:
            col_name = "iqr_o_depth"
        outlier_flags[col_name] = ((df_subset[dim] < lower) | (df_subset[dim] > upper)).map(
            {True: "Yes", False: "No"}
        )
    return outlier_flags


def calculate_dynamic_iqr(filtered_df, multipliers=None):
    if multipliers is None:
        multipliers = {"H": 1.5, "W": 1.5, "D": 1.5}

    df_enriched = filtered_df.copy()
    for dim in ["H", "W", "D"]:
        df_enriched[f"{dim}_IQR"] = 0.0
        df_enriched[f"{dim}_Q1"] = 0.0
        df_enriched[f"{dim}_Q3"] = 0.0
        df_enriched[f"{dim}_lower_bound"] = 0.0
        df_enriched[f"{dim}_upper_bound"] = 0.0
        df_enriched[f"{dim}_multiplier"] = multipliers[dim]

    unique_types = df_enriched["Type"].unique()
    if len(unique_types) > 1:
        for product_type in unique_types:
            type_mask = df_enriched["Type"] == product_type
            type_data = df_enriched[type_mask]
            if len(type_data) >= 4:
                iqr_stats = calculate_iqr_bounds(type_data, multipliers=multipliers)
                for dim in ["H", "W", "D"]:
                    df_enriched.loc[type_mask, f"{dim}_IQR"] = iqr_stats[dim]["IQR"]
                    df_enriched.loc[type_mask, f"{dim}_Q1"] = iqr_stats[dim]["Q1"]
                    df_enriched.loc[type_mask, f"{dim}_Q3"] = iqr_stats[dim]["Q3"]
                    df_enriched.loc[type_mask, f"{dim}_lower_bound"] = iqr_stats[dim]["lower_bound"]
                    df_enriched.loc[type_mask, f"{dim}_upper_bound"] = iqr_stats[dim]["upper_bound"]
    else:
        if len(df_enriched) >= 4:
            iqr_stats = calculate_iqr_bounds(df_enriched, multipliers=multipliers)
            for dim in ["H", "W", "D"]:
                df_enriched[f"{dim}_IQR"] = iqr_stats[dim]["IQR"]
                df_enriched[f"{dim}_Q1"] = iqr_stats[dim]["Q1"]
                df_enriched[f"{dim}_Q3"] = iqr_stats[dim]["Q3"]
                df_enriched[f"{dim}_lower_bound"] = iqr_stats[dim]["lower_bound"]
                df_enriched[f"{dim}_upper_bound"] = iqr_stats[dim]["upper_bound"]
    return df_enriched


def detect_outliers_dbscan(filtered_df, eps=1.0, min_samples=4):
    df_dbscan = filtered_df.copy()
    x_values = df_dbscan[["H", "W", "D"]].values

    scaler = StandardScaler()
    x_scaled = scaler.fit_transform(x_values)
    dbscan = DBSCAN(eps=eps, min_samples=min_samples, metric="euclidean")
    clusters = dbscan.fit_predict(x_scaled)

    is_outlier_dbscan = pd.Series((clusters == -1), index=df_dbscan.index)
    df_dbscan["dbscan_cluster"] = clusters
    df_dbscan["dbscan_is_outlier"] = is_outlier_dbscan
    df_dbscan["dbscan_o_height"] = is_outlier_dbscan.map({True: "Yes", False: "No"})
    df_dbscan["dbscan_o_width"] = is_outlier_dbscan.map({True: "Yes", False: "No"})
    df_dbscan["dbscan_o_depth"] = is_outlier_dbscan.map({True: "Yes", False: "No"})

    return is_outlier_dbscan, df_dbscan


def combine_algorithm_results(filtered_df, selected_algorithms, multipliers):
    df_combined = filtered_df.copy()
    df_combined["is_outlier_combined"] = False
    outlier_flags_by_algo = {}

    if "IQR" in selected_algorithms:
        df_iqr = calculate_dynamic_iqr(df_combined.copy(), multipliers=multipliers)
        df_iqr["iqr_is_outlier"] = False
        unique_types = df_iqr["Type"].unique()

        if len(unique_types) > 1:
            for product_type in unique_types:
                type_mask = df_iqr["Type"] == product_type
                type_data = df_iqr[type_mask]
                if len(type_data) >= 4:
                    iqr_stats = {}
                    for dim in ["H", "W", "D"]:
                        iqr_stats[dim] = {
                            "Q1": type_data[f"{dim}_Q1"].iloc[0],
                            "Q3": type_data[f"{dim}_Q3"].iloc[0],
                            "IQR": type_data[f"{dim}_IQR"].iloc[0],
                            "lower_bound": type_data[f"{dim}_lower_bound"].iloc[0],
                            "upper_bound": type_data[f"{dim}_upper_bound"].iloc[0],
                        }
                    outlier_mask = detect_outliers_iqr(type_data, iqr_stats)
                    df_iqr.loc[type_mask, "iqr_is_outlier"] = outlier_mask
                    outlier_flags = detect_outliers_per_dimension_iqr(type_data, iqr_stats)
                    for col, values in outlier_flags.items():
                        df_iqr.loc[type_mask, col] = values
        else:
            if len(df_iqr) >= 4:
                iqr_stats = {}
                for dim in ["H", "W", "D"]:
                    iqr_stats[dim] = {
                        "Q1": df_iqr[f"{dim}_Q1"].iloc[0],
                        "Q3": df_iqr[f"{dim}_Q3"].iloc[0],
                        "IQR": df_iqr[f"{dim}_IQR"].iloc[0],
                        "lower_bound": df_iqr[f"{dim}_lower_bound"].iloc[0],
                        "upper_bound": df_iqr[f"{dim}_upper_bound"].iloc[0],
                    }
                outlier_mask = detect_outliers_iqr(df_iqr, iqr_stats)
                df_iqr["iqr_is_outlier"] = outlier_mask
                outlier_flags = detect_outliers_per_dimension_iqr(df_iqr, iqr_stats)
                for col, values in outlier_flags.items():
                    df_iqr[col] = values

        outlier_flags_by_algo["IQR"] = df_iqr["iqr_is_outlier"]
        iqr_cols = [
            col
            for col in df_iqr.columns
            if col.startswith("iqr_")
            or col.endswith("_IQR")
            or "Q1" in col
            or "Q3" in col
            or "bound" in col
        ]
        for col in iqr_cols:
            df_combined[col] = df_iqr[col]

    if "DBSCAN" in selected_algorithms:
        _, df_dbscan = detect_outliers_dbscan(df_combined.copy(), eps=1.0, min_samples=4)
        outlier_flags_by_algo["DBSCAN"] = df_dbscan["dbscan_is_outlier"]
        for col in [c for c in df_dbscan.columns if c.startswith("dbscan_")]:
            df_combined[col] = df_dbscan[col]

    if outlier_flags_by_algo:
        first_algo = list(outlier_flags_by_algo.keys())[0]
        df_combined["is_outlier_combined"] = outlier_flags_by_algo[first_algo].values
        for algo_name, outlier_series in outlier_flags_by_algo.items():
            if algo_name != first_algo:
                df_combined["is_outlier_combined"] = df_combined["is_outlier_combined"] & outlier_series.values

    return df_combined, outlier_flags_by_algo


def prepare_export_dataframe(filtered_df, multipliers, selected_algorithms):
    df_export, _ = combine_algorithm_results(filtered_df.copy(), selected_algorithms, multipliers)
    export_data = {}
    export_data["brand"] = df_export["Brand"] if "Brand" in df_export.columns else None
    export_data["category"] = df_export["Category"] if "Category" in df_export.columns else None
    export_data["product_type"] = df_export["Type"] if "Type" in df_export.columns else None
    export_data["product_id"] = df_export["product_id"] if "product_id" in df_export.columns else None
    export_data["web_id"] = df_export["SKU"] if "SKU" in df_export.columns else None
    export_data["url_key"] = df_export["url_key"] if "url_key" in df_export.columns else None
    export_data["height"] = df_export["H"] if "H" in df_export.columns else None
    export_data["width"] = df_export["W"] if "W" in df_export.columns else None
    export_data["depth"] = df_export["D"] if "D" in df_export.columns else None

    if "IQR" in selected_algorithms:
        if "iqr_o_height" in df_export.columns:
            export_data["iqr_status"] = df_export.apply(
                lambda row: "No"
                if (
                    row.get("iqr_o_height") == "Yes"
                    or row.get("iqr_o_width") == "Yes"
                    or row.get("iqr_o_depth") == "Yes"
                )
                else "Yes",
                axis=1,
            )
            export_data["iqr_height"] = df_export["iqr_o_height"]
            export_data["iqr_width"] = df_export["iqr_o_width"]
            export_data["iqr_depth"] = df_export["iqr_o_depth"]
        else:
            export_data["iqr_status"] = "N/A"
            export_data["iqr_height"] = "N/A"
            export_data["iqr_width"] = "N/A"
            export_data["iqr_depth"] = "N/A"

    if "DBSCAN" in selected_algorithms:
        if "dbscan_is_outlier" in df_export.columns:
            export_data["dbscan_status"] = df_export["dbscan_is_outlier"].map({True: "No", False: "Yes"})
            export_data["dbscan_height"] = (
                df_export["dbscan_o_height"] if "dbscan_o_height" in df_export.columns else "N/A"
            )
            export_data["dbscan_width"] = (
                df_export["dbscan_o_width"] if "dbscan_o_width" in df_export.columns else "N/A"
            )
            export_data["dbscan_depth"] = (
                df_export["dbscan_o_depth"] if "dbscan_o_depth" in df_export.columns else "N/A"
            )
            export_data["dbscan_cluster"] = (
                df_export["dbscan_cluster"] if "dbscan_cluster" in df_export.columns else "N/A"
            )
        else:
            export_data["dbscan_status"] = "N/A"
            export_data["dbscan_height"] = "N/A"
            export_data["dbscan_width"] = "N/A"
            export_data["dbscan_depth"] = "N/A"
            export_data["dbscan_cluster"] = "N/A"

    result_df = pd.DataFrame(export_data).dropna(axis=1, how="all")
    return result_df


def build_counts(series):
    counts = series.value_counts().to_dict()
    return [{"label": f"{k} ({counts[k]})", "value": k} for k in sorted(series.dropna().unique())]


def create_figure(filtered, outliers_to_display, normals_to_display, selected_algorithms):
    padding = 0.15
    x_min, x_max = filtered["W"].min(), filtered["W"].max()
    y_min, y_max = filtered["H"].min(), filtered["H"].max()
    z_min, z_max = filtered["D"].min(), filtered["D"].max()

    x_pad = max((x_max - x_min) * padding, 5)
    y_pad = max((y_max - y_min) * padding, 5)
    z_pad = max((z_max - z_min) * padding, 5)
    fig = go.Figure()

    if len(selected_algorithms) == 1:
        if selected_algorithms[0] == "IQR":
            normal_color, outlier_color = "#4472C4", "#E74C3C"
        else:
            normal_color, outlier_color = "#2ecc71", "#e67e22"
    else:
        normal_color, outlier_color = "#9b59b6", "#c0392b"

    if not normals_to_display.empty:
        fig.add_trace(
            go.Scatter3d(
                x=normals_to_display["W"],
                y=normals_to_display["H"],
                z=normals_to_display["D"],
                mode="markers",
                marker=dict(
                    size=6,
                    color=normal_color,
                    opacity=0.8,
                    line=dict(color="#2E5090", width=1.5),
                    symbol="circle",
                ),
                name="Normal Products",
                customdata=normals_to_display[["SKU", "Brand", "Category", "Type", "H", "W", "D", "imageUrl"]].values,
                hovertemplate=(
                    "<b style='font-size:14px'>%{customdata[0]}</b><br>"
                    + "<b>Brand:</b> %{customdata[1]}<br>"
                    + "<b>Category:</b> %{customdata[2]}<br>"
                    + "<b>Type:</b> %{customdata[3]}<br><br>"
                    + "<b style='color:#2c3e50'>Dimensions:</b><br>"
                    + "Height: <b>%{customdata[4]} Inch</b><br>"
                    + "Width: <b>%{customdata[5]} Inch</b><br>"
                    + "Depth: <b>%{customdata[6]} Inch</b><br><extra></extra>"
                ),
                text=normals_to_display["SKU"],
            )
        )

    if not outliers_to_display.empty:
        if "IQR" in selected_algorithms and "H_lower_bound" in outliers_to_display.columns:
            outliers_display = outliers_to_display.copy()
            outliers_display["H_lower_bound"] = outliers_display["H_lower_bound"].clip(lower=0)
            outliers_display["W_lower_bound"] = outliers_display["W_lower_bound"].clip(lower=0)
            outliers_display["D_lower_bound"] = outliers_display["D_lower_bound"].clip(lower=0)
            custom_cols = [
                "SKU",
                "Brand",
                "Category",
                "Type",
                "H",
                "W",
                "D",
                "H_lower_bound",
                "H_upper_bound",
                "W_lower_bound",
                "W_upper_bound",
                "D_lower_bound",
                "D_upper_bound",
                "imageUrl",
            ]
            custom_values = outliers_display[custom_cols].values
            hovertemplate_outlier = (
                "<b style='color:#B45309; font-size:15px'>OUTLIER DETECTED</b><br>"
                + "<b style='font-size:14px; color:#111827'>%{customdata[0]}</b><br>"
                + "<span style='color:#334155'><b>Brand:</b> %{customdata[1]}</span><br>"
                + "<span style='color:#334155'><b>Category:</b> %{customdata[2]}</span><br>"
                + "<span style='color:#334155'><b>Type:</b> %{customdata[3]}</span><br><br>"
                + "<b style='color:#B45309'>Dimensions (Out of Range)</b><br>"
                + "<span style='color:#111827'>Height:</span> <b style='color:#C2410C'>%{customdata[4]} Inch</b> "
                + "<span style='color:#64748B'>(Expected: %{customdata[7]:.1f} - %{customdata[8]:.1f})</span><br>"
                + "<span style='color:#111827'>Width:</span> <b style='color:#C2410C'>%{customdata[5]} Inch</b> "
                + "<span style='color:#64748B'>(Expected: %{customdata[9]:.1f} - %{customdata[10]:.1f})</span><br>"
                + "<span style='color:#111827'>Depth:</span> <b style='color:#C2410C'>%{customdata[6]} Inch</b> "
                + "<span style='color:#64748B'>(Expected: %{customdata[11]:.1f} - %{customdata[12]:.1f})</span><br>"
                + f"<br><span style='color:#0F766E'><b>Algorithm(s):</b> {', '.join(selected_algorithms)}</span><extra></extra>"
            )
        else:
            custom_cols = ["SKU", "Brand", "Category", "Type", "H", "W", "D", "imageUrl"]
            custom_values = outliers_to_display[custom_cols].values
            hovertemplate_outlier = (
                "<b style='color:#B45309; font-size:15px'>OUTLIER DETECTED</b><br>"
                + "<b style='font-size:14px; color:#111827'>%{customdata[0]}</b><br>"
                + "<span style='color:#334155'><b>Brand:</b> %{customdata[1]}</span><br>"
                + "<span style='color:#334155'><b>Category:</b> %{customdata[2]}</span><br>"
                + "<span style='color:#334155'><b>Type:</b> %{customdata[3]}</span><br><br>"
                + "<b style='color:#B45309'>Dimensions</b><br>"
                + "<span style='color:#111827'>Height:</span> <b style='color:#C2410C'>%{customdata[4]} Inch</b><br>"
                + "<span style='color:#111827'>Width:</span> <b style='color:#C2410C'>%{customdata[5]} Inch</b><br>"
                + "<span style='color:#111827'>Depth:</span> <b style='color:#C2410C'>%{customdata[6]} Inch</b><br>"
                + f"<br><span style='color:#0F766E'><b>Algorithm(s):</b> {', '.join(selected_algorithms)}</span><extra></extra>"
            )

        fig.add_trace(
            go.Scatter3d(
                x=outliers_to_display["W"],
                y=outliers_to_display["H"],
                z=outliers_to_display["D"],
                mode="markers",
                marker=dict(
                    size=12,
                    color=outlier_color,
                    symbol="diamond",
                    opacity=0.95,
                    line=dict(color="#C0392B", width=2),
                ),
                hoverlabel=dict(
                    bgcolor="#FFF7ED",
                    bordercolor="#FDBA74",
                    font=dict(color="#7C2D12"),
                ),
                name="Outliers",
                customdata=custom_values,
                hovertemplate=hovertemplate_outlier,
                text=outliers_to_display["SKU"],
            )
        )

    fig.update_layout(
        scene=dict(
            xaxis=dict(
                title=dict(text="<b>Width (Inch)</b>", font=dict(size=16, color="#2c3e50", family="Arial Black")),
                range=[x_min - x_pad, x_max + x_pad],
                gridcolor="#bdc3c7",
                showbackground=True,
                backgroundcolor="#ecf0f1",
            ),
            yaxis=dict(
                title=dict(text="<b>Height (Inch)</b>", font=dict(size=16, color="#2c3e50", family="Arial Black")),
                range=[y_min - y_pad, y_max + y_pad],
                gridcolor="#bdc3c7",
                showbackground=True,
                backgroundcolor="#ecf0f1",
            ),
            zaxis=dict(
                title=dict(text="<b>Depth (Inch)</b>", font=dict(size=16, color="#2c3e50", family="Arial Black")),
                range=[z_min - z_pad, z_max + z_pad],
                gridcolor="#bdc3c7",
                showbackground=True,
                backgroundcolor="#ecf0f1",
            ),
            camera=dict(eye=dict(x=1.8, y=1.8, z=1.4), center=dict(x=0, y=0, z=0), up=dict(x=0, y=1, z=0)),
            aspectmode="cube",
            dragmode="orbit",
        ),
        showlegend=True,
        legend=dict(x=0.02, y=0.98, bgcolor="rgba(255,255,255,0.95)", bordercolor="#2c3e50", borderwidth=2),
        margin=dict(l=0, r=0, t=40, b=0),
        paper_bgcolor="#ffffff",
        plot_bgcolor="#ffffff",
        title=dict(
            text=f"<b>Interactive 3D Product Dimensions</b><br><sub>Algorithm(s): {', '.join(selected_algorithms)}</sub>",
            x=0.5,
            xanchor="center",
        ),
    )
    return fig


def state_set(payload):
    token = uuid.uuid4().hex
    CHART_STATE[token] = payload
    if len(CHART_STATE) > 30:
        oldest = list(CHART_STATE.keys())[0]
        CHART_STATE.pop(oldest, None)
    return token


CHART_STATE = {}
