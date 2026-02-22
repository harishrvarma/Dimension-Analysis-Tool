# Version: 2.0
import pandas as pd
from dash import Dash, dcc, html, Input, Output, State, dash_table, ALL, MATCH, ctx
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler
from Helpers.db_loader import get_product_groups_for_dropdown, get_brands_by_group, get_categories_by_brands
from Helpers.product_helper import bulk_update_skip_status
from Models.database import SessionLocal

def calculate_iqr_bounds(data_subset, multipliers={'H': 1.5, 'W': 1.5, 'D': 1.5}):
    iqr_stats = {}
    for dim in ['H', 'W', 'D']:
        Q1, Q3 = data_subset[dim].quantile(0.25), data_subset[dim].quantile(0.75)
        IQR = Q3 - Q1
        iqr_stats[dim] = {'lower': Q1 - multipliers[dim] * IQR, 'upper': Q3 + multipliers[dim] * IQR}
    return iqr_stats

def run_iqr(df, multipliers):
    df = df.copy()
    stats = calculate_iqr_bounds(df, multipliers)
    for dim in ['H', 'W', 'D']:
        col_map = {'H': 'height', 'W': 'width', 'D': 'depth'}
        in_range = (df[dim] >= stats[dim]['lower']) & (df[dim] <= stats[dim]['upper'])
        df[f'iqr_{dim.lower()}_status'] = in_range.map({True: 'Normal', False: 'Outlier'})
        df[f'iqr_{col_map[dim]}_status'] = in_range.astype(int)
    df['iqr_status'] = ((df['iqr_height_status'] == 1) & 
                    (df['iqr_width_status'] == 1) & 
                    (df['iqr_depth_status'] == 1)).astype(int)
    return df

def run_dbscan(df):
    df = df.copy()
    X = StandardScaler().fit_transform(df[['H', 'W', 'D']].values)
    clusters = DBSCAN(eps=1.0, min_samples=4).fit_predict(X)
    is_normal = clusters != -1
    status_text = pd.Series(is_normal, index=df.index).map({True: 'Normal', False: 'Outlier'})
    df['dbs_h_status'] = status_text
    df['dbs_w_status'] = status_text
    df['dbs_d_status'] = status_text
    df['dbs_height_status'] = is_normal.astype(int)
    df['dbs_width_status'] = is_normal.astype(int)
    df['dbs_depth_status'] = is_normal.astype(int)
    df['dbscan_status'] = is_normal.astype(int)
    df['dbs_status'] = is_normal.astype(int)
    return df

app = Dash(__name__, suppress_callback_exceptions=True)
app.title = "Product Grid Manager v2.0"

app.layout = html.Div([
    html.H1("📊 Product Grid Manager v2.0", style={"color": "#2c3e50", "marginBottom": "20px"}),
    
    html.Div([
        html.Div([
            html.Label("📁 Product Group:", style={"fontWeight": "600", "marginBottom": "5px", "display": "block"}),
            dcc.Dropdown(id="group-dropdown", options=get_product_groups_for_dropdown(), placeholder="Select Group")
        ], style={"flex": "1", "marginRight": "10px"}),
        
        html.Div([
            html.Label("🔬 Algorithm:", style={"fontWeight": "600", "marginBottom": "5px", "display": "block"}),
            dcc.Dropdown(id="algorithm-dropdown", options=[
                {"label": "IQR", "value": "IQR"},
                {"label": "DBSCAN", "value": "DBSCAN"}
            ], multi=True, placeholder="Select Algorithm(s)")
        ], style={"flex": "1", "marginRight": "10px"}),
        
        html.Div([
            html.Label("✅ Final Status:", style={"fontWeight": "600", "marginBottom": "5px", "display": "block"}),
            dcc.Dropdown(id="final-status-dropdown", options=[
                {"label": "Normal", "value": "Normal"},
                {"label": "Outlier", "value": "Outlier"}
            ], multi=True, placeholder="Select Status(es)")
        ], style={"flex": "1"})
    ], style={"display": "flex", "marginBottom": "20px", "padding": "15px", "backgroundColor": "#f8f9fa", "borderRadius": "8px"}),
    
    html.Div([
        html.Div([
            html.Label("🏷️ Brand:", style={"fontWeight": "600", "marginBottom": "5px", "display": "block"}),
            dcc.Dropdown(id="brand-dropdown", multi=True, placeholder="Select Brand(s)")
        ], style={"flex": "1", "marginRight": "10px"}),
        
        html.Div([
            html.Label("📂 Category:", style={"fontWeight": "600", "marginBottom": "5px", "display": "block"}),
            dcc.Dropdown(id="category-dropdown", multi=True, placeholder="Select Category(s)")
        ], style={"flex": "1", "marginRight": "10px"}),
        
        html.Div([
            dcc.Checklist(
                id="filter-skip-checkbox",
                options=[{"label": " Hide Skipped", "value": "hide"}],
                value=[],
                style={"marginTop": "30px"}
            )
        ], style={"flex": "0.5"})
    ], style={"display": "flex", "marginBottom": "20px", "padding": "15px", "backgroundColor": "#f8f9fa", "borderRadius": "8px"}),
    
    html.Div([
        html.Button("💾 Save Changes", id="save-btn", n_clicks=0, style={
            "padding": "10px 20px", "backgroundColor": "#28a745", "color": "white",
            "border": "none", "borderRadius": "4px", "cursor": "pointer", "fontWeight": "600"
        }),
        html.Div(id="save-status", style={"marginLeft": "15px", "fontSize": "14px", "fontWeight": "600"})
    ], style={"display": "flex", "alignItems": "center", "marginBottom": "15px"}),
    
    dcc.Loading(id="loading-grid", children=[html.Div(id="grid-container")], color="#3498db")
], style={"padding": "20px 30px", "fontFamily": "Arial", "backgroundColor": "#fff", "minHeight": "100vh"})


@app.callback(
    Output("brand-dropdown", "options"),
    Input("group-dropdown", "value")
)
def update_brands(group_id):
    if not group_id:
        return []
    try:
        brands = get_brands_by_group(group_id)
        return [{"label": f"{b[0]} ({b[1]})", "value": b[0]} for b in brands]
    except Exception as e:
        print(f"Error loading brands: {e}")
        return []


@app.callback(
    Output("category-dropdown", "options"),
    [Input("brand-dropdown", "value"), Input("group-dropdown", "value")]
)
def update_categories(brands, group_id):
    if not group_id or not brands:
        return []
    try:
        categories = get_categories_by_brands(group_id, brands)
        return [{"label": f"{c[0]} ({c[1]})", "value": c[0]} for c in categories]
    except Exception as e:
        print(f"Error loading categories: {e}")
        return []


@app.callback(
    Output("grid-container", "children"),
    [Input("group-dropdown", "value"), Input("algorithm-dropdown", "value"),
     Input("brand-dropdown", "value"), Input("category-dropdown", "value"),
     Input("filter-skip-checkbox", "value"), Input("final-status-dropdown", "value")]
)
def update_grid(group_id, algorithms, brands, categories, filter_skip, final_statuses):
    if not group_id:
        return html.Div("⚠️ Please select Product Group", 
                       style={"padding": "40px", "textAlign": "center", "color": "#e74c3c"})
    
    if not brands:
        return html.Div("⚠️ Please select at least one Brand", 
                       style={"padding": "40px", "textAlign": "center", "color": "#e74c3c"})
    
    if len(brands) > 1 and not categories:
        return html.Div("⚠️ Multiple brands selected. Please select a category.", 
                       style={"padding": "40px", "textAlign": "center", "color": "#e74c3c"})

    from Helpers.db_loader import load_filtered_data
    df = load_filtered_data(group_id, brands, categories)
    
    if df.empty:
        return html.Div("No products match filters", style={"padding": "20px", "color": "#e74c3c"})
    
    # Run algorithms if selected
    if algorithms:
        if 'IQR' in algorithms:
            df = run_iqr(df, {'H': 1.5, 'W': 1.5, 'D': 1.5})
        if 'DBSCAN' in algorithms:
            df = run_dbscan(df)
        
        # Calculate Final Status
        if 'IQR' in algorithms and 'DBSCAN' in algorithms:
            df['final_status'] = ((df['iqr_status'] == 1) | (df['dbs_status'] == 1)).astype(int)
        elif 'IQR' in algorithms:
            df['final_status'] = df['iqr_status']
        elif 'DBSCAN' in algorithms:
            df['final_status'] = df['dbs_status']
    else:
        df['final_status'] = 1
    
    # Filter by Final Status
    if final_statuses:
        status_map = {'Normal': 1, 'Outlier': 0}
        status_values = [status_map[s] for s in final_statuses]
        df = df[df['final_status'].isin(status_values)]
    
    # Filter skipped products
    if filter_skip and 'hide' in filter_skip:
        df = df[df['skip_status'] != 'Yes']
    
    # Prepare columns
    columns = [
        {"name": "SKU", "id": "SKU"},
        {"name": "QB Code", "id": "qb_code", "presentation": "markdown"},
        {"name": "Brand", "id": "Brand"},
        {"name": "Category", "id": "Category"},
        {"name": "Type", "id": "Type"},
        {"name": "Name", "id": "Name"},
        {"name": "H", "id": "H", "type": "numeric", "format": {"specifier": ".2f"}},
        {"name": "W", "id": "W", "type": "numeric", "format": {"specifier": ".2f"}},
        {"name": "D", "id": "D", "type": "numeric", "format": {"specifier": ".2f"}}
    ]
    
    if algorithms:
        if 'IQR' in algorithms:
            columns.extend([
                {"name": "IQR H Status", "id": "iqr_h_status"},
                {"name": "IQR W Status", "id": "iqr_w_status"},
                {"name": "IQR D Status", "id": "iqr_d_status"},
                {"name": "IQR Status", "id": "iqr_status"}
            ])
        if 'DBSCAN' in algorithms:
            columns.extend([
                {"name": "DBS H Status", "id": "dbs_h_status"},
                {"name": "DBS W Status", "id": "dbs_w_status"},
                {"name": "DBS D Status", "id": "dbs_d_status"},
                {"name": "DBS Status", "id": "dbs_status"}
            ])
    
    columns.append({"name": "Skip Status", "id": "skip_status"})
    
    if algorithms:
        columns.append({"name": "Final Status", "id": "final_status"})
    
    df['qb_code_display'] = df['qb_code']
    df['qb_code'] = df.apply(
        lambda r: f"[{r['qb_code']}]({r['productUrl']})" if r.get('productUrl') and r.get('qb_code') else (r.get('qb_code') or ''),
        axis=1
    )
    
    df['skip_status_bool'] = df['skip_status'].apply(lambda x: x == 'Yes')
    
    if algorithms:
        if 'IQR' in algorithms:
            df['iqr_status_int'] = df['iqr_status']
            df['iqr_status'] = df['iqr_status'].map({1: 'Normal', 0: 'Outlier'})
        if 'DBSCAN' in algorithms:
            df['dbs_status_int'] = df['dbs_status']
            df['dbs_status'] = df['dbs_status'].map({1: 'Normal', 0: 'Outlier'})
        df['final_status_int'] = df['final_status']
        df['final_status'] = df['final_status'].map({1: 'Normal', 0: 'Outlier'})
    
    data = df.to_dict('records')
    
    # Build custom grid with checkboxes, sorting, and filtering
    header_style = {'backgroundColor': '#3498db', 'color': 'white', 'fontWeight': 'bold', 'padding': '12px', 'border': '1px solid #dee2e6', 'position': 'sticky', 'top': '0', 'zIndex': '10'}
    cell_style = {'padding': '12px', 'border': '1px solid #dee2e6', 'fontSize': '13px'}
    
    # Create header row with sort buttons and filter inputs
    header_cells = []
    for col in columns:
        if col['id'] == 'skip_status':
            continue
        col_id = col['id']
        header_cells.append(html.Th([
            html.Div([
                col['name'],
                html.Button('⇅', id={'type': 'sort-btn', 'column': col_id}, 
                           style={'marginLeft': '5px', 'border': 'none', 'background': 'transparent', 
                                  'color': 'white', 'cursor': 'pointer', 'fontSize': '14px'})
            ], style={'display': 'flex', 'alignItems': 'center', 'justifyContent': 'space-between'}),
            dcc.Input(id={'type': 'filter-input', 'column': col_id}, type='text', 
                     placeholder='Filter...', debounce=True,
                     style={'width': '100%', 'marginTop': '5px', 'padding': '4px', 'fontSize': '12px', 'color': "#000000"})
        ], style=header_style))
    
    header_cells.append(html.Th('Skip', style=header_style))
    
    # Create data rows
    rows = []
    for idx, row in enumerate(data):
        cells = []
        for col in columns:
            if col['id'] == 'skip_status':
                continue
            col_id = col['id']
            if col_id == 'qb_code' and row.get('productUrl'):
                cells.append(html.Td(html.A(row.get('qb_code_display', ''), href=row.get('productUrl', ''), target='_blank'), 
                                    style=cell_style, **{'data-column': col_id}))
            else:
                value = row.get(col_id, '')
                if col.get('type') == 'numeric' and isinstance(value, (int, float)):
                    value = f"{value:.2f}"
                cells.append(html.Td(str(value), style=cell_style, **{'data-column': col_id}))
        
        # Add checkbox for skip_status
        cells.append(html.Td(
            dcc.Checklist(
                id={'type': 'skip-checkbox', 'index': row['product_id']},
                options=[{'label': '', 'value': 'skip'}],
                value=['skip'] if row.get('skip_status_bool', False) else [],
                style={'margin': '0'}
            ),
            style={**cell_style, 'textAlign': 'center'}
        ))
        
        row_style = {'backgroundColor': '#f8f9fa'} if idx % 2 else {}
        rows.append(html.Tr(cells, style=row_style, id={'type': 'data-row', 'index': idx}))
    
    return html.Div([
        html.Div(f"📊 Showing {len(df)} products", style={
            "padding": "10px 15px", "backgroundColor": "#e8f4f8", "borderRadius": "6px",
            "marginBottom": "10px", "fontWeight": "600", "color": "#2c3e50"
        }),
        html.Div(
            html.Table([
                html.Thead(html.Tr(header_cells)),
                html.Tbody(rows, id='table-body')
            ], style={'width': '100%', 'borderCollapse': 'collapse', 'border': '1px solid #dee2e6'}),
            style={'overflowX': 'auto', 'maxHeight': '70vh', 'overflowY': 'auto'}
        ),
        dcc.Store(id='grid-data', data=data),
        dcc.Store(id='sort-state', data={'column': None, 'ascending': True})
    ])


@app.callback(
    [Output('table-body', 'children'), Output('sort-state', 'data')],
    [Input({'type': 'sort-btn', 'column': ALL}, 'n_clicks'),
     Input({'type': 'filter-input', 'column': ALL}, 'value')],
    [State({'type': 'sort-btn', 'column': ALL}, 'id'),
     State({'type': 'filter-input', 'column': ALL}, 'id'),
     State('grid-data', 'data'),
     State('sort-state', 'data'),
     State('algorithm-dropdown', 'value')],
    prevent_initial_call=True
)
def sort_and_filter_table(sort_clicks, filter_values, sort_ids, filter_ids, grid_data, sort_state, algorithms):
    if not grid_data:
        return [], sort_state
    
    df = pd.DataFrame(grid_data)
    
    # Apply filters
    for filter_id, filter_val in zip(filter_ids, filter_values):
        if filter_val:
            col = filter_id['column']
            if col in df.columns:
                df = df[df[col].astype(str).str.contains(filter_val, case=False, na=False)]
    
    # Apply sorting
    triggered = ctx.triggered_id
    if triggered and triggered.get('type') == 'sort-btn':
        col = triggered['column']
        if sort_state['column'] == col:
            sort_state['ascending'] = not sort_state['ascending']
        else:
            sort_state['column'] = col
            sort_state['ascending'] = True
    
    if sort_state['column'] and sort_state['column'] in df.columns:
        df = df.sort_values(by=sort_state['column'], ascending=sort_state['ascending'])
    
    # Rebuild rows
    cell_style = {'padding': '12px', 'border': '1px solid #dee2e6', 'fontSize': '13px'}
    rows = []
    data = df.to_dict('records')
    
    for idx, row in enumerate(data):
        cells = []
        for key in ['SKU', 'qb_code', 'Brand', 'Category', 'Type', 'Name', 'H', 'W', 'D']:
            if key == 'qb_code' and row.get('productUrl'):
                cells.append(html.Td(html.A(row.get('qb_code_display', ''), href=row.get('productUrl', ''), target='_blank'), style=cell_style))
            elif key in ['H', 'W', 'D']:
                val = row.get(key, '')
                cells.append(html.Td(f"{val:.2f}" if isinstance(val, (int, float)) else str(val), style=cell_style))
            else:
                cells.append(html.Td(str(row.get(key, '')), style=cell_style))
        
        if algorithms:
            if 'IQR' in algorithms:
                for key in ['iqr_h_status', 'iqr_w_status', 'iqr_d_status', 'iqr_status']:
                    cells.append(html.Td(str(row.get(key, '')), style=cell_style))
            if 'DBSCAN' in algorithms:
                for key in ['dbs_h_status', 'dbs_w_status', 'dbs_d_status', 'dbs_status']:
                    cells.append(html.Td(str(row.get(key, '')), style=cell_style))
        
        cells.append(html.Td(
            dcc.Checklist(
                id={'type': 'skip-checkbox', 'index': row['product_id']},
                options=[{'label': '', 'value': 'skip'}],
                value=['skip'] if row.get('skip_status_bool', False) else [],
                style={'margin': '0'}
            ),
            style={**cell_style, 'textAlign': 'center'}
        ))
        
        if algorithms:
            cells.append(html.Td(str(row.get('final_status', '')), style=cell_style))
        
        row_style = {'backgroundColor': '#f8f9fa'} if idx % 2 else {}
        rows.append(html.Tr(cells, style=row_style))
    
    return rows, sort_state


@app.callback(
    [Output("save-status", "children"), Output("save-status", "style")],
    Input("save-btn", "n_clicks"),
    [State({'type': 'skip-checkbox', 'index': ALL}, 'value'),
     State({'type': 'skip-checkbox', 'index': ALL}, 'id'),
     State('grid-data', 'data'),
     State("algorithm-dropdown", "value")],
    prevent_initial_call=True
)
def save_changes(n_clicks, checkbox_values, checkbox_ids, grid_data, algorithms):
    if not grid_data:
        return "No data to save", {"marginLeft": "15px", "fontSize": "14px", "fontWeight": "600", "color": "#e74c3c"}
    
    from Models.models import Product
    
    db = SessionLocal()
    try:
        # Create map of product_id to skip status from checkboxes
        skip_map = {}
        for checkbox_id, checkbox_val in zip(checkbox_ids, checkbox_values):
            product_id = checkbox_id['index']
            skip_map[product_id] = 'Yes' if checkbox_val and 'skip' in checkbox_val else 'No'
        
        # Get all products
        product_ids = [row['product_id'] for row in grid_data]
        products = db.query(Product).filter(Product.product_id.in_(product_ids)).all()
        product_map = {p.product_id: p for p in products}
        
        # Update products
        for row in grid_data:
            product_id = row['product_id']
            product = product_map.get(product_id)
            if not product:
                continue
            
            # Update skip status from checkbox
            product.skip_status = skip_map.get(product_id, 'No')
            
            # Update algorithm statuses if present
            if algorithms:
                if 'IQR' in algorithms:
                    product.iqr_status = int(row.get('iqr_status_int', 0) or 0)
                    product.iqr_height_status = int(row.get('iqr_height_status', 0) or 0)
                    product.iqr_width_status = int(row.get('iqr_width_status', 0) or 0)
                    product.iqr_depth_status = int(row.get('iqr_depth_status', 0) or 0)
                
                if 'DBSCAN' in algorithms:
                    product.dbs_status = int(row.get('dbs_status_int', 0) or 0)
                    product.dbs_height_status = int(row.get('dbs_height_status', 0) or 0)
                    product.dbs_width_status = int(row.get('dbs_width_status', 0) or 0)
                    product.dbs_depth_status = int(row.get('dbs_depth_status', 0) or 0)
                
                product.final_status = int(row.get('final_status_int', 1) or 1)
        
        db.commit()
        return f"✅ Saved {len(grid_data)} products", {"marginLeft": "15px", "fontSize": "14px", "fontWeight": "600", "color": "#28a745"}
    except Exception as e:
        db.rollback()
        return f"❌ Error: {str(e)}", {"marginLeft": "15px", "fontSize": "14px", "fontWeight": "600", "color": "#e74c3c"}
    finally:
        db.close()


if __name__ == "__main__":
    app.run(debug=True, host="192.168.0.80", port=8775)
