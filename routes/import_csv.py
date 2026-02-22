from flask import Blueprint, render_template, request, jsonify
import csv
import io
from models.base.base import engine, SessionLocal
from sqlalchemy import inspect, text

import_bp = Blueprint("import_bp", __name__)

@import_bp.route("/import")
def import_page():
    return render_template("import.html", active_page="import")

@import_bp.route("/api/import/parse-csv", methods=["POST"])
def parse_csv():
    if 'file' not in request.files:
        return jsonify({"error": "No file uploaded"}), 400

    file = request.files['file']
    if not file.filename.endswith('.csv'):
        return jsonify({"error": "File must be a CSV"}), 400

    try:
        content = file.read().decode('utf-8')
        reader = csv.DictReader(io.StringIO(content))
        headers = reader.fieldnames
        rows = list(reader)

        return jsonify({
            "headers": headers,
            "rowCount": len(rows)
        })
    except Exception as e:
        return jsonify({"error": f"Invalid CSV: {str(e)}"}), 400

@import_bp.route("/api/import/tables", methods=["GET"])
def get_tables():
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    return jsonify({"tables": tables})

@import_bp.route("/api/import/columns/<table_name>", methods=["GET"])
def get_columns(table_name):
    inspector = inspect(engine)
    columns = [col['name'] for col in inspector.get_columns(table_name)]
    required_columns = [col['name'] for col in inspector.get_columns(table_name)
                       if not col['nullable'] and col['default'] is None and col.get('autoincrement') != True]
    return jsonify({"columns": columns, "requiredColumns": required_columns})

@import_bp.route("/api/import/save", methods=["POST"])
def save_import():
    data = request.json

    if not data.get('csvData'):
        return jsonify({"error": "CSV file is required"}), 400
    if not data.get('table'):
        return jsonify({"error": "Table selection is required"}), 400
    if not data.get('mappings') or len(data['mappings']) == 0:
        return jsonify({"error": "At least one mapping must be selected"}), 400
    if not data.get('identityColumns') or len(data['identityColumns']) == 0:
        return jsonify({"error": "At least one identity column must be selected"}), 400
    if not data.get('importMode'):
        return jsonify({"error": "Import mode must be selected"}), 400

    inspector = inspect(engine)
    required_columns = [col['name'] for col in inspector.get_columns(data['table'])
                       if not col['nullable'] and col['default'] is None and col.get('autoincrement') != True]
    mapped_columns = [m['tableColumn'] for m in data['mappings']]
    missing_required = [col for col in required_columns if col not in mapped_columns]

    if missing_required:
        return jsonify({"error": f"Missing table's required columns in csv file: {', '.join(missing_required)}"}), 400

    try:
        result = process_import(
            data['csvData'],
            data['table'],
            data['mappings'],
            data['identityColumns'],
            data['importMode']
        )
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

def process_import(csv_data, table_name, mappings, identity_columns, import_mode):
    session = SessionLocal()
    inserted = 0
    updated = 0
    errors = 0
    error_messages = []
    total = 0

    try:
        reader = csv.DictReader(io.StringIO(csv_data))
        rows = list(reader)
        total = len(rows)

        mapping_dict = {m['csvColumn']: m['tableColumn'] for m in mappings}
        csv_to_table = {csv_col: table_col for csv_col, table_col in mapping_dict.items()}

        for idx, row in enumerate(rows, 1):
            try:
                where_parts = []
                where_values = {}

                for identity_csv_col in identity_columns:
                    table_col = csv_to_table[identity_csv_col]
                    csv_value = row.get(identity_csv_col, '').strip()
                    where_parts.append(f"{table_col} = :id_{table_col}")
                    where_values[f"id_{table_col}"] = csv_value if csv_value else None

                where_clause = " AND ".join(where_parts)
                check_query = text(f"SELECT COUNT(*) as cnt FROM {table_name} WHERE {where_clause}")
                exists = session.execute(check_query, where_values).scalar() > 0

                if import_mode == 'insert' and not exists:
                    insert_values = {}
                    for csv_col, table_col in csv_to_table.items():
                        csv_value = row.get(csv_col, '').strip()
                        insert_values[table_col] = csv_value if csv_value else None

                    cols = list(insert_values.keys())
                    placeholders = ", ".join([f":{col}" for col in cols])
                    insert_query = text(f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES ({placeholders})")
                    session.execute(insert_query, insert_values)
                    inserted += 1

                elif import_mode == 'update' and exists:
                    update_values = {}
                    set_parts = []

                    for csv_col, table_col in csv_to_table.items():
                        if csv_col not in identity_columns:
                            csv_value = row.get(csv_col, '').strip()
                            update_values[table_col] = csv_value if csv_value else None
                            set_parts.append(f"{table_col} = :{table_col}")

                    for identity_csv_col in identity_columns:
                        table_col = csv_to_table[identity_csv_col]
                        csv_value = row.get(identity_csv_col, '').strip()
                        update_values[f"id_{table_col}"] = csv_value if csv_value else None

                    if set_parts:
                        set_clause = ", ".join(set_parts)
                        update_query = text(f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}")
                        session.execute(update_query, update_values)
                        updated += 1

                elif import_mode == 'insert_update':
                    if exists:
                        update_values = {}
                        set_parts = []

                        for csv_col, table_col in csv_to_table.items():
                            if csv_col not in identity_columns:
                                csv_value = row.get(csv_col, '').strip()
                                update_values[table_col] = csv_value if csv_value else None
                                set_parts.append(f"{table_col} = :{table_col}")

                        for identity_csv_col in identity_columns:
                            table_col = csv_to_table[identity_csv_col]
                            csv_value = row.get(identity_csv_col, '').strip()
                            update_values[f"id_{table_col}"] = csv_value if csv_value else None

                        if set_parts:
                            set_clause = ", ".join(set_parts)
                            update_query = text(f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}")
                            session.execute(update_query, update_values)
                            updated += 1
                    else:
                        insert_values = {}
                        for csv_col, table_col in csv_to_table.items():
                            csv_value = row.get(csv_col, '').strip()
                            insert_values[table_col] = csv_value if csv_value else None

                        cols = list(insert_values.keys())
                        placeholders = ", ".join([f":{col}" for col in cols])
                        insert_query = text(f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES ({placeholders})")
                        session.execute(insert_query, insert_values)
                        inserted += 1

            except Exception as e:
                errors += 1
                error_msg = f"Row {idx}: {str(e)}"
                error_messages.append(error_msg)
                print(error_msg)

        session.commit()
        return {
            "success": True,
            "report": {
                "total": total,
                "inserted": inserted,
                "updated": updated,
                "errors": errors,
                "errorMessages": error_messages[:5]
            }
        }
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()
