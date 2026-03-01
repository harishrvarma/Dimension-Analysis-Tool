from flask import Blueprint, render_template, request, jsonify
import csv
import io
from models.base.base import engine, SessionLocal
from sqlalchemy import inspect, text

import_bp = Blueprint("import_bp", __name__)

@import_bp.route("/import")
def import_page():
    return render_template("database/import.html", active_page="import")

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
    db_name = engine.url.database
    return jsonify({"tables": tables, "database": db_name})

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
    if not data.get('importMode'):
        return jsonify({"error": "Import mode must be selected"}), 400

    # Identity columns only required for modes that need them
    if data['importMode'] != 'insert_direct' and (not data.get('identityColumns') or len(data['identityColumns']) == 0):
        return jsonify({"error": "At least one identity column must be selected"}), 400

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
    BATCH_SIZE = 500

    try:
        reader = csv.DictReader(io.StringIO(csv_data))
        rows = list(reader)
        total = len(rows)
        csv_to_table = {m['csvColumn']: m['tableColumn'] for m in mappings}

        # Direct insert mode - batch processing without identity check
        if import_mode == 'insert_direct':
            for batch_start in range(0, total, BATCH_SIZE):
                batch_rows = rows[batch_start:batch_start + BATCH_SIZE]
                batch_values = []

                for idx, row in enumerate(batch_rows, batch_start + 1):
                    try:
                        row_values = {csv_to_table[csv_col]: row.get(csv_col, '').strip() or None
                                     for csv_col in csv_to_table}
                        batch_values.append(row_values)
                    except Exception as e:
                        errors += 1
                        error_messages.append(f"Row {idx}: {str(e)}")

                if batch_values:
                    try:
                        cols = list(batch_values[0].keys())
                        placeholders = ", ".join([f":{col}" for col in cols])
                        insert_query = text(f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES ({placeholders})")
                        session.execute(insert_query, batch_values)
                        inserted += len(batch_values)
                        session.commit()
                    except Exception as e:
                        session.rollback()
                        errors += len(batch_values)
                        error_messages.append(f"Batch {batch_start}-{batch_start + len(batch_values)}: {str(e)}")

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

        # Modes with identity columns - optimized batch processing
        identity_table_cols = [csv_to_table[ic] for ic in identity_columns]

        for batch_start in range(0, total, BATCH_SIZE):
            batch_rows = rows[batch_start:batch_start + BATCH_SIZE]

            # Build identity values for all rows in batch
            batch_identity_values = []
            for row in batch_rows:
                if len(identity_columns) == 1:
                    val = row.get(identity_columns[0], '').strip()
                    # Try to convert to number if possible for proper comparison
                    try:
                        val = int(val) if val else None
                    except (ValueError, TypeError):
                        val = val if val else None
                    batch_identity_values.append(val)
                else:
                    vals = []
                    for ic in identity_columns:
                        val = row.get(ic, '').strip()
                        try:
                            val = int(val) if val else None
                        except (ValueError, TypeError):
                            val = val if val else None
                        vals.append(val)
                    batch_identity_values.append(tuple(vals))

            # Build single bulk query to check all records at once
            existing_records = set()
            if len(identity_columns) == 1:
                identity_col = identity_table_cols[0]
                unique_values = [v for v in set(batch_identity_values) if v is not None]
                if unique_values:
                    placeholders = ','.join([f":val_{i}" for i in range(len(unique_values))])
                    check_query = text(f"SELECT {identity_col} FROM {table_name} WHERE {identity_col} IN ({placeholders})")
                    params = {f"val_{i}": val for i, val in enumerate(unique_values)}
                    result = session.execute(check_query, params)
                    existing_vals = set()
                    for row in result:
                        val = row[0]
                        # Ensure type consistency
                        try:
                            val = int(val) if val is not None else None
                        except (ValueError, TypeError):
                            pass
                        existing_vals.add(val)
                    existing_records = {i for i, val in enumerate(batch_identity_values) if val in existing_vals}
            else:
                # Multi-column identity: build OR conditions for batch check
                or_conditions = []
                params = {}
                for i, identity_vals in enumerate(batch_identity_values):
                    and_parts = []
                    for j, (ic, val) in enumerate(zip(identity_table_cols, identity_vals)):
                        param_name = f"v{i}_{j}"
                        and_parts.append(f"{ic} = :{param_name}")
                        params[param_name] = val
                    or_conditions.append(f"({' AND '.join(and_parts)})")

                if or_conditions:
                    check_query = text(f"SELECT {', '.join(identity_table_cols)} FROM {table_name} WHERE {' OR '.join(or_conditions)}")
                    result = session.execute(check_query, params)
                    existing_tuples = set()
                    for row in result:
                        # Ensure type consistency for tuple comparison
                        vals = []
                        for val in row:
                            try:
                                vals.append(int(val) if val is not None else None)
                            except (ValueError, TypeError):
                                vals.append(val)
                        existing_tuples.add(tuple(vals))
                    existing_records = {i for i, vals in enumerate(batch_identity_values) if vals in existing_tuples}

            # Process batch
            insert_batch = []
            update_batch = []

            for idx, row in enumerate(batch_rows):
                try:
                    row_values = {csv_to_table[csv_col]: row.get(csv_col, '').strip() or None
                                 for csv_col in csv_to_table}
                    exists = idx in existing_records

                    if import_mode == 'insert_with_identity' and not exists:
                        insert_batch.append(row_values)
                    elif import_mode == 'update' and exists:
                        update_data = {k: v for k, v in row_values.items()
                                      if k not in identity_table_cols}
                        # Build identity_data correctly for single vs multiple columns
                        if len(identity_columns) == 1:
                            identity_data = {identity_table_cols[0]: batch_identity_values[idx]}
                        else:
                            identity_data = {identity_table_cols[i]: batch_identity_values[idx][i]
                                            for i in range(len(identity_columns))}
                        update_batch.append((update_data, identity_data))
                    elif import_mode == 'insert_update':
                        if exists:
                            update_data = {k: v for k, v in row_values.items()
                                          if k not in identity_table_cols}
                            # Build identity_data correctly for single vs multiple columns
                            if len(identity_columns) == 1:
                                identity_data = {identity_table_cols[0]: batch_identity_values[idx]}
                            else:
                                identity_data = {identity_table_cols[i]: batch_identity_values[idx][i]
                                                for i in range(len(identity_columns))}
                            update_batch.append((update_data, identity_data))
                        else:
                            insert_batch.append(row_values)
                except Exception as e:
                    errors += 1
                    error_messages.append(f"Row {batch_start + idx + 1}: {str(e)}")

            # Execute batch inserts
            if insert_batch:
                try:
                    cols = list(insert_batch[0].keys())
                    placeholders = ", ".join([f":{col}" for col in cols])
                    insert_query = text(f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES ({placeholders})")
                    session.execute(insert_query, insert_batch)
                    inserted += len(insert_batch)
                except Exception as e:
                    errors += len(insert_batch)
                    error_messages.append(f"Insert batch error: {str(e)}")

            # Execute batch updates - use CASE statements for bulk update
            if update_batch:
                try:
                    if update_batch[0][0]:  # Check if there are columns to update
                        update_cols = list(update_batch[0][0].keys())
                        identity_cols = list(update_batch[0][1].keys())

                        # Build CASE statements for each update column
                        case_statements = []
                        for col in update_cols:
                            cases = []
                            for i, (update_data, identity_data) in enumerate(update_batch):
                                when_parts = [f"{id_col} = :id_{i}_{id_col}" for id_col in identity_cols]
                                cases.append(f"WHEN {' AND '.join(when_parts)} THEN :upd_{i}_{col}")
                            case_statements.append(f"{col} = CASE {' '.join(cases)} ELSE {col} END")

                        # Build WHERE clause with all identity combinations
                        where_conditions = []
                        params = {}
                        for i, (update_data, identity_data) in enumerate(update_batch):
                            and_parts = []
                            for id_col, id_val in identity_data.items():
                                and_parts.append(f"{id_col} = :id_{i}_{id_col}")
                                params[f"id_{i}_{id_col}"] = id_val
                            where_conditions.append(f"({' AND '.join(and_parts)})")

                            for col in update_cols:
                                params[f"upd_{i}_{col}"] = update_data[col]

                        update_query = text(f"UPDATE {table_name} SET {', '.join(case_statements)} WHERE {' OR '.join(where_conditions)}")
                        session.execute(update_query, params)
                        updated += len(update_batch)
                except Exception as e:
                    errors += len(update_batch)
                    error_messages.append(f"Update batch error: {str(e)}")

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
