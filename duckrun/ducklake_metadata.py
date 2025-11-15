# File: ducklake_delta_exporter.py
import json
import time
import duckdb
import os
import tempfile
import shutil

def map_type_ducklake_to_spark(t):
    """Maps DuckDB data types to their Spark SQL equivalents for the Delta schema."""
    t_lower = t.lower()
    if 'int' in t_lower:
        return 'long' if '64' in t_lower else 'integer'
    elif 'float' in t_lower:
        return 'double'
    elif 'double' in t_lower:
        return 'double'
    elif 'decimal' in t_lower:
        # Preserve the original decimal precision and scale
        return t_lower
    elif 'bool' in t_lower:
        return 'boolean'
    elif 'timestamp' in t_lower:
        return 'timestamp'
    elif 'date' in t_lower:
        return 'date'
    return 'string'

def convert_stat_value_to_json(value_str, column_type):
    """
    Convert DuckLake stat string value to proper JSON type for Delta Lake.
    
    Args:
        value_str: String representation of the value from DuckLake
        column_type: DuckDB column type
    
    Returns:
        Properly typed value for JSON serialization
    """
    if value_str is None:
        return None
    
    column_type = column_type.lower()
    
    try:
        # Timestamp: Convert to ISO 8601 with .000Z suffix (UTC format)
        if 'timestamp' in column_type:
            # Parse and format to ISO 8601
            # Handle various input formats from DuckDB:
            # - "2025-06-22 23:55:00" -> "2025-06-22T23:55:00.000Z"
            # - "2025-06-22T23:55:00+00" -> "2025-06-22T23:55:00.000Z"
            # - "2025-06-22T23:55:00.123+00:00" -> "2025-06-22T23:55:00.123Z"
            
            # Replace space with T if needed
            if 'T' not in value_str:
                value_str = value_str.replace(' ', 'T')
            
            # Remove timezone offset formats to normalize to UTC
            # Strip patterns like: +00, +00:00, -05:00, etc.
            import re
            value_str = re.sub(r'[+-]\d{2}(?::\d{2})?$', '', value_str)
            
            # Ensure milliseconds are present
            if '.' not in value_str:
                value_str += '.000'
            
            # Add Z suffix for UTC if not present
            if not value_str.endswith('Z'):
                value_str += 'Z'
            
            return value_str
        
        # Date: Keep as YYYY-MM-DD string
        elif 'date' in column_type:
            return value_str
        
        # Boolean: Convert to JSON boolean
        elif 'bool' in column_type:
            return value_str.lower() in ('true', 't', '1', 'yes')
        
        # Numeric types: Convert to number (not string)
        elif any(t in column_type for t in ['int', 'float', 'double', 'decimal', 'numeric']):
            # Try to parse as float first (handles both int and float)
            if '.' in value_str or 'e' in value_str.lower():
                return float(value_str)
            else:
                return int(value_str)
        
        # String and others: Keep as string
        else:
            return value_str
    
    except (ValueError, AttributeError):
        # If conversion fails, return as string
        return value_str

def create_spark_schema_string(fields):
    """Creates a JSON string for the Spark schema from a list of fields."""
    return json.dumps({"type": "struct", "fields": fields})

def get_latest_ducklake_snapshot(con, table_id):
    """
    Get the latest DuckLake snapshot ID for a table.
    """
    latest_snapshot  = con.execute(f""" SELECT MAX(begin_snapshot) as latest_snapshot FROM ducklake_data_file  WHERE table_id = {table_id} """).fetchone()[0]
    return latest_snapshot

def get_latest_delta_checkpoint(con, table_id):
    """
    check how many times a table has being modified.
    """
    delta_checkpoint = con.execute(f""" SELECT count(snapshot_id) FROM ducklake_snapshot_changes
                                   where changes_made like '%:{table_id}' or changes_made like '%:{table_id},%' """).fetchone()[0]
    return delta_checkpoint

def get_file_modification_time(dummy_time):
    """
    Return a dummy modification time for parquet files.
    This avoids the latency of actually reading file metadata.
    
    Args:
        dummy_time: Timestamp in milliseconds to use as modification time
    
    Returns:
        Modification time in milliseconds
    """
    return dummy_time

def create_dummy_json_log(local_table_root, delta_version, table_info, schema_fields, now, latest_snapshot, 
                         num_files, total_rows=None, total_bytes=None):
    """
    Create a minimal Delta Lake transaction log file for Spark compatibility.
    Writes to local filesystem (temp directory) following Delta Lake specification.
    Entry order: commitInfo → metaData → protocol (as per Delta Lake spec)
    
    Note: The actual add entries are in the checkpoint.parquet file.
    This JSON log provides metadata for Delta readers to understand the checkpoint.
    """
    import uuid
    
    local_delta_log_dir = os.path.join(local_table_root, '_delta_log')
    json_log_file = os.path.join(local_delta_log_dir, f"{delta_version:020d}.json")
    
    # Ensure directory exists
    os.makedirs(local_delta_log_dir, exist_ok=True)
    
    # 1. Commit info entry (FIRST - as per Delta Lake spec)
    commitinfo_json = json.dumps({
        "commitInfo": {
            "timestamp": now,
            "operation": "CONVERT",
            "operationParameters": {
                "convertedFrom": "DuckLake",
                "duckLakeSnapshotId": str(latest_snapshot),
                "partitionBy": "[]"
            },
            "isolationLevel": "Serializable",
            "isBlindAppend": False,
            "operationMetrics": {
                "numFiles": str(num_files),
                "numOutputRows": str(total_rows) if total_rows else "0",
                "numOutputBytes": str(total_bytes) if total_bytes else "0"
            },
            "engineInfo": "DuckLake-Delta-Exporter/1.0.0",
            "txnId": str(uuid.uuid4())
        }
    })
    
    # 2. Metadata entry (SECOND)
    metadata_json = json.dumps({
        "metaData": {
            "id": str(uuid.uuid4()),  # Use UUID for metadata ID
            "name": table_info['table_name'],
            "description": None,
            "format": {
                "provider": "parquet",
                "options": {}
            },
            "schemaString": create_spark_schema_string(schema_fields),
            "partitionColumns": [],
            "createdTime": now,
            "configuration": {}
        }
    })
    
    # 3. Protocol entry (THIRD)
    protocol_json = json.dumps({
        "protocol": {
            "minReaderVersion": 1,
            "minWriterVersion": 2
        }
    })
    
    # Write JSON log file (newline-delimited JSON) in correct order
    with open(json_log_file, 'w') as f:
        f.write(commitinfo_json + '\n')
        f.write(metadata_json + '\n')
        f.write(protocol_json + '\n')
    
    return json_log_file

def build_file_path(table_root, relative_path):
    """
    Build full file path from table root and relative path.
    Works with both local paths and S3 URLs.
    """
    table_root = table_root.rstrip('/')
    relative_path = relative_path.lstrip('/')
    return f"{table_root}/{relative_path}"

def create_checkpoint_for_latest_snapshot(con, table_info, data_root, temp_dir, store=None, token=None):
    """
    Create a Delta checkpoint file for the latest DuckLake snapshot.
    
    Args:
        con: DuckDB connection to DuckLake database
        table_info: Dictionary with table metadata
        data_root: Root path for data (used for constructing remote paths)
        temp_dir: Temporary directory for writing local files
        store: obstore AzureStore instance for uploading files (None for local mode)
        token: Azure auth token (None for local mode)
    """
    # Construct table path (relative to data_root)
    # Clean up paths to avoid double slashes
    schema_path = table_info['schema_path'].strip('/')
    table_path = table_info['table_path'].strip('/')
    table_relative_path = f"{schema_path}/{table_path}" if schema_path else table_path
    
    # Local temporary directory for this table
    local_table_root = os.path.join(temp_dir, table_relative_path.replace('/', os.sep))
    
    # Remote path (for ABFSS upload) - always use forward slashes
    remote_table_root = f"{data_root.rstrip('/')}/{table_relative_path}"
    
    # Get the latest snapshot
    latest_snapshot = get_latest_ducklake_snapshot(con, table_info['table_id'])
    if latest_snapshot is None:
        print(f"⚠️ {table_info['schema_name']}.{table_info['table_name']}: No snapshots found")
        return False
    
    # Use snapshot ID as the delta version
    delta_version = latest_snapshot
    
    # Local checkpoint files (in temp directory)
    local_delta_log_dir = os.path.join(local_table_root, '_delta_log')
    local_checkpoint_file = os.path.join(local_delta_log_dir, f"{delta_version:020d}.checkpoint.parquet")
    local_json_log_file = os.path.join(local_delta_log_dir, f"{delta_version:020d}.json")
    local_last_checkpoint_file = os.path.join(local_delta_log_dir, "_last_checkpoint")
    
    # Remote paths (for ABFSS upload) - always use forward slashes
    remote_checkpoint_file = remote_table_root + f"/_delta_log/{delta_version:020d}.checkpoint.parquet"
    remote_json_log_file = remote_table_root + f"/_delta_log/{delta_version:020d}.json"
    remote_last_checkpoint_file = remote_table_root + "/_delta_log/_last_checkpoint"
    
    # Check if checkpoint already exists (if store is provided)
    if store:
        # Read _last_checkpoint to get the current version
        try:
            last_checkpoint_result = con.execute(f"""
                SELECT version
                FROM read_json_auto('{remote_last_checkpoint_file}')
                LIMIT 1
            """).fetchone()
            
            if last_checkpoint_result:
                current_version = last_checkpoint_result[0]
                current_json_file = remote_table_root + f"/_delta_log/{current_version:020d}.json"
                
                # Read the current version's JSON to check snapshot ID
                result = con.execute(f"""
                    SELECT 
                        commitInfo.operationParameters.duckLakeSnapshotId as snapshot_id
                    FROM read_json_auto('{current_json_file}', format='newline_delimited')
                    WHERE commitInfo IS NOT NULL
                    LIMIT 1
                """).fetchone()
                
                if result and result[0]:
                    last_snapshot = result[0]
                    if last_snapshot == str(latest_snapshot):
                        print(f"⚠️ {table_info['schema_name']}.{table_info['table_name']}: Snapshot {latest_snapshot} already exported (version {current_version})")
                        return False
                    else:
                        print(f"📊 {table_info['schema_name']}.{table_info['table_name']}: New snapshot detected (was {last_snapshot}, now {latest_snapshot})")
        except Exception:
            # _last_checkpoint doesn't exist, this is first export
            pass
    
    now = int(time.time() * 1000)
    
    # Get all files with their stats for the latest snapshot
    file_stats_query = f"""
        SELECT 
            df.data_file_id,
            df.path,
            df.file_size_bytes,
            c.column_name,
            c.column_type,
            fcs.value_count,
            fcs.null_count,
            fcs.min_value,
            fcs.max_value
        FROM ducklake_data_file df
        LEFT JOIN ducklake_file_column_stats fcs ON df.data_file_id = fcs.data_file_id
        LEFT JOIN ducklake_column c ON fcs.column_id = c.column_id
        WHERE df.table_id = {table_info['table_id']}
          AND df.begin_snapshot <= {latest_snapshot}
          AND (df.end_snapshot IS NULL OR df.end_snapshot > {latest_snapshot})
          AND (c.begin_snapshot IS NULL OR c.begin_snapshot <= {latest_snapshot})
          AND (c.end_snapshot IS NULL OR c.end_snapshot > {latest_snapshot})
        ORDER BY df.data_file_id, c.column_order
    """
    
    file_stats_rows = con.execute(file_stats_query).fetchall()
    
    # Group stats by file
    from collections import defaultdict
    files_dict = defaultdict(lambda: {
        'path': None,
        'size': 0,
        'num_records': 0,
        'min_values': {},
        'max_values': {},
        'null_count': {}
    })
    
    for row in file_stats_rows:
        file_id, path, size, col_name, col_type, value_count, null_count, min_val, max_val = row
        
        file_data = files_dict[file_id]
        file_data['path'] = path
        file_data['size'] = size
        
        # Set num_records from first column's value_count (all columns have same count)
        if file_data['num_records'] == 0 and value_count is not None:
            file_data['num_records'] = value_count
        
        # Only add column stats if column name exists (handle LEFT JOIN nulls)
        if col_name is not None:
            # Convert and add min/max values with proper typing
            if min_val is not None:
                file_data['min_values'][col_name] = convert_stat_value_to_json(min_val, col_type)
            if max_val is not None:
                file_data['max_values'][col_name] = convert_stat_value_to_json(max_val, col_type)
            if null_count is not None:
                file_data['null_count'][col_name] = null_count
    
    # Convert to list format for processing
    file_rows = [(f['path'], f['size'], f['num_records'], f['min_values'], f['max_values'], f['null_count']) 
                 for f in files_dict.values()]
    
    # Calculate aggregate metrics for commitInfo
    total_files = len(file_rows)
    total_rows = sum(f[2] for f in file_rows)  # num_records
    total_bytes = sum(f[1] for f in file_rows)  # size
    
    # Get schema for the latest snapshot
    columns = con.execute(f"""
        SELECT column_name, column_type FROM ducklake_column
        WHERE table_id = {table_info['table_id']}
        AND begin_snapshot <= {latest_snapshot} 
        AND (end_snapshot IS NULL OR end_snapshot > {latest_snapshot})
        ORDER BY column_order
    """).fetchall()
    
    # Generate deterministic UUID for table metadata ID (Delta Lake spec requirement)
    # Same table_id always produces same UUID for consistency across versions
    import uuid
    namespace = uuid.UUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8')  # DNS namespace
    table_meta_id = str(uuid.uuid5(namespace, f"ducklake_table_{table_info['table_id']}"))
    
    # Prepare schema
    schema_fields = [
        {"name": name, "type": map_type_ducklake_to_spark(typ), "nullable": True, "metadata": {}} 
        for name, typ in columns
    ]
    
    # Create checkpoint data using DuckDB directly
    checkpoint_data = []
    
    # Create checkpoint data directly in DuckDB using proper data types
    duckdb.execute("DROP TABLE IF EXISTS checkpoint_table")
    
    # Create the checkpoint table with proper nested structure
    duckdb.execute("""
        CREATE TABLE checkpoint_table AS
        WITH checkpoint_data AS (
            -- Protocol record
            SELECT 
                {'minReaderVersion': 1, 'minWriterVersion': 2}::STRUCT(minReaderVersion INTEGER, minWriterVersion INTEGER) AS protocol,
                NULL::STRUCT(id VARCHAR, name VARCHAR, description VARCHAR, format STRUCT(provider VARCHAR, options MAP(VARCHAR, VARCHAR)), schemaString VARCHAR, partitionColumns VARCHAR[], createdTime BIGINT, configuration MAP(VARCHAR, VARCHAR)) AS metaData,
                NULL::STRUCT(path VARCHAR, partitionValues MAP(VARCHAR, VARCHAR), size BIGINT, modificationTime BIGINT, dataChange BOOLEAN, stats VARCHAR, tags MAP(VARCHAR, VARCHAR)) AS add,
                NULL::STRUCT(path VARCHAR, deletionTimestamp BIGINT, dataChange BOOLEAN) AS remove,
                NULL::STRUCT(timestamp TIMESTAMP, operation VARCHAR, operationParameters MAP(VARCHAR, VARCHAR), isBlindAppend BOOLEAN, engineInfo VARCHAR, clientVersion VARCHAR) AS commitInfo
            
            UNION ALL
            
            -- Metadata record
            SELECT 
                NULL::STRUCT(minReaderVersion INTEGER, minWriterVersion INTEGER) AS protocol,
                {
                    'id': ?, 
                    'name': ?, 
                    'description': NULL, 
                    'format': {'provider': 'parquet', 'options': MAP{}}::STRUCT(provider VARCHAR, options MAP(VARCHAR, VARCHAR)),
                    'schemaString': ?, 
                    'partitionColumns': []::VARCHAR[], 
                    'createdTime': ?, 
                    'configuration': MAP{'delta.logRetentionDuration': 'interval 1 hour'}
                }::STRUCT(id VARCHAR, name VARCHAR, description VARCHAR, format STRUCT(provider VARCHAR, options MAP(VARCHAR, VARCHAR)), schemaString VARCHAR, partitionColumns VARCHAR[], createdTime BIGINT, configuration MAP(VARCHAR, VARCHAR)) AS metaData,
                NULL::STRUCT(path VARCHAR, partitionValues MAP(VARCHAR, VARCHAR), size BIGINT, modificationTime BIGINT, dataChange BOOLEAN, stats VARCHAR, tags MAP(VARCHAR, VARCHAR)) AS add,
                NULL::STRUCT(path VARCHAR, deletionTimestamp BIGINT, dataChange BOOLEAN) AS remove,
                NULL::STRUCT(timestamp TIMESTAMP, operation VARCHAR, operationParameters MAP(VARCHAR, VARCHAR), isBlindAppend BOOLEAN, engineInfo VARCHAR, clientVersion VARCHAR) AS commitInfo
        )
        SELECT * FROM checkpoint_data
    """, [table_meta_id, table_info['table_name'], create_spark_schema_string(schema_fields), now])
    
    # Add file records with real statistics
    for path, size, num_records, min_values, max_values, null_count in file_rows:
        rel_path = path.lstrip('/')
        full_path = build_file_path(remote_table_root, rel_path)
        mod_time = get_file_modification_time(now)
        
        # Build stats JSON with real values from DuckLake metadata
        stats_json = json.dumps({
            "numRecords": num_records,
            "minValues": min_values,
            "maxValues": max_values,
            "nullCount": null_count
        })
        
        duckdb.execute("""
            INSERT INTO checkpoint_table
            SELECT 
                NULL::STRUCT(minReaderVersion INTEGER, minWriterVersion INTEGER) AS protocol,
                NULL::STRUCT(id VARCHAR, name VARCHAR, description VARCHAR, format STRUCT(provider VARCHAR, options MAP(VARCHAR, VARCHAR)), schemaString VARCHAR, partitionColumns VARCHAR[], createdTime BIGINT, configuration MAP(VARCHAR, VARCHAR)) AS metaData,
                {
                    'path': ?, 
                    'partitionValues': MAP{}::MAP(VARCHAR, VARCHAR), 
                    'size': ?, 
                    'modificationTime': ?, 
                    'dataChange': true, 
                    'stats': ?, 
                    'tags': MAP{}::MAP(VARCHAR, VARCHAR)
                }::STRUCT(path VARCHAR, partitionValues MAP(VARCHAR, VARCHAR), size BIGINT, modificationTime BIGINT, dataChange BOOLEAN, stats VARCHAR, tags MAP(VARCHAR, VARCHAR)) AS add,
                NULL::STRUCT(path VARCHAR, deletionTimestamp BIGINT, dataChange BOOLEAN) AS remove,
                NULL::STRUCT(timestamp TIMESTAMP, operation VARCHAR, operationParameters MAP(VARCHAR, VARCHAR), isBlindAppend BOOLEAN, engineInfo VARCHAR, clientVersion VARCHAR) AS commitInfo
        """, [rel_path, size, mod_time, stats_json])
    
    # Create the _delta_log directory
    os.makedirs(local_delta_log_dir, exist_ok=True)
    
    # Write the checkpoint file to local temp directory
    duckdb.execute(f"COPY (SELECT * FROM checkpoint_table) TO '{local_checkpoint_file}' (FORMAT PARQUET)")
    
    # Create minimal JSON log file (writes to local temp)
    # Note: Full add entries are in the checkpoint.parquet, JSON only has metadata
    create_dummy_json_log(local_table_root, delta_version, table_info, schema_fields, now, latest_snapshot,
                         total_files, total_rows, total_bytes)
    
    # Write the _last_checkpoint file to local temp directory
    with open(local_last_checkpoint_file, 'w') as f:
        total_records = 2 + len(file_rows)  # protocol + metadata + file records
        f.write(json.dumps({"version": delta_version, "size": total_records}))
    
    # Upload files to OneLake if store is provided
    if store:
        try:
            import obstore as obs
            
            # Extract relative paths from full ABFSS URLs for obstore
            # obstore expects paths relative to the store's base URL
            # remote_checkpoint_file is like: "abfss://.../Tables/simple/ducklake/_delta_log/file.parquet"
            # We need just: "simple/ducklake/_delta_log/file.parquet"
            def get_relative_path(full_path):
                # Split on /Tables/ and take the part after it
                if '/Tables/' in full_path:
                    return full_path.split('/Tables/')[-1]
                return full_path.lstrip('/')
            
            rel_checkpoint = get_relative_path(remote_checkpoint_file)
            rel_json_log = get_relative_path(remote_json_log_file)
            rel_last_checkpoint = get_relative_path(remote_last_checkpoint_file)
            
            # Upload checkpoint file first
            with open(local_checkpoint_file, 'rb') as f:
                obs.put(store, rel_checkpoint, f.read())
            
            # Upload JSON log file second
            with open(local_json_log_file, 'rb') as f:
                obs.put(store, rel_json_log, f.read())
            
            # Upload _last_checkpoint file last for semi-decent consistency
            # (readers check this first to find the latest checkpoint)
            with open(local_last_checkpoint_file, 'rb') as f:
                obs.put(store, rel_last_checkpoint, f.read())
            
            print(f"✅ Exported DuckLake snapshot {latest_snapshot} as Delta checkpoint v{delta_version}")
            print(f"✅ Uploaded to: {remote_table_root}/_delta_log/")
        except Exception as e:
            print(f"❌ Failed to upload checkpoint files: {e}")
            return False
    else:
        # Local mode - files are already written to temp directory
        print(f"✅ Exported DuckLake snapshot {latest_snapshot} as Delta checkpoint v{delta_version}")
        print(f"✅ Created local files in: {local_delta_log_dir}")
    
    # Clean up temporary tables
    duckdb.execute("DROP TABLE IF EXISTS checkpoint_table")
    
    return True, delta_version, latest_snapshot

def generate_latest_delta_log(db_path: str, data_root: str = None, store=None, token=None):
    """
    Export the latest DuckLake snapshot for each table as a Delta checkpoint file.
    Creates both checkpoint files and minimal JSON log files for Spark compatibility.
    
    Args:
        db_path (str): The path to the DuckLake database file (can be ABFSS URL or local path).
        data_root (str): The root directory for the lakehouse data. If None, reads from DuckLake metadata.
        store: obstore AzureStore instance for uploading files (None for local mode).
        token: Azure auth token (None for local mode).
    """
    # Create temporary directory for local file operations
    temp_dir = tempfile.mkdtemp(prefix='ducklake_export_')
    
    try:
        # Create an in-memory DuckDB connection
        con = duckdb.connect(':memory:')
        
        # If token is provided and db_path is ABFSS URL, set up Azure authentication
        if token and db_path.startswith('abfss://'):
            con.sql(f"CREATE OR REPLACE SECRET ducklake_secret (TYPE AZURE, PROVIDER ACCESS_TOKEN, ACCESS_TOKEN '{token}')")
        
        # Attach the DuckLake database (works for both local and ABFSS paths)
        con.execute(f"ATTACH '{db_path}' AS ducklake_db (READ_ONLY)")
        con.execute("USE ducklake_db")
        
        if data_root is None:
            data_root = con.sql("SELECT value FROM ducklake_metadata WHERE key = 'data_path'").fetchone()[0]
        
        # Get all active tables
        tables = con.execute("""
            SELECT 
                t.table_id, 
                t.table_name, 
                s.schema_name,
                t.path as table_path, 
                s.path as schema_path
            FROM ducklake_table t
            JOIN ducklake_schema s USING(schema_id)
            WHERE t.end_snapshot IS NULL
        """).fetchall()
        
        total_tables = len(tables)
        successful_exports = 0
        
        for table_row in tables:
            table_info = {
                'table_id': table_row[0],
                'table_name': table_row[1],
                'schema_name': table_row[2],
                'table_path': table_row[3],
                'schema_path': table_row[4]
            }
            
            table_key = f"{table_info['schema_name']}.{table_info['table_name']}"
            print(f"Processing {table_key}...")
            
            try:
                result = create_checkpoint_for_latest_snapshot(con, table_info, data_root, temp_dir, store, token)
                
                if result is False:
                    # False means checkpoint already exists or no snapshots
                    pass  # Message already printed by the function
                else:
                    successful_exports += 1
                    
            except Exception as e:
                print(f"❌ {table_key}: Failed to export checkpoint - {e}")
                import traceback
                traceback.print_exc()
        
        con.close()
        print(f"\n🎉 Export completed! {successful_exports}/{total_tables} tables exported successfully.")
        
    finally:
        # Clean up temporary directory
        try:
            shutil.rmtree(temp_dir)
        except Exception as e:
            print(f"⚠️ Warning: Could not clean up temp directory {temp_dir}: {e}")