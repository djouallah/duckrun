# File: ducklake_delta_exporter.py
import json
import time
import duckdb
import os
import tempfile
import shutil

def map_type_ducklake_to_spark(t):
    """Maps DuckDB data types to their Spark SQL equivalents for the Delta schema."""
    t = t.lower()
    if 'int' in t:
        return 'long' if '64' in t else 'integer'
    elif 'float' in t:
        return 'double'
    elif 'double' in t:
        return 'double'
    elif 'decimal' in t:
        return 'decimal(10,0)'
    elif 'bool' in t:
        return 'boolean'
    elif 'timestamp' in t:
        return 'timestamp'
    elif 'date' in t:
        return 'date'
    return 'string'

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

def create_dummy_json_log(local_table_root, delta_version, table_info, schema_fields, now, latest_snapshot):
    """
    Create a minimal JSON log file for Spark compatibility.
    Writes to local filesystem (temp directory).
    """
    local_delta_log_dir = os.path.join(local_table_root, '_delta_log')
    json_log_file = os.path.join(local_delta_log_dir, f"{delta_version:020d}.json")
    json_log_file = os.path.join(local_delta_log_dir, f"{delta_version:020d}.json")
    
    # Ensure directory exists
    os.makedirs(local_delta_log_dir, exist_ok=True)
    
    # Protocol entry
    protocol_json = json.dumps({
        "protocol": {
            "minReaderVersion": 1,
            "minWriterVersion": 2
        }
    })
    
    # Metadata entry
    metadata_json = json.dumps({
        "metaData": {
            "id": str(table_info['table_id']),
            "name": table_info['table_name'],
            "description": None,
            "format": {
                "provider": "parquet",
                "options": {}
            },
            "schemaString": create_spark_schema_string(schema_fields),
            "partitionColumns": [],
            "createdTime": now,
            "configuration": {
                "delta.logRetentionDuration": "interval 1 hour"
            }
        }
    })
    
    # Commit info entry
    commitinfo_json = json.dumps({
        "commitInfo": {
            "timestamp": now,
            "operation": "CONVERT",
            "operationParameters": {
                "convertedFrom": "DuckLake",
                "duckLakeSnapshotId": latest_snapshot
            },
            "isBlindAppend": True,
            "engineInfo": "DuckLake-Delta-Exporter",
            "clientVersion": "1.0.0"
        }
    })
    
    # Write JSON log file (newline-delimited JSON)
    with open(json_log_file, 'w') as f:
        f.write(protocol_json + '\n')
        f.write(metadata_json + '\n')
        f.write(commitinfo_json + '\n')
    
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
    delta_version   = get_latest_delta_checkpoint(con, table_info['table_id'])
    
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
        try:
            import obstore as obs
            # Extract relative path for obstore check
            def get_relative_path(full_path):
                if '/Tables/' in full_path:
                    return full_path.split('/Tables/')[-1]
                return full_path.lstrip('/')
            
            rel_json = get_relative_path(remote_json_log_file)
            # Try to read existing JSON log to get last exported snapshot
            try:
                json_bytes = obs.get(store, rel_json)
                json_content = json_bytes.decode('utf-8')
                # Parse newline-delimited JSON to find commitInfo
                for line in json_content.strip().split('\n'):
                    entry = json.loads(line)
                    if 'commitInfo' in entry:
                        last_snapshot = entry['commitInfo'].get('operationParameters', {}).get('duckLakeSnapshotId')
                        if last_snapshot == latest_snapshot:
                            print(f"⚠️ {table_info['schema_name']}.{table_info['table_name']}: Snapshot {latest_snapshot} already exported (version {delta_version})")
                            return False
                        else:
                            print(f"📊 {table_info['schema_name']}.{table_info['table_name']}: New snapshot detected (was {last_snapshot}, now {latest_snapshot})")
                            break
            except Exception:
                # JSON file doesn't exist or couldn't be read, proceed with export
                pass
            
            # Fallback: check if checkpoint file exists (for backwards compatibility)
            rel_checkpoint = get_relative_path(remote_checkpoint_file)
            try:
                obs.head(store, rel_checkpoint)
                print(f"⚠️ {table_info['schema_name']}.{table_info['table_name']}: Checkpoint exists but no snapshot info found (version {delta_version})")
                return False
            except Exception:
                pass
        except:
            pass  # File doesn't exist, proceed with creation
    
    now = int(time.time() * 1000)
    
    # Get all files for the latest snapshot
    file_rows = con.execute(f"""
        SELECT path, file_size_bytes FROM ducklake_data_file
        WHERE table_id = {table_info['table_id']}
        AND begin_snapshot <= {latest_snapshot} 
        AND (end_snapshot IS NULL OR end_snapshot > {latest_snapshot})
    """).fetchall()
    
    # Get schema for the latest snapshot
    columns = con.execute(f"""
        SELECT column_name, column_type FROM ducklake_column
        WHERE table_id = {table_info['table_id']}
        AND begin_snapshot <= {latest_snapshot} 
        AND (end_snapshot IS NULL OR end_snapshot > {latest_snapshot})
        ORDER BY column_order
    """).fetchall()
    
    # Get or generate table metadata ID
    table_meta_id = str(table_info['table_id'])
    
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
    
    # Add file records
    for path, size in file_rows:
        rel_path = path.lstrip('/')
        full_path = build_file_path(remote_table_root, rel_path)
        mod_time = get_file_modification_time(now)
        
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
                    'tags': NULL::MAP(VARCHAR, VARCHAR)
                }::STRUCT(path VARCHAR, partitionValues MAP(VARCHAR, VARCHAR), size BIGINT, modificationTime BIGINT, dataChange BOOLEAN, stats VARCHAR, tags MAP(VARCHAR, VARCHAR)) AS add,
                NULL::STRUCT(path VARCHAR, deletionTimestamp BIGINT, dataChange BOOLEAN) AS remove,
                NULL::STRUCT(timestamp TIMESTAMP, operation VARCHAR, operationParameters MAP(VARCHAR, VARCHAR), isBlindAppend BOOLEAN, engineInfo VARCHAR, clientVersion VARCHAR) AS commitInfo
        """, [rel_path, size, mod_time, json.dumps({"numRecords": None})])
    
    # Create the _delta_log directory
    os.makedirs(local_delta_log_dir, exist_ok=True)
    
    # Write the checkpoint file to local temp directory
    duckdb.execute(f"COPY (SELECT * FROM checkpoint_table) TO '{local_checkpoint_file}' (FORMAT PARQUET)")
    
    # Create dummy JSON log file for Spark compatibility (writes to local temp)
    create_dummy_json_log(local_table_root, delta_version, table_info, schema_fields, now, latest_snapshot)
    
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
            
            # Upload checkpoint file
            with open(local_checkpoint_file, 'rb') as f:
                obs.put(store, rel_checkpoint, f.read())
            
            # Upload JSON log file
            with open(local_json_log_file, 'rb') as f:
                obs.put(store, rel_json_log, f.read())
            
            # Upload _last_checkpoint file
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