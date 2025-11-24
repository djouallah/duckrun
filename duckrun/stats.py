"""
Delta Lake table statistics functionality for duckrun
"""
import duckdb
from deltalake import DeltaTable
from datetime import datetime
import pyarrow as pa


def _table_exists(duckrun_instance, schema_name: str, table_name: str) -> bool:
    """Check if a specific table exists by trying to query it directly."""
    try:
        # For main schema, just use table name directly
        if schema_name == "main":
            query = f"SELECT COUNT(*) FROM {table_name} LIMIT 1"
        else:
            query = f"SELECT COUNT(*) FROM {schema_name}.{table_name} LIMIT 1"
        duckrun_instance.con.execute(query)
        return True
    except:
        return False


def _schema_exists(duckrun_instance, schema_name: str) -> bool:
    """Check if a schema exists by querying information_schema."""
    try:
        # For main schema, always exists
        if schema_name == "main":
            return True
        else:
            # Use information_schema which works in DuckDB 1.2.2
            query = f"SELECT 1 FROM information_schema.schemata WHERE schema_name = '{schema_name}' LIMIT 1"
            result = duckrun_instance.con.execute(query).fetchall()
            return len(result) > 0
    except:
        return False


def _get_existing_tables_in_schema(duckrun_instance, schema_name: str) -> list:
    """Get all existing tables in a schema using information_schema, excluding temporary tables."""
    try:
        # For main schema, use SHOW TABLES
        if schema_name == "main":
            query = "SHOW TABLES"
            result = duckrun_instance.con.execute(query).fetchall()
            if result:
                tables = [row[0] for row in result]
                filtered_tables = [tbl for tbl in tables if not tbl.startswith('tbl_')]
                return filtered_tables
        else:
            # Use information_schema which works in DuckDB 1.2.2
            query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema_name}'"
            result = duckrun_instance.con.execute(query).fetchall()
            if result:
                tables = [row[0] for row in result]
                filtered_tables = [tbl for tbl in tables if not tbl.startswith('tbl_')]
                return filtered_tables
        return []
    except:
        return []


def _match_tables_by_pattern(duckrun_instance, pattern: str) -> dict:
    """Match tables across all schemas using a wildcard pattern.
    Pattern can be:
    - '*.summary' - matches 'summary' table in all schemas
    - '*summary' - matches any table ending with 'summary'
    - 'schema.*' - matches all tables in 'schema'
    Returns a dict mapping schema names to lists of matching table names."""
    import fnmatch
    
    try:
        # Query all schemas and tables in one go
        query = """
            SELECT table_schema, table_name 
            FROM information_schema.tables 
            WHERE table_schema NOT LIKE 'pg_%' 
            AND table_schema != 'information_schema'
            AND table_name NOT LIKE 'tbl_%'
        """
        result = duckrun_instance.con.execute(query).fetchall()
        
        matched = {}
        
        # Check if pattern contains a dot (schema.table pattern)
        if '.' in pattern:
            schema_pattern, table_pattern = pattern.split('.', 1)
            for schema, table in result:
                if fnmatch.fnmatch(schema, schema_pattern) and fnmatch.fnmatch(table, table_pattern):
                    if schema not in matched:
                        matched[schema] = []
                    matched[schema].append(table)
        else:
            # Pattern matches only table names
            for schema, table in result:
                if fnmatch.fnmatch(table, pattern):
                    if schema not in matched:
                        matched[schema] = []
                    matched[schema].append(table)
        
        return matched
    except:
        return {}


def get_stats(duckrun_instance, source: str = None, detailed = False):
    """
    Get comprehensive statistics for Delta Lake tables.
    
    Args:
        duckrun_instance: The Duckrun connection instance
        source: Optional. Can be one of:
               - None: Use all tables in the connection's schema (default)
               - Table name: 'table_name' (uses main schema in DuckDB)
               - Schema.table: 'schema.table_name' (specific table in schema, if multi-schema)
               - Schema only: 'schema' (all tables in schema, if multi-schema)
               - Wildcard pattern: '*.summary' (matches tables across all schemas)
        detailed: Optional. Controls the level of detail in statistics:
                 - False (default): Aggregated table-level stats (total rows, file count, 
                   row groups, average row group size, file sizes, VORDER status)
                 - True: Row group level statistics with compression details, row group sizes,
                   and parquet metadata
    
    Returns:
        DataFrame with statistics based on detailed parameter:
        - If detailed=False: Aggregated table-level summary
        - If detailed=True: Granular file and row group level stats
    
    Examples:
        con = duckrun.connect("tmp/data.lakehouse/test")
        
        # All tables in the connection's schema (aggregated)
        stats = con.get_stats()
        
        # Single table with detailed row group statistics
        stats_detailed = con.get_stats('price_today', detailed=True)
        
        # Specific table in different schema (only if multi-schema enabled)
        stats = con.get_stats('aemo.price')
        
        # All tables in a schema (only if multi-schema enabled)
        stats = con.get_stats('aemo')
        
        # Wildcard pattern across all schemas (only if multi-schema enabled)
        stats = con.get_stats('*.summary')
    """
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # DuckDB always uses 'main' as the default schema, regardless of connection URL schema
    duckdb_schema = "main"
    url_schema = duckrun_instance.schema  # This is from the connection URL path
    
    # If source is not provided, default to all tables in the connection's schema
    if source is None:
        source = url_schema
    
    # Check if source contains wildcard characters
    if '*' in source or '?' in source:
        # Wildcard pattern mode - only valid if multi-schema is enabled
        if not duckrun_instance.scan_all_schemas:
            raise ValueError(f"Wildcard pattern '{source}' not supported. Connection was made to a specific schema '{url_schema}'. Enable multi-schema mode to use wildcards.")
        
        matched_tables = _match_tables_by_pattern(duckrun_instance, source)
        
        if not matched_tables:
            raise ValueError(f"No tables found matching pattern '{source}'")
        
        # Flatten the matched tables into a list with schema info
        tables_with_schemas = []
        for schema, tables in matched_tables.items():
            for table in tables:
                tables_with_schemas.append((schema, table))
        
        print(f"Found {len(tables_with_schemas)} tables matching pattern '{source}'")
        
    # Parse the source and validate existence
    elif '.' in source:
        # Format: schema.table - only valid if multi-schema is enabled
        schema_name, table_name = source.split('.', 1)
        
        if not duckrun_instance.scan_all_schemas:
            raise ValueError(f"Multi-schema format '{source}' not supported. Connection was made to a specific schema '{url_schema}'. Use just the table name '{table_name}' instead.")
        
        # Validate the specific table exists in the actual DuckDB schema
        if not _table_exists(duckrun_instance, schema_name, table_name):
            raise ValueError(f"Table '{table_name}' does not exist in schema '{schema_name}'")
        
        tables_with_schemas = [(schema_name, table_name)]
    else:
        # Could be just table name or schema name
        if duckrun_instance.scan_all_schemas:
            # Multi-schema mode: DuckDB has actual schemas
            # First check if it's a table in main schema
            if _table_exists(duckrun_instance, duckdb_schema, source):
                tables_with_schemas = [(duckdb_schema, source)]
            # Otherwise, check if it's a schema name
            elif _schema_exists(duckrun_instance, source):
                schema_name = source
                list_tables = _get_existing_tables_in_schema(duckrun_instance, source)
                if not list_tables:
                    raise ValueError(f"Schema '{source}' exists but contains no tables")
                tables_with_schemas = [(schema_name, tbl) for tbl in list_tables]
            else:
                raise ValueError(f"Neither table '{source}' in main schema nor schema '{source}' exists")
        else:
            # Single-schema mode: tables are in DuckDB's main schema, use URL schema for file paths
            if _table_exists(duckrun_instance, duckdb_schema, source):
                # It's a table name
                tables_with_schemas = [(url_schema, source)]
            elif source == url_schema:
                # Special case: user asked for stats on the URL schema name - list all tables
                list_tables = _get_existing_tables_in_schema(duckrun_instance, duckdb_schema)
                if not list_tables:
                    raise ValueError(f"No tables found in schema '{url_schema}'")
                tables_with_schemas = [(url_schema, tbl) for tbl in list_tables]
            else:
                raise ValueError(f"Table '{source}' does not exist in the current context (schema: {url_schema})")
    
    # Use the existing connection
    con = duckrun_instance.con
    
    print(f"Processing {len(tables_with_schemas)} tables from {len(set(s for s, t in tables_with_schemas))} schema(s)")
    
    successful_tables = []
    for idx, (schema_name, tbl) in enumerate(tables_with_schemas):
        print(f"[{idx+1}/{len(tables_with_schemas)}] Processing table '{schema_name}.{tbl}'...")
        # Construct lakehouse path using correct ABFSS URL format (no .Lakehouse suffix)
        table_path = f"{duckrun_instance.table_base_url}{schema_name}/{tbl}"
        
        try:
            dt = DeltaTable(table_path)
            add_actions = dt.get_add_actions(flatten=True)
            
            # Convert RecordBatch to dict - works with both PyArrow (deltalake 0.18.2) and arro3 (newer versions)
            # Strategy: Use duck typing - try direct conversion first, then manual extraction
            # This works because both PyArrow and arro3 RecordBatches have schema and column() methods
            
            try:
                # Old deltalake (0.18.2): PyArrow RecordBatch has to_pydict() directly
                xx = add_actions.to_pydict()
            except AttributeError:
                # New deltalake with arro3: Use schema and column() methods
                # This is the universal approach that works with both PyArrow and arro3
                if hasattr(add_actions, 'schema') and hasattr(add_actions, 'column'):
                    # Extract columns manually and create PyArrow table
                    arrow_table = pa.table({name: add_actions.column(name) for name in add_actions.schema.names})
                    xx = arrow_table.to_pydict()
                else:
                    # Fallback: empty dict (shouldn't happen)
                    print(f"Warning: Could not convert RecordBatch for table '{tbl}': Unexpected type {type(add_actions)}")
                    xx = {}
            
            # Check if VORDER exists - handle both formats:
            # 1. Flattened format: 'tags.VORDER' or 'tags.vorder' in keys
            # 2. Nested format: check in 'tags' dict for 'VORDER' or 'vorder'
            vorder = False
            if 'tags.VORDER' in xx.keys() or 'tags.vorder' in xx.keys():
                vorder = True
            elif 'tags' in xx.keys() and xx['tags']:
                # Check nested tags dictionary (tags is a list of dicts, one per file)
                for tag_dict in xx['tags']:
                    if tag_dict and ('VORDER' in tag_dict or 'vorder' in tag_dict):
                        vorder = True
                        break
            
            # Calculate total size
            total_size = sum(xx['size_bytes']) if xx['size_bytes'] else 0
            
            # Get Delta files
            delta_files = dt.files()
            delta = [table_path + "/" + f for f in delta_files]
            
            # Check if table has any files
            if not delta:
                # Empty table - create empty temp table
                con.execute(f'''
                    CREATE OR REPLACE TEMP TABLE tbl_{idx} AS
                    SELECT 
                        '{schema_name}' as schema,
                        '{tbl}' as tbl,
                        'empty' as file_name,
                        0 as num_rows,
                        0 as num_row_groups,
                        0 as size,
                        {vorder} as vorder,
                        '' as compression,
                        '{timestamp}' as timestamp
                    WHERE false
                ''')
            else:
                # Get parquet metadata and create temp table with compression info
                if detailed == True:
                    # Detailed mode: Include ALL parquet_metadata columns
                    con.execute(f'''
                        CREATE OR REPLACE TEMP TABLE tbl_{idx} AS
                        SELECT 
                            '{schema_name}' as schema,
                            '{tbl}' as tbl,
                            {vorder} as vorder,
                            pm.*,
                            '{timestamp}' as timestamp
                        FROM parquet_metadata({delta}) pm
                    ''')
                else:
                    # Aggregated mode: Original summary statistics
                    con.execute(f'''
                        CREATE OR REPLACE TEMP TABLE tbl_{idx} AS
                        SELECT 
                            '{schema_name}' as schema,
                            '{tbl}' as tbl,
                            fm.file_name,
                            fm.num_rows,
                            fm.num_row_groups,
                            CEIL({total_size}/(1024*1024)) as size,
                            {vorder} as vorder,
                            COALESCE(STRING_AGG(DISTINCT pm.compression, ', ' ORDER BY pm.compression), 'UNCOMPRESSED') as compression,
                            '{timestamp}' as timestamp
                        FROM parquet_file_metadata({delta}) fm
                        LEFT JOIN parquet_metadata({delta}) pm ON fm.file_name = pm.file_name
                        GROUP BY fm.file_name, fm.num_rows, fm.num_row_groups
                    ''')
            
        except Exception as e:
            error_msg = str(e)
            print(f"Warning: Could not process table '{tbl}' using DeltaTable API: {e}")
            
            # Fallback: Use DuckDB's delta_scan with filename parameter
            if "Invalid JSON" in error_msg or "MetadataValue" in error_msg:
                print(f"   Detected JSON parsing issue - falling back to DuckDB delta_scan")
            else:
                print(f"   Falling back to DuckDB delta_scan")
            
            try:
                # First get the list of actual parquet files using delta_scan
                file_list_result = con.execute(f'''
                    SELECT DISTINCT filename 
                    FROM delta_scan('{table_path}', filename=1)
                ''').fetchall()
                
                if not file_list_result:
                    # Empty table
                    con.execute(f'''
                        CREATE OR REPLACE TEMP TABLE tbl_{idx} AS
                        SELECT 
                            '{schema_name}' as schema,
                            '{tbl}' as tbl,
                            'empty' as file_name,
                            0 as num_rows,
                            0 as num_row_groups,
                            0 as size,
                            false as vorder,
                            '' as compression,
                            '{timestamp}' as timestamp
                        WHERE false
                    ''')
                else:
                    # Extract just the filename (not the full path) from delta_scan results
                    # delta_scan returns full ABFSS paths, we need to extract just the filename part
                    filenames = []
                    for row in file_list_result:
                        full_path = row[0]
                        # Extract just the filename from the full ABFSS path
                        if '/' in full_path:
                            filename = full_path.split('/')[-1]
                        else:
                            filename = full_path
                        filenames.append(table_path + "/" + filename)
                    
                    # Use parquet_file_metadata to get actual parquet stats with compression
                    if detailed == True:
                        # Detailed mode: Include ALL parquet_metadata columns
                        con.execute(f'''
                            CREATE OR REPLACE TEMP TABLE tbl_{idx} AS
                            SELECT 
                                '{schema_name}' as schema,
                                '{tbl}' as tbl,
                                false as vorder,
                                pm.*,
                                '{timestamp}' as timestamp
                            FROM parquet_metadata({filenames}) pm
                        ''')
                    else:
                        # Aggregated mode: Original summary statistics
                        con.execute(f'''
                            CREATE OR REPLACE TEMP TABLE tbl_{idx} AS
                            SELECT 
                                '{schema_name}' as schema,
                                '{tbl}' as tbl,
                                fm.file_name,
                                fm.num_rows,
                                fm.num_row_groups,
                                0 as size,
                                false as vorder,
                                COALESCE(STRING_AGG(DISTINCT pm.compression, ', ' ORDER BY pm.compression), 'UNCOMPRESSED') as compression,
                                '{timestamp}' as timestamp
                            FROM parquet_file_metadata({filenames}) fm
                            LEFT JOIN parquet_metadata({filenames}) pm ON fm.file_name = pm.file_name
                            GROUP BY fm.file_name, fm.num_rows, fm.num_row_groups
                        ''')
                
                print(f"   ✓ Successfully processed '{tbl}' using DuckDB fallback with parquet metadata")
            except Exception as fallback_error:
                print(f"   ✗ DuckDB fallback also failed for '{tbl}': {fallback_error}")
                print(f"   ⏭️  Skipping table '{tbl}'")
                continue
        
        # Mark this table as successfully processed
        successful_tables.append(idx)
    
    # Only union tables that were successfully processed
    if not successful_tables:
        # No tables were processed successfully - return empty dataframe
        print("⚠️  No tables could be processed successfully")
        import pandas as pd
        if detailed == True:
            return pd.DataFrame(columns=['schema', 'tbl', 'vorder', 'timestamp'])
        else:
            return pd.DataFrame(columns=['schema', 'tbl', 'total_rows', 'num_files', 'num_row_group', 
                                         'average_row_group', 'file_size_MB', 'vorder', 'compression', 'timestamp'])
    
    # Union all successfully processed temp tables
    union_parts = [f'SELECT * FROM tbl_{i}' for i in successful_tables]
    union_query = ' UNION ALL '.join(union_parts)
    
    # Generate final summary based on detailed flag
    if detailed == True:
        # Detailed mode: Return ALL parquet_metadata columns
        final_result = con.execute(f'''
            SELECT *
            FROM ({union_query})
            WHERE tbl IS NOT NULL
            ORDER BY schema, tbl, file_name, row_group_id, column_id
        ''').df()
    else:
        # Aggregated mode: Original summary statistics
        final_result = con.execute(f'''
            SELECT 
                schema,
                tbl,
                SUM(num_rows) as total_rows,
                COUNT(*) as num_files,
                SUM(num_row_groups) as num_row_group,
                CAST(CEIL(SUM(num_rows)::DOUBLE / NULLIF(SUM(num_row_groups), 0)) AS INTEGER) as average_row_group,
                MIN(size) as file_size_MB,
                ANY_VALUE(vorder) as vorder,
                STRING_AGG(DISTINCT compression, ', ' ORDER BY compression) as compression,
                ANY_VALUE(timestamp) as timestamp
            FROM ({union_query})
            WHERE tbl IS NOT NULL
            GROUP BY schema, tbl
            ORDER BY total_rows DESC
        ''').df()
    
    return final_result


def get_rle(duckrun_instance, source: str = None) -> 'pd.DataFrame':
    """
    Get RLE statistics for tables at the column level.
    
    Args:
        duckrun_instance: Duckrun connection (from duckrun.connect())
        source: Optional. Can be one of:
               - None: Use all tables in the connection's schema (default)
               - Table name: 'table_name' (uses main schema in DuckDB)
               - Schema.table: 'schema.table_name' (specific table in schema)
               - Schema only: 'schema' (all tables in schema)
               - Wildcard pattern: '*.summary' (matches tables across all schemas)
    
    Returns:
        DataFrame with columns:
        - schema_name: Schema name
        - table_name: Table name
        - column_name: Column name
        - total_rows: Total number of rows
        - rle_runs: RLE runs for this column in natural order
        - ndv: Number of distinct values
        - total_rle_runs: Sum of RLE runs across all columns (same for all rows of a table)
    """
    import fnmatch
    import pandas as pd
    
    con = duckrun_instance.con  # Get underlying DuckDB connection
    
    # Determine which tables to process
    tables_to_process = []  # List of (schema, table) tuples
    
    if source is None:
        # Get all tables in the connection's schema
        schema_name = duckrun_instance.schema if hasattr(duckrun_instance, 'schema') else 'main'
        try:
            if schema_name == 'main':
                query = "SHOW TABLES"
                result = con.execute(query).fetchall()
                if result:
                    tables = [row[0] for row in result if not row[0].startswith('tbl_')]
                    tables_to_process = [(schema_name, tbl) for tbl in tables]
            else:
                query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema_name}'"
                result = con.execute(query).fetchall()
                if result:
                    tables = [row[0] for row in result if not row[0].startswith('tbl_')]
                    tables_to_process = [(schema_name, tbl) for tbl in tables]
        except:
            pass
    
    elif '.' in source:
        parts = source.split('.', 1)
        schema_pattern, table_pattern = parts[0], parts[1]
        
        # Check if patterns contain wildcards
        if '*' in schema_pattern or '*' in table_pattern:
            # Wildcard matching
            query = """
                SELECT table_schema, table_name 
                FROM information_schema.tables 
                WHERE table_schema NOT LIKE 'pg_%' 
                AND table_schema != 'information_schema'
                AND table_name NOT LIKE 'tbl_%'
            """
            result = con.execute(query).fetchall()
            for schema, table in result:
                if fnmatch.fnmatch(schema, schema_pattern) and fnmatch.fnmatch(table, table_pattern):
                    tables_to_process.append((schema, table))
        else:
            # Exact schema.table
            tables_to_process = [(schema_pattern, table_pattern)]
    
    elif '*' in source:
        # Wildcard pattern for table names across all schemas
        query = """
            SELECT table_schema, table_name 
            FROM information_schema.tables 
            WHERE table_schema NOT LIKE 'pg_%' 
            AND table_schema != 'information_schema'
            AND table_name NOT LIKE 'tbl_%'
        """
        result = con.execute(query).fetchall()
        for schema, table in result:
            if fnmatch.fnmatch(table, source):
                tables_to_process.append((schema, table))
    
    else:
        # Check if it's a schema name or table name
        try:
            # Try as schema first
            schema_query = f"SELECT 1 FROM information_schema.schemata WHERE schema_name = '{source}' LIMIT 1"
            schema_exists = con.execute(schema_query).fetchone()
            
            if schema_exists:
                # It's a schema - get all tables
                tables_query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{source}'"
                result = con.execute(tables_query).fetchall()
                if result:
                    tables = [row[0] for row in result if not row[0].startswith('tbl_')]
                    tables_to_process = [(source, tbl) for tbl in tables]
            else:
                # It's a table name in default schema
                schema_name = duckrun_instance.schema if hasattr(duckrun_instance, 'schema') else 'main'
                tables_to_process = [(schema_name, source)]
        except:
            # Assume it's a table name
            schema_name = duckrun_instance.schema if hasattr(duckrun_instance, 'schema') else 'main'
            tables_to_process = [(schema_name, source)]
    
    if not tables_to_process:
        print("No tables found matching the criteria")
        return pd.DataFrame(columns=['schema_name', 'table_name', 'column_name', 'total_rows', 
                                     'rle_runs', 'ndv', 'total_rle_runs'])
    
    print(f"Processing {len(tables_to_process)} table(s)...")
    
    # Process each table
    results = []
    for schema, table in tables_to_process:
        table_path = f"{duckrun_instance.table_base_url}{schema}/{table}"
        
        print(f"\nCalculating RLE runs for {schema}.{table}...")
        
        # Get column names and row count
        try:
            schema_info = con.sql(f"""
                SELECT column_name
                FROM (DESCRIBE SELECT * FROM delta_scan('{table_path}'))
            """).df()
            
            # Get total row count
            total_rows = con.sql(f"SELECT COUNT(*) FROM delta_scan('{table_path}')").fetchone()[0]
            
            if schema_info.empty:
                continue
            
            # Track total RLE runs for this table
            table_total_rle = 0
            table_results = []
            
            for _, row in schema_info.iterrows():
                col_name = row['column_name']
                
                # Calculate RLE runs in natural (physical) order using delta_scan
                rle_query = f"""
                WITH numbered AS (
                    SELECT 
                        filename,
                        file_row_number,
                        {col_name},
                        LAG({col_name}) OVER (ORDER BY filename, file_row_number) as prev_value
                    FROM delta_scan('{table_path}', file_row_number=1, filename=1)
                )
                SELECT COUNT(*) as runs
                FROM numbered
                WHERE prev_value IS NULL OR {col_name} != prev_value OR {col_name} IS NULL OR prev_value IS NULL
                """
                
                try:
                    runs = con.sql(rle_query).fetchone()[0]
                    
                    # Also calculate NDV for this column
                    ndv_query = f"SELECT COUNT(DISTINCT {col_name}) FROM delta_scan('{table_path}')"
                    ndv = con.sql(ndv_query).fetchone()[0]
                    
                    table_total_rle += runs
                    
                    print(f"  {col_name}: {runs:,} runs, ndv={ndv:,}")
                    
                    table_results.append({
                        'schema_name': schema,
                        'table_name': table,
                        'column_name': col_name,
                        'total_rows': total_rows,
                        'rle_runs': runs,
                        'ndv': ndv
                    })
                except Exception as e:
                    print(f"  Warning: Could not calculate RLE runs for {col_name}: {e}")
            
            # Add total_rle_runs to all rows for this table
            for result in table_results:
                result['total_rle_runs'] = table_total_rle
                results.append(result)
            
            print(f"  Total RLE runs for table: {table_total_rle:,}")
        
        except Exception as e:
            print(f"  Error processing table: {e}")
    
    return pd.DataFrame(results)

