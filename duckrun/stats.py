"""
Delta Lake table statistics functionality for duckrun
"""
import duckdb
from deltalake import DeltaTable
from datetime import datetime


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


def get_stats(duckrun_instance, source: str):
    """
    Get comprehensive statistics for Delta Lake tables.
    
    Args:
        duckrun_instance: The Duckrun connection instance
        source: Can be one of:
               - Table name: 'table_name' (uses main schema in DuckDB)
               - Schema.table: 'schema.table_name' (specific table in schema, if multi-schema)
               - Schema only: 'schema' (all tables in schema, if multi-schema)
    
    Returns:
        Arrow table with statistics including total rows, file count, row groups, 
        average row group size, file sizes, VORDER status, and timestamp
    
    Examples:
        con = duckrun.connect("tmp/data.lakehouse/test")
        
        # Single table in main schema (DuckDB uses 'main', not 'test')
        stats = con.get_stats('price_today')
        
        # Specific table in different schema (only if multi-schema enabled)
        stats = con.get_stats('aemo.price')
        
        # All tables in a schema (only if multi-schema enabled)
        stats = con.get_stats('aemo')
    """
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # DuckDB always uses 'main' as the default schema, regardless of connection URL schema
    duckdb_schema = "main"
    url_schema = duckrun_instance.schema  # This is from the connection URL path
    
    # Parse the source and validate existence
    if '.' in source:
        # Format: schema.table - only valid if multi-schema is enabled
        schema_name, table_name = source.split('.', 1)
        
        if not duckrun_instance.scan_all_schemas:
            raise ValueError(f"Multi-schema format '{source}' not supported. Connection was made to a specific schema '{url_schema}'. Use just the table name '{table_name}' instead.")
        
        # Validate the specific table exists in the actual DuckDB schema
        if not _table_exists(duckrun_instance, schema_name, table_name):
            raise ValueError(f"Table '{table_name}' does not exist in schema '{schema_name}'")
        
        list_tables = [table_name]
    else:
        # Could be just table name or schema name
        if duckrun_instance.scan_all_schemas:
            # Multi-schema mode: DuckDB has actual schemas
            # First check if it's a table in main schema
            if _table_exists(duckrun_instance, duckdb_schema, source):
                list_tables = [source]
                schema_name = duckdb_schema
            # Otherwise, check if it's a schema name
            elif _schema_exists(duckrun_instance, source):
                schema_name = source
                list_tables = _get_existing_tables_in_schema(duckrun_instance, source)
                if not list_tables:
                    raise ValueError(f"Schema '{source}' exists but contains no tables")
            else:
                raise ValueError(f"Neither table '{source}' in main schema nor schema '{source}' exists")
        else:
            # Single-schema mode: tables are in DuckDB's main schema, use URL schema for file paths
            if _table_exists(duckrun_instance, duckdb_schema, source):
                # It's a table name
                list_tables = [source]
                schema_name = url_schema  # Use URL schema for file path construction
            elif source == url_schema:
                # Special case: user asked for stats on the URL schema name - list all tables
                list_tables = _get_existing_tables_in_schema(duckrun_instance, duckdb_schema)
                schema_name = url_schema  # Use URL schema for file path construction
                if not list_tables:
                    raise ValueError(f"No tables found in schema '{url_schema}'")
            else:
                raise ValueError(f"Table '{source}' does not exist in the current context (schema: {url_schema})")
    
    # Use the existing connection
    con = duckrun_instance.con
    
    print(f"Processing {len(list_tables)} tables: {list_tables}")
    
    for idx, tbl in enumerate(list_tables):
        # Construct lakehouse path using correct ABFSS URL format (no .Lakehouse suffix)
        table_path = f"{duckrun_instance.table_base_url}{schema_name}/{tbl}"
        
        try:
            dt = DeltaTable(table_path)
            xx = dt.get_add_actions(flatten=True).to_pydict()
            
            # Check if VORDER exists
            vorder = 'tags.VORDER' in xx.keys()
            
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
                        '{tbl}' as tbl,
                        'empty' as file_name,
                        0 as num_rows,
                        0 as num_row_groups,
                        0 as size,
                        {vorder} as vorder,
                        '{timestamp}' as timestamp
                    WHERE false
                ''')
            else:
                # Get parquet metadata and create temp table
                con.execute(f'''
                    CREATE OR REPLACE TEMP TABLE tbl_{idx} AS
                    SELECT 
                        '{tbl}' as tbl,
                        file_name,
                        num_rows,
                        num_row_groups,
                        CEIL({total_size}/(1024*1024)) as size,
                        {vorder} as vorder,
                        '{timestamp}' as timestamp
                    FROM parquet_file_metadata({delta})
                ''')
            
        except Exception as e:
            print(f"Warning: Could not process table '{tbl}': {e}")
            # Create empty temp table for failed tables
            con.execute(f'''
                CREATE OR REPLACE TEMP TABLE tbl_{idx} AS
                SELECT 
                    '{tbl}' as tbl,
                    'error' as file_name,
                    0 as num_rows,
                    0 as num_row_groups,
                    0 as size,
                    false as vorder,
                    '{timestamp}' as timestamp
                WHERE false
            ''')
    
    # Union all temp tables
    union_parts = [f'SELECT * FROM tbl_{i}' for i in range(len(list_tables))]
    union_query = ' UNION ALL '.join(union_parts)
    
    # Generate final summary
    final_result = con.execute(f'''
        SELECT 
            tbl,
            SUM(num_rows) as total_rows,
            COUNT(*) as num_files,
            SUM(num_row_groups) as num_row_group,
            CAST(CEIL(SUM(num_rows)::DOUBLE / NULLIF(SUM(num_row_groups), 0)) AS INTEGER) as average_row_group,
            MIN(size) as file_size_MB,
            ANY_VALUE(vorder) as vorder,
            ANY_VALUE(timestamp) as timestamp
        FROM ({union_query})
        WHERE tbl IS NOT NULL
        GROUP BY tbl
        ORDER BY total_rows DESC
    ''').df()
    
    return final_result


