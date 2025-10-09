"""
Delta Lake writer functionality for duckrun - Spark-style write API
"""
from deltalake import DeltaTable, write_deltalake


# Row Group configuration for optimal Delta Lake performance
RG = 8_000_000


def _build_write_deltalake_args(path, df, mode, schema_mode=None, partition_by=None):
    """
    Build arguments for write_deltalake based on requirements:
    - If schema_mode='merge': use rust engine (no row group params)
    - Otherwise: use pyarrow engine with row group optimization
    """
    args = {
        'table_or_uri': path,
        'data': df,
        'mode': mode
    }
    
    # Add partition_by if specified
    if partition_by:
        args['partition_by'] = partition_by
    
    # Engine selection based on schema_mode
    if schema_mode == 'merge':
        # Use rust engine for schema merging (no row group params supported)
        args['schema_mode'] = 'merge'
        args['engine'] = 'rust'
    else:
        # Use pyarrow engine with row group optimization (default)
        args['max_rows_per_file'] = RG
        args['max_rows_per_group'] = RG
        args['min_rows_per_group'] = RG
    
    return args


class DeltaWriter:
    """Spark-style write API for Delta Lake"""
    
    def __init__(self, relation, duckrun_instance):
        self.relation = relation
        self.duckrun = duckrun_instance
        self._format = "delta"
        self._mode = "overwrite"
        self._schema_mode = None
        self._partition_by = None
    
    def format(self, format_type: str):
        """Set output format (only 'delta' supported)"""
        if format_type.lower() != "delta":
            raise ValueError(f"Only 'delta' format is supported, got '{format_type}'")
        self._format = "delta"
        return self
    
    def mode(self, write_mode: str):
        """Set write mode: 'overwrite' or 'append'"""
        if write_mode not in {"overwrite", "append"}:
            raise ValueError(f"Mode must be 'overwrite' or 'append', got '{write_mode}'")
        self._mode = write_mode
        return self
    
    def option(self, key: str, value):
        """Set write option (Spark-compatible)"""
        if key == "mergeSchema":
            if str(value).lower() in ("true", "1"):
                self._schema_mode = "merge"
            else:
                self._schema_mode = None
        else:
            raise ValueError(f"Unsupported option: {key}")
        return self
    
    def partitionBy(self, *columns):
        """Set partition columns (Spark-compatible)"""
        if len(columns) == 1 and isinstance(columns[0], (list, tuple)):
            # Handle partitionBy(["col1", "col2"]) case
            self._partition_by = list(columns[0])
        else:
            # Handle partitionBy("col1", "col2") case
            self._partition_by = list(columns)
        return self
    
    def saveAsTable(self, table_name: str):
        """Save query result as Delta table"""
        if self._format != "delta":
            raise RuntimeError(f"Only 'delta' format is supported, got '{self._format}'")
        
        if "." in table_name:
            schema, table = table_name.split(".", 1)
        else:
            schema = self.duckrun.schema
            table = table_name
        
        self.duckrun._create_onelake_secret()
        path = f"{self.duckrun.table_base_url}{schema}/{table}"
        df = self.relation.record_batch()
        
        # Build write arguments based on schema_mode and partition_by
        write_args = _build_write_deltalake_args(
            path, df, self._mode, 
            schema_mode=self._schema_mode,
            partition_by=self._partition_by
        )
        
        engine_info = f" (engine=rust, schema_mode=merge)" if self._schema_mode == 'merge' else " (engine=pyarrow)"
        partition_info = f" partitioned by {self._partition_by}" if self._partition_by else ""
        print(f"Writing to Delta table: {schema}.{table} (mode={self._mode}){engine_info}{partition_info}")
        
        write_deltalake(**write_args)
        
        # Create view with appropriate schema qualification
        # If user explicitly specified schema.table, create view with schema qualification
        # If user just specified table, create view in current schema
        if "." in table_name:
            # User explicitly specified schema.table - create qualified view
            view_name = f"{schema}.{table}"
            # Ensure the schema exists before creating the view
            self.duckrun.con.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        else:
            # User specified just table name - create view in current schema
            view_name = table
            
        self.duckrun.con.sql(f"DROP VIEW IF EXISTS {view_name}")
        self.duckrun.con.sql(f"""
            CREATE OR REPLACE VIEW {view_name}
            AS SELECT * FROM delta_scan('{path}')
        """)
        
        dt = DeltaTable(path)
        
        if self._mode == "overwrite":
            dt.vacuum(retention_hours=0, dry_run=False, enforce_retention_duration=False)
            dt.cleanup_metadata()
            print(f"✅ Table {schema}.{table} created/overwritten")
        else:
            file_count = len(dt.file_uris())
            if file_count > self.duckrun.compaction_threshold:
                print(f"Compacting {schema}.{table} ({file_count} files)")
                dt.optimize.compact()
                dt.vacuum(dry_run=False)
                dt.cleanup_metadata()
            print(f"✅ Data appended to {schema}.{table}")
        
        return table


class QueryResult:
    """Wrapper for DuckDB relation with write API"""
    
    def __init__(self, relation, duckrun_instance):
        self.relation = relation
        self.duckrun = duckrun_instance
    
    @property
    def write(self):
        """Access write API"""
        return DeltaWriter(self.relation, self.duckrun)
    
    def __getattr__(self, name):
        """Delegate all other methods to underlying DuckDB relation"""
        return getattr(self.relation, name)