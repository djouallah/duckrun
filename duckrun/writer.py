"""
Delta Lake writer functionality for duckrun - Spark-style write API
"""
from deltalake import DeltaTable, write_deltalake, __version__ as deltalake_version

# Try to import WriterProperties for Rust engine (available in 0.18.2+)
try:
    from deltalake.writer import WriterProperties
    _HAS_WRITER_PROPERTIES = True
except ImportError:
    _HAS_WRITER_PROPERTIES = False

# Try to import PyArrow dataset for old PyArrow engine
try:
    import pyarrow.dataset as ds
    _HAS_PYARROW_DATASET = True
except ImportError:
    _HAS_PYARROW_DATASET = False


# Row Group configuration for optimal Delta Lake performance
RG = 8_000_000

# Check deltalake version once at module load
# Version 0.18.x and 0.19.x support engine parameter and row group optimization
# Version 0.20+ removed these features (rust only, no row groups)
_DELTALAKE_VERSION = tuple(map(int, deltalake_version.split('.')[:2]))
_IS_OLD_DELTALAKE = _DELTALAKE_VERSION < (0, 20)


def _build_write_deltalake_args(path, df, mode, schema_mode=None, partition_by=None):
    """
    Build arguments for write_deltalake based on requirements and version:
    
    deltalake 0.18.2 - 0.19.x:
    - Has 'engine' parameter (defaults to 'pyarrow')
    - Has max_rows_per_file/max_rows_per_group/min_rows_per_group for optimization
    - When mergeSchema=True: must set schema_mode='merge' + engine='rust', NO row group params
    - When mergeSchema=False: use row group params, DON'T set engine (pyarrow is default)
    - COMPRESSION: Defaults to ZSTD via writer_properties (rust) or file_options (pyarrow)
    
    deltalake 0.20+:
    - Does NOT have 'engine' parameter (everything is rust, pyarrow deprecated)
    - Does NOT have max_rows_per_file (row group optimization removed)
    - When mergeSchema=True: must set schema_mode='merge'
    - When mergeSchema=False: just write normally (no special params)
    - COMPRESSION: Defaults to ZSTD via writer_properties (rust only)
    
    Uses version detection for simpler logic.
    """
    args = {
        'table_or_uri': path,
        'data': df,
        'mode': mode
    }
    
    # Add partition_by if specified
    if partition_by:
        args['partition_by'] = partition_by
    
    if schema_mode == 'merge':
        # Schema merging mode - must explicitly set schema_mode='merge'
        args['schema_mode'] = 'merge'
        
        if _IS_OLD_DELTALAKE:
            # deltalake 0.18.2-0.19.x: must also set engine='rust' for schema merging
            # Do NOT use row group params (they conflict with rust engine)
            args['engine'] = 'rust'
            # Set ZSTD compression for Rust engine
            if _HAS_WRITER_PROPERTIES:
                args['writer_properties'] = WriterProperties(compression='ZSTD')
        else:
            # Version 0.20+: rust is default, just add compression
            if _HAS_WRITER_PROPERTIES:
                args['writer_properties'] = WriterProperties(compression='ZSTD')
    else:
        # Normal write mode (no schema merging)
        if _IS_OLD_DELTALAKE:
            # deltalake 0.18.2-0.19.x: use row group optimization
            # DON'T set engine parameter - pyarrow is the default and works with row groups
            args['max_rows_per_file'] = RG
            args['max_rows_per_group'] = RG
            args['min_rows_per_group'] = RG
            # Set ZSTD compression for PyArrow engine
            if _HAS_PYARROW_DATASET:
                args['file_options'] = ds.ParquetFileFormat().make_write_options(
                    compression='ZSTD'
                )
        else:
            # Version 0.20+: no optimization available (rust by default, no row group params supported)
            # Set ZSTD compression for Rust engine
            if _HAS_WRITER_PROPERTIES:
                args['writer_properties'] = WriterProperties(compression='ZSTD')
    
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
        """Set write mode: 'overwrite', 'append', or 'ignore'"""
        if write_mode not in {"overwrite", "append", "ignore"}:
            raise ValueError(f"Mode must be 'overwrite', 'append', or 'ignore', got '{write_mode}'")
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
        
        # Handle 'ignore' mode - skip if table already exists
        if self._mode == 'ignore':
            try:
                DeltaTable(path)
                print(f"Table {schema}.{table} exists. Skipping (mode='ignore')")
                return table
            except Exception:
                # Table doesn't exist, proceed with creation
                print(f"Creating table {schema}.{table} (mode='ignore', table doesn't exist)")
                # Change mode to 'overwrite' for actual write
                self._mode = 'overwrite'
        
        df = self.relation.record_batch()
        
        # Build write arguments based on schema_mode and partition_by
        write_args = _build_write_deltalake_args(
            path, df, self._mode, 
            schema_mode=self._schema_mode,
            partition_by=self._partition_by
        )
        
        # Prepare info message based on version and settings
        if self._schema_mode == 'merge':
            if _IS_OLD_DELTALAKE:
                engine_info = " (engine=rust, schema_mode=merge, compression=ZSTD)"
            else:
                engine_info = " (schema_mode=merge, rust by default, compression=ZSTD)"
        else:
            if _IS_OLD_DELTALAKE:
                engine_info = " (engine=pyarrow, optimized row groups, compression=ZSTD)"
            else:
                engine_info = " (engine=rust by default, compression=ZSTD)"
        
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