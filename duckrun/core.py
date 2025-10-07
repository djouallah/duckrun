import duckdb
import requests
import os
import importlib.util
from deltalake import DeltaTable, write_deltalake
from typing import List, Tuple, Union, Optional, Callable, Dict, Any
from string import Template
import obstore as obs
from obstore.store import AzureStore

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
        
        self.duckrun.con.sql(f"DROP VIEW IF EXISTS {table}")
        self.duckrun.con.sql(f"""
            CREATE OR REPLACE VIEW {table}
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


class Duckrun:
    """
    Lakehouse task runner with clean tuple-based API.
    Powered by DuckDB for fast data processing.
    
    Task formats:
        Python: ('function_name', (arg1, arg2, ...))
        SQL:    ('table_name', 'mode', {params})
    
    Usage:
        # For pipelines:
        dr = Duckrun.connect("workspace/lakehouse.lakehouse/schema", sql_folder="./sql")
        dr = Duckrun.connect("workspace/lakehouse.lakehouse")  # defaults to dbo schema, lists all tables
        dr.run(pipeline)
        
        # For data exploration with Spark-style API:
        dr = Duckrun.connect("workspace/lakehouse.lakehouse")
        dr.sql("SELECT * FROM table").show()
        dr.sql("SELECT 43").write.mode("append").saveAsTable("test")
        
        # Schema evolution and partitioning (exact Spark API):
        dr.sql("SELECT * FROM source").write.mode("append").option("mergeSchema", "true").partitionBy("region").saveAsTable("sales")
        
        # Pipeline formats:
        pipeline = [
            # SQL with parameters only
            ('table_name', 'mode', {'param1': 'value1'}),
            
            # SQL with Delta options (4-tuple format)
            ('table_name', 'mode', {'param1': 'value1'}, {'mergeSchema': 'true', 'partitionBy': ['region']}),
            
            # Python task
            ('process_data', ('table_name',))
        ]
    """

    def __init__(self, workspace: str, lakehouse_name: str, schema: str = "dbo", 
                 sql_folder: Optional[str] = None, compaction_threshold: int = 10,
                 scan_all_schemas: bool = False):
        self.workspace = workspace
        self.lakehouse_name = lakehouse_name
        self.schema = schema
        self.sql_folder = sql_folder.strip() if sql_folder else None
        self.compaction_threshold = compaction_threshold
        self.scan_all_schemas = scan_all_schemas
        self.table_base_url = f'abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{lakehouse_name}.Lakehouse/Tables/'
        self.con = duckdb.connect()
        self.con.sql("SET preserve_insertion_order = false")
        self._attach_lakehouse()

    @classmethod
    def connect(cls, connection_string: str, sql_folder: Optional[str] = None,
                compaction_threshold: int = 100):
        """
        Create and connect to lakehouse.
        
        Uses compact format: connect("ws/lh.lakehouse/schema") or connect("ws/lh.lakehouse")
        
        Args:
            connection_string: OneLake path "ws/lh.lakehouse/schema" or "ws/lh.lakehouse"
            sql_folder: Optional path or URL to SQL files folder
            compaction_threshold: File count threshold for compaction
        
        Examples:
            dr = Duckrun.connect("ws/lh.lakehouse/schema", sql_folder="./sql")
            dr = Duckrun.connect("ws/lh.lakehouse/schema")  # no SQL folder
            dr = Duckrun.connect("ws/lh.lakehouse")  # defaults to dbo schema
        """
        print("Connecting to Lakehouse...")
        
        scan_all_schemas = False
        
        # Only support compact format: "ws/lh.lakehouse/schema" or "ws/lh.lakehouse"
        if not connection_string or "/" not in connection_string:
            raise ValueError(
                "Invalid connection string format. "
                "Expected format: 'workspace/lakehouse.lakehouse/schema' or 'workspace/lakehouse.lakehouse'"
            )
        
        parts = connection_string.split("/")
        if len(parts) == 2:
            workspace, lakehouse_name = parts
            scan_all_schemas = True
            schema = "dbo"
            print(f"ℹ️  No schema specified. Using default schema 'dbo' for operations.")
            print(f"   Scanning all schemas for table discovery...\n")
        elif len(parts) == 3:
            workspace, lakehouse_name, schema = parts
        else:
            raise ValueError(
                f"Invalid connection string format: '{connection_string}'. "
                "Expected format: 'workspace/lakehouse.lakehouse' or 'workspace/lakehouse.lakehouse/schema'"
            )
        
        if lakehouse_name.endswith(".lakehouse"):
            lakehouse_name = lakehouse_name[:-10]
        
        if not workspace or not lakehouse_name:
            raise ValueError(
                "Missing required parameters. Use compact format:\n"
                "  connect('workspace/lakehouse.lakehouse/schema', 'sql_folder')\n"
                "  connect('workspace/lakehouse.lakehouse')  # defaults to dbo"
            )
        
        return cls(workspace, lakehouse_name, schema, sql_folder, compaction_threshold, scan_all_schemas)

    def _get_storage_token(self):
        return os.environ.get("AZURE_STORAGE_TOKEN", "PLACEHOLDER_TOKEN_TOKEN_NOT_AVAILABLE")

    def _create_onelake_secret(self):
        token = self._get_storage_token()
        if token != "PLACEHOLDER_TOKEN_TOKEN_NOT_AVAILABLE":
            self.con.sql(f"CREATE OR REPLACE SECRET onelake (TYPE AZURE, PROVIDER ACCESS_TOKEN, ACCESS_TOKEN '{token}')")
        else:
            print("Authenticating with Azure (trying CLI, will fallback to browser if needed)...")
            from azure.identity import AzureCliCredential, InteractiveBrowserCredential, ChainedTokenCredential
            credential = ChainedTokenCredential(AzureCliCredential(), InteractiveBrowserCredential())
            token = credential.get_token("https://storage.azure.com/.default")
            os.environ["AZURE_STORAGE_TOKEN"] = token.token
            self.con.sql("CREATE OR REPLACE PERSISTENT SECRET onelake (TYPE azure, PROVIDER credential_chain, CHAIN 'cli', ACCOUNT_NAME 'onelake')")

    def _discover_tables_fast(self) -> List[Tuple[str, str]]:
        """
        Fast Delta table discovery using obstore with list_with_delimiter.
        Only lists directories, not files - super fast!
        
        Returns:
            List of tuples: [(schema, table_name), ...]
        """
        token = self._get_storage_token()
        if token == "PLACEHOLDER_TOKEN_TOKEN_NOT_AVAILABLE":
            print("Authenticating with Azure for table discovery (trying CLI, will fallback to browser if needed)...")
            from azure.identity import AzureCliCredential, InteractiveBrowserCredential, ChainedTokenCredential
            credential = ChainedTokenCredential(AzureCliCredential(), InteractiveBrowserCredential())
            token_obj = credential.get_token("https://storage.azure.com/.default")
            token = token_obj.token
            os.environ["AZURE_STORAGE_TOKEN"] = token
        
        url = f"abfss://{self.workspace}@onelake.dfs.fabric.microsoft.com/"
        store = AzureStore.from_url(url, bearer_token=token)
        
        base_path = f"{self.lakehouse_name}.Lakehouse/Tables/"
        tables_found = []
        
        if self.scan_all_schemas:
            # Discover all schemas first
            print("🔍 Discovering schemas...")
            schemas_result = obs.list_with_delimiter(store, prefix=base_path)
            schemas = [
                prefix.rstrip('/').split('/')[-1] 
                for prefix in schemas_result['common_prefixes']
            ]
            print(f"   Found {len(schemas)} schemas: {', '.join(schemas)}\n")
            
            # Discover tables in each schema
            print("🔍 Discovering tables...")
            for schema_name in schemas:
                schema_path = f"{base_path}{schema_name}/"
                result = obs.list_with_delimiter(store, prefix=schema_path)
                
                for table_prefix in result['common_prefixes']:
                    table_name = table_prefix.rstrip('/').split('/')[-1]
                    # Skip non-table directories
                    if table_name not in ('metadata', 'iceberg'):
                        tables_found.append((schema_name, table_name))
        else:
            # Scan specific schema only
            print(f"🔍 Discovering tables in schema '{self.schema}'...")
            schema_path = f"{base_path}{self.schema}/"
            result = obs.list_with_delimiter(store, prefix=schema_path)
            
            for table_prefix in result['common_prefixes']:
                table_name = table_prefix.rstrip('/').split('/')[-1]
                if table_name not in ('metadata', 'iceberg'):
                    tables_found.append((self.schema, table_name))
        
        return tables_found

    def _attach_lakehouse(self):
        """Attach lakehouse tables as DuckDB views using fast discovery"""
        self._create_onelake_secret()
        
        try:
            tables = self._discover_tables_fast()
            
            if not tables:
                if self.scan_all_schemas:
                    print(f"No Delta tables found in {self.lakehouse_name}.Lakehouse/Tables/")
                else:
                    print(f"No Delta tables found in {self.lakehouse_name}.Lakehouse/Tables/{self.schema}/")
                return
            
            print(f"\n📊 Found {len(tables)} Delta tables. Attaching as views...\n")
            
            attached_count = 0
            for schema_name, table_name in tables:
                try:
                    if self.scan_all_schemas:
                        # Create proper schema.table structure in DuckDB
                        self.con.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
                        view_name = f"{schema_name}.{table_name}"
                    else:
                        # Single schema mode - use just table name
                        view_name = table_name
                    
                    self.con.sql(f"""
                        CREATE OR REPLACE VIEW {view_name}
                        AS SELECT * FROM delta_scan('{self.table_base_url}{schema_name}/{table_name}');
                    """)
                    print(f"  ✓ Attached: {schema_name}.{table_name} → {view_name}")
                    attached_count += 1
                except Exception as e:
                    print(f"  ⚠ Skipped {schema_name}.{table_name}: {str(e)[:100]}")
                    continue
            
            print(f"\n{'='*60}")
            print(f"✅ Successfully attached {attached_count}/{len(tables)} tables")
            print(f"{'='*60}\n")
            
            if self.scan_all_schemas:
                print(f"\n💡 Note: Tables use schema.table format (e.g., aemo.calendar, dbo.results)")
                print(f"   Default schema for operations: {self.schema}\n")
                
        except Exception as e:
            print(f"❌ Error attaching lakehouse: {e}")
            print("Continuing without pre-attached tables.")

    def _normalize_table_name(self, name: str) -> str:
        """Extract base table name before first '__'"""
        return name.split('__', 1)[0] if '__' in name else name

    def _read_sql_file(self, table_name: str, params: Optional[Dict] = None) -> Optional[str]:
        if self.sql_folder is None:
            raise RuntimeError("sql_folder is not configured. Cannot read SQL files.")
        
        is_url = self.sql_folder.startswith("http")
        if is_url:
            url = f"{self.sql_folder.rstrip('/')}/{table_name}.sql".strip()
            try:
                resp = requests.get(url)
                resp.raise_for_status()
                content = resp.text
            except Exception as e:
                print(f"Failed to fetch SQL from {url}: {e}")
                return None
        else:
            path = os.path.join(self.sql_folder, f"{table_name}.sql")
            try:
                with open(path, 'r') as f:
                    content = f.read()
            except Exception as e:
                print(f"Failed to read SQL file {path}: {e}")
                return None

        if not content.strip():
            print(f"SQL file is empty: {table_name}.sql")
            return None

        full_params = {
            'ws': self.workspace,
            'lh': self.lakehouse_name,
            'schema': self.schema
        }
        if params:
            full_params.update(params)

        try:
            template = Template(content)
            content = template.substitute(full_params)
        except KeyError as e:
            print(f"Missing parameter in SQL file: ${e}")
            return None
        except Exception as e:
            print(f"Error during SQL template substitution: {e}")
            return None

        return content

    def _load_py_function(self, name: str) -> Optional[Callable]:
        if self.sql_folder is None:
            raise RuntimeError("sql_folder is not configured. Cannot load Python functions.")
        
        is_url = self.sql_folder.startswith("http")
        try:
            if is_url:
                url = f"{self.sql_folder.rstrip('/')}/{name}.py".strip()
                resp = requests.get(url)
                resp.raise_for_status()
                code = resp.text
                namespace = {}
                exec(code, namespace)
                func = namespace.get(name)
                return func if callable(func) else None
            else:
                path = os.path.join(self.sql_folder, f"{name}.py")
                if not os.path.isfile(path):
                    print(f"Python file not found: {path}")
                    return None
                spec = importlib.util.spec_from_file_location(name, path)
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)
                func = getattr(mod, name, None)
                return func if callable(func) else None
        except Exception as e:
            print(f"Error loading Python function '{name}': {e}")
            return None

    def _run_python(self, name: str, args: tuple) -> Any:
        """Execute Python task, return result"""
        self._create_onelake_secret()
        func = self._load_py_function(name)
        if not func:
            raise RuntimeError(f"Python function '{name}' not found")
        
        print(f"Running Python: {name}{args}")
        result = func(*args)
        print(f"✅ Python '{name}' completed")
        return result

    def _run_sql(self, table: str, mode: str, params: Dict, delta_options: Dict = None) -> str:
        """Execute SQL task, write to Delta, return normalized table name"""
        self._create_onelake_secret()
        
        if mode not in {'overwrite', 'append', 'ignore'}:
            raise ValueError(f"Invalid mode '{mode}'. Use: overwrite, append, or ignore")

        sql = self._read_sql_file(table, params)
        if sql is None:
            raise RuntimeError(f"Failed to read SQL file for '{table}'")

        normalized_table = self._normalize_table_name(table)
        path = f"{self.table_base_url}{self.schema}/{normalized_table}"

        # Extract Delta Lake specific options from delta_options
        delta_options = delta_options or {}
        merge_schema = delta_options.get('mergeSchema')
        schema_mode = 'merge' if str(merge_schema).lower() in ('true', '1') else None
        partition_by = delta_options.get('partitionBy') or delta_options.get('partition_by')

        if mode == 'overwrite':
            self.con.sql(f"DROP VIEW IF EXISTS {normalized_table}")
            df = self.con.sql(sql).record_batch()
            
            write_args = _build_write_deltalake_args(
                path, df, 'overwrite', 
                schema_mode=schema_mode, 
                partition_by=partition_by
            )
            write_deltalake(**write_args)
            
            self.con.sql(f"CREATE OR REPLACE VIEW {normalized_table} AS SELECT * FROM delta_scan('{path}')")
            dt = DeltaTable(path)
            dt.vacuum(retention_hours=0, dry_run=False, enforce_retention_duration=False)
            dt.cleanup_metadata()

        elif mode == 'append':
            df = self.con.sql(sql).record_batch()
            
            write_args = _build_write_deltalake_args(
                path, df, 'append', 
                schema_mode=schema_mode, 
                partition_by=partition_by
            )
            write_deltalake(**write_args)
            
            self.con.sql(f"CREATE OR REPLACE VIEW {normalized_table} AS SELECT * FROM delta_scan('{path}')")
            dt = DeltaTable(path)
            if len(dt.file_uris()) > self.compaction_threshold:
                print(f"Compacting {normalized_table} ({len(dt.file_uris())} files)")
                dt.optimize.compact()
                dt.vacuum(dry_run=False)
                dt.cleanup_metadata()

        elif mode == 'ignore':
            try:
                DeltaTable(path)
                print(f"Table {normalized_table} exists. Skipping (mode='ignore')")
            except Exception:
                print(f"Table {normalized_table} doesn't exist. Creating...")
                self.con.sql(f"DROP VIEW IF EXISTS {normalized_table}")
                df = self.con.sql(sql).record_batch()
                
                write_args = _build_write_deltalake_args(
                    path, df, 'overwrite', 
                    schema_mode=schema_mode, 
                    partition_by=partition_by
                )
                write_deltalake(**write_args)
                
                self.con.sql(f"CREATE OR REPLACE VIEW {normalized_table} AS SELECT * FROM delta_scan('{path}')")
                dt = DeltaTable(path)
                dt.vacuum(dry_run=False)
                dt.cleanup_metadata()

        engine_info = f" (engine=rust, schema_mode=merge)" if schema_mode == 'merge' else " (engine=pyarrow)"
        partition_info = f" partitioned by {partition_by}" if partition_by else ""
        print(f"✅ SQL '{table}' → '{normalized_table}' ({mode}){engine_info}{partition_info}")
        return normalized_table

    def run(self, pipeline: List[Tuple]) -> bool:
        """
        Execute pipeline of tasks.
        
        Task formats:
            - Python: ('function_name', (arg1, arg2, ...))
            - SQL:    ('table_name', 'mode') or ('table_name', 'mode', {sql_params})
            - SQL with Delta options: ('table_name', 'mode', {sql_params}, {delta_options})
        
        Returns:
            True if all tasks succeeded
            False if any task failed (exception) or Python task returned 0 (early exit)
        """
        if self.sql_folder is None:
            raise RuntimeError("sql_folder is not configured. Cannot run pipelines.")
        
        for i, task in enumerate(pipeline, 1):
            print(f"\n{'='*60}")
            print(f"Task {i}/{len(pipeline)}: {task[0]}")
            print('='*60)
            
            try:
                result = None
                
                if len(task) == 2:
                    name, second = task
                    if isinstance(second, str) and second in {'overwrite', 'append', 'ignore'}:
                        result = self._run_sql(name, second, {}, {})
                    else:
                        args = second if isinstance(second, (tuple, list)) else (second,)
                        result = self._run_python(name, tuple(args))
                    
                elif len(task) == 3:
                    table, mode, params = task
                    if not isinstance(params, dict):
                        raise ValueError(f"Expected dict for params, got {type(params)}")
                    result = self._run_sql(table, mode, params, {})
                    
                elif len(task) == 4:
                    table, mode, params, delta_options = task
                    if not isinstance(params, dict):
                        raise ValueError(f"Expected dict for SQL params, got {type(params)}")
                    if not isinstance(delta_options, dict):
                        raise ValueError(f"Expected dict for Delta options, got {type(delta_options)}")
                    result = self._run_sql(table, mode, params, delta_options)
                    
                else:
                    raise ValueError(f"Invalid task format: {task}")
                
                # Check if Python task returned 0 (early exit condition)
                # Only check for Python tasks as SQL tasks return table names (strings) and only stop on exceptions
                if (len(task) == 2 and 
                    not isinstance(task[1], str) and 
                    result == 0):
                    print(f"\n⏹️  Python task {i} returned 0 - stopping pipeline execution")
                    print(f"   Remaining tasks ({len(pipeline) - i}) will not be executed")
                    return False
                    
            except Exception as e:
                print(f"\n❌ Task {i} failed: {e}")
                return False

        print(f"\n{'='*60}")
        print("✅ All tasks completed successfully")
        print('='*60)
        return True

    def copy(self, local_folder: str, remote_folder: str, 
             file_extensions: Optional[List[str]] = None, 
             overwrite: bool = False) -> bool:
        """
        Copy files from a local folder to OneLake Files section.
        
        Args:
            local_folder: Path to local folder containing files to upload
            remote_folder: Target subfolder path in OneLake Files (e.g., "reports/daily") - REQUIRED
            file_extensions: Optional list of file extensions to filter (e.g., ['.csv', '.parquet'])
            overwrite: Whether to overwrite existing files (default: False)
            
        Returns:
            True if all files uploaded successfully, False otherwise
            
        Examples:
            # Upload all files from local folder to a target folder
            dr.copy("./local_data", "uploaded_data")
            
            # Upload only CSV files to a specific subfolder
            dr.copy("./reports", "daily_reports", ['.csv'])
            
            # Upload with overwrite enabled
            dr.copy("./backup", "backups", overwrite=True)
        """
        if not os.path.exists(local_folder):
            print(f"❌ Local folder not found: {local_folder}")
            return False
            
        if not os.path.isdir(local_folder):
            print(f"❌ Path is not a directory: {local_folder}")
            return False
            
        # Get Azure token
        token = self._get_storage_token()
        if token == "PLACEHOLDER_TOKEN_TOKEN_NOT_AVAILABLE":
            print("Authenticating with Azure for file upload (trying CLI, will fallback to browser if needed)...")
            from azure.identity import AzureCliCredential, InteractiveBrowserCredential, ChainedTokenCredential
            credential = ChainedTokenCredential(AzureCliCredential(), InteractiveBrowserCredential())
            token_obj = credential.get_token("https://storage.azure.com/.default")
            token = token_obj.token
            os.environ["AZURE_STORAGE_TOKEN"] = token
        
        # Setup OneLake Files URL (not Tables)
        files_base_url = f'abfss://{self.workspace}@onelake.dfs.fabric.microsoft.com/{self.lakehouse_name}.Lakehouse/Files/'
        store = AzureStore.from_url(files_base_url, bearer_token=token)
        
        # Collect files to upload
        files_to_upload = []
        for root, dirs, files in os.walk(local_folder):
            for file in files:
                local_file_path = os.path.join(root, file)
                
                # Filter by extensions if specified
                if file_extensions:
                    _, ext = os.path.splitext(file)
                    if ext.lower() not in [e.lower() for e in file_extensions]:
                        continue
                
                # Calculate relative path from local_folder
                rel_path = os.path.relpath(local_file_path, local_folder)
                
                # Build remote path in OneLake Files (remote_folder is now mandatory)
                remote_path = f"{remote_folder.strip('/')}/{rel_path}".replace("\\", "/")
                
                files_to_upload.append((local_file_path, remote_path))
        
        if not files_to_upload:
            print(f"No files found to upload in {local_folder}")
            if file_extensions:
                print(f"  (filtered by extensions: {file_extensions})")
            return True
        
        print(f"📁 Uploading {len(files_to_upload)} files from '{local_folder}' to OneLake Files...")
        print(f"   Target folder: {remote_folder}")
        
        uploaded_count = 0
        failed_count = 0
        
        for local_path, remote_path in files_to_upload:
            try:
                # Check if file exists (if not overwriting)
                if not overwrite:
                    try:
                        obs.head(store, remote_path)
                        print(f"  ⏭ Skipped (exists): {remote_path}")
                        continue
                    except Exception:
                        # File doesn't exist, proceed with upload
                        pass
                
                # Read local file
                with open(local_path, 'rb') as f:
                    file_data = f.read()
                
                # Upload to OneLake Files
                obs.put(store, remote_path, file_data)
                
                file_size = len(file_data)
                size_mb = file_size / (1024 * 1024) if file_size > 1024*1024 else file_size / 1024
                size_unit = "MB" if file_size > 1024*1024 else "KB"
                
                print(f"  ✓ Uploaded: {local_path} → {remote_path} ({size_mb:.1f} {size_unit})")
                uploaded_count += 1
                
            except Exception as e:
                print(f"  ❌ Failed: {local_path} → {remote_path} | Error: {str(e)[:100]}")
                failed_count += 1
        
        print(f"\n{'='*60}")
        if failed_count == 0:
            print(f"✅ Successfully uploaded all {uploaded_count} files to OneLake Files")
        else:
            print(f"⚠ Uploaded {uploaded_count} files, {failed_count} failed")
        print(f"{'='*60}")
        
        return failed_count == 0

    def download(self, remote_folder: str = "", local_folder: str = "./downloaded_files",
                 file_extensions: Optional[List[str]] = None,
                 overwrite: bool = False) -> bool:
        """
        Download files from OneLake Files section to a local folder.
        
        Args:
            remote_folder: Optional subfolder path in OneLake Files to download from
            local_folder: Local folder path to download files to (default: "./downloaded_files")
            file_extensions: Optional list of file extensions to filter (e.g., ['.csv', '.parquet'])
            overwrite: Whether to overwrite existing local files (default: False)
            
        Returns:
            True if all files downloaded successfully, False otherwise
            
        Examples:
            # Download all files from OneLake Files root
            dr.download_from_files()
            
            # Download only CSV files from a specific subfolder
            dr.download_from_files("daily_reports", "./reports", ['.csv'])
        """
        # Get Azure token
        token = self._get_storage_token()
        if token == "PLACEHOLDER_TOKEN_TOKEN_NOT_AVAILABLE":
            print("Authenticating with Azure for file download (trying CLI, will fallback to browser if needed)...")
            from azure.identity import AzureCliCredential, InteractiveBrowserCredential, ChainedTokenCredential
            credential = ChainedTokenCredential(AzureCliCredential(), InteractiveBrowserCredential())
            token_obj = credential.get_token("https://storage.azure.com/.default")
            token = token_obj.token
            os.environ["AZURE_STORAGE_TOKEN"] = token
        
        # Setup OneLake Files URL (not Tables)
        files_base_url = f'abfss://{self.workspace}@onelake.dfs.fabric.microsoft.com/{self.lakehouse_name}.Lakehouse/Files/'
        store = AzureStore.from_url(files_base_url, bearer_token=token)
        
        # Create local directory
        os.makedirs(local_folder, exist_ok=True)
        
        # List files in OneLake Files
        print(f"📁 Discovering files in OneLake Files...")
        if remote_folder:
            print(f"   Source folder: {remote_folder}")
            prefix = f"{remote_folder.strip('/')}/"
        else:
            prefix = ""
        
        try:
            list_stream = obs.list(store, prefix=prefix)
            files_to_download = []
            
            for batch in list_stream:
                for obj in batch:
                    remote_path = obj["path"]
                    
                    # Filter by extensions if specified
                    if file_extensions:
                        _, ext = os.path.splitext(remote_path)
                        if ext.lower() not in [e.lower() for e in file_extensions]:
                            continue
                    
                    # Calculate local path
                    if remote_folder:
                        rel_path = os.path.relpath(remote_path, remote_folder.strip('/'))
                    else:
                        rel_path = remote_path
                    
                    local_path = os.path.join(local_folder, rel_path).replace('/', os.sep)
                    files_to_download.append((remote_path, local_path))
            
            if not files_to_download:
                print(f"No files found to download")
                if file_extensions:
                    print(f"  (filtered by extensions: {file_extensions})")
                return True
            
            print(f"📥 Downloading {len(files_to_download)} files to '{local_folder}'...")
            
            downloaded_count = 0
            failed_count = 0
            
            for remote_path, local_path in files_to_download:
                try:
                    # Check if local file exists (if not overwriting)
                    if not overwrite and os.path.exists(local_path):
                        print(f"  ⏭ Skipped (exists): {local_path}")
                        continue
                    
                    # Ensure local directory exists
                    os.makedirs(os.path.dirname(local_path), exist_ok=True)
                    
                    # Download file
                    data = obs.get(store, remote_path).bytes()
                    
                    # Write to local file
                    with open(local_path, 'wb') as f:
                        f.write(data)
                    
                    file_size = len(data)
                    size_mb = file_size / (1024 * 1024) if file_size > 1024*1024 else file_size / 1024
                    size_unit = "MB" if file_size > 1024*1024 else "KB"
                    
                    print(f"  ✓ Downloaded: {remote_path} → {local_path} ({size_mb:.1f} {size_unit})")
                    downloaded_count += 1
                    
                except Exception as e:
                    print(f"  ❌ Failed: {remote_path} → {local_path} | Error: {str(e)[:100]}")
                    failed_count += 1
            
            print(f"\n{'='*60}")
            if failed_count == 0:
                print(f"✅ Successfully downloaded all {downloaded_count} files from OneLake Files")
            else:
                print(f"⚠ Downloaded {downloaded_count} files, {failed_count} failed")
            print(f"{'='*60}")
            
            return failed_count == 0
            
        except Exception as e:
            print(f"❌ Error listing files from OneLake: {e}")
            return False

    def sql(self, query: str):
        """
        Execute raw SQL query with Spark-style write API.
        
        Example:
            dr.sql("SELECT * FROM table").show()
            df = dr.sql("SELECT * FROM table").df()
            dr.sql("SELECT 43 as value").write.mode("append").saveAsTable("test")
        """
        relation = self.con.sql(query)
        return QueryResult(relation, self)

    def get_connection(self):
        """Get underlying DuckDB connection"""
        return self.con

    def close(self):
        """Close DuckDB connection"""
        if self.con:
            self.con.close()
            print("Connection closed")