import duckdb
import requests
import os
import importlib.util
from deltalake import DeltaTable, write_deltalake
from typing import List, Tuple, Union, Optional, Callable, Dict, Any
from string import Template


class DeltaWriter:
    """Spark-style write API for Delta Lake"""
    
    def __init__(self, relation, duckrun_instance):
        self.relation = relation
        self.duckrun = duckrun_instance
        self._format = "delta"  # Default to delta format
        self._mode = "overwrite"
    
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
    
    def saveAsTable(self, table_name: str):
        """Save query result as Delta table"""
        # Format defaults to "delta", so no need to check
        if self._format != "delta":
            raise RuntimeError(f"Only 'delta' format is supported, got '{self._format}'")
        
        # Parse schema.table or use default schema
        if "." in table_name:
            schema, table = table_name.split(".", 1)
        else:
            schema = self.duckrun.schema
            table = table_name
        
        # Ensure OneLake secret is created
        self.duckrun._create_onelake_secret()
        
        # Build path
        path = f"{self.duckrun.table_base_url}{schema}/{table}"
        
        # Execute query and get result
        df = self.relation.record_batch()
        
        print(f"Writing to Delta table: {schema}.{table} (mode={self._mode})")
        
        # Write to Delta
        write_deltalake(path, df, mode=self._mode)
        
        # Create or replace view in DuckDB
        self.duckrun.con.sql(f"DROP VIEW IF EXISTS {table}")
        self.duckrun.con.sql(f"""
            CREATE OR REPLACE VIEW {table}
            AS SELECT * FROM delta_scan('{path}')
        """)
        
        # Optimize if needed
        dt = DeltaTable(path)
        
        if self._mode == "overwrite":
            dt.vacuum(retention_hours=0, dry_run=False, enforce_retention_duration=False)
            dt.cleanup_metadata()
            print(f"✅ Table {schema}.{table} created/overwritten")
        else:  # append
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
    def connect(cls, workspace: Union[str, None] = None, lakehouse_name: Optional[str] = None, 
                schema: str = "dbo", sql_folder: Optional[str] = None, 
                compaction_threshold: int = 100):
        """
        Create and connect to lakehouse.
        
        Supports two formats:
        1. Compact: connect("ws/lh.lakehouse/schema") or connect("ws/lh.lakehouse")
        2. Traditional: connect("ws", "lh", "schema") or connect("ws", "lh")
        
        Schema defaults to "dbo" if not specified. When no schema is provided,
        all tables across all schemas will be listed, but operations will use "dbo".
        
        Examples:
            dr = Duckrun.connect("myworkspace/mylakehouse.lakehouse/bronze")
            dr = Duckrun.connect("myworkspace/mylakehouse.lakehouse")  # lists all, uses dbo
            dr = Duckrun.connect("myworkspace", "mylakehouse", "bronze")
            dr = Duckrun.connect("myworkspace", "mylakehouse")  # lists all, uses dbo
            dr = Duckrun.connect("ws/lh.lakehouse", sql_folder="./sql")
        """
        print("Connecting to Lakehouse...")
        
        scan_all_schemas = False
        
        # Check if using compact format: "ws/lh.lakehouse/schema" or "ws/lh.lakehouse"
        if workspace and "/" in workspace and lakehouse_name is None:
            parts = workspace.split("/")
            if len(parts) == 2:
                # Format: "ws/lh.lakehouse" (schema will use default)
                workspace, lakehouse_name = parts
                scan_all_schemas = True
                print(f"ℹ️  No schema specified. Using default schema 'dbo' for operations.")
                print(f"   Scanning all schemas for table discovery...")
                print(f"   ⚠️  WARNING: Scanning all schemas can be slow for large lakehouses!")
                print(f"   💡 For better performance, specify a schema: {workspace}/{lakehouse_name}.lakehouse/schema\n")
            elif len(parts) == 3:
                # Format: "ws/lh.lakehouse/schema"
                workspace, lakehouse_name, schema = parts
            else:
                raise ValueError(
                    f"Invalid connection string format: '{workspace}'. "
                    "Expected format: 'workspace/lakehouse.lakehouse' or 'workspace/lakehouse.lakehouse/schema'"
                )
            
            # Remove .lakehouse suffix if present
            if lakehouse_name.endswith(".lakehouse"):
                lakehouse_name = lakehouse_name[:-10]
        elif lakehouse_name is not None:
            # Traditional format used, check if schema was explicitly provided
            # If schema is still "dbo" (default), scan all schemas
            if schema == "dbo":
                scan_all_schemas = True
                print(f"ℹ️  No schema specified. Using default schema 'dbo' for operations.")
                print(f"   Scanning all schemas for table discovery...")
                print(f"   ⚠️  WARNING: Scanning all schemas can be slow for large lakehouses!")
                print(f"   💡 For better performance, specify a schema explicitly.\n")
        
        # Validate all required parameters are present
        if not workspace or not lakehouse_name:
            raise ValueError(
                "Missing required parameters. Use either:\n"
                "  connect('workspace/lakehouse.lakehouse/schema')\n"
                "  connect('workspace/lakehouse.lakehouse')  # defaults to dbo, lists all\n"
                "  connect('workspace', 'lakehouse', 'schema')\n"
                "  connect('workspace', 'lakehouse')  # defaults to dbo, lists all"
            )
        
        return cls(workspace, lakehouse_name, schema, sql_folder, compaction_threshold, scan_all_schemas)

    def _get_storage_token(self):
        return os.environ.get("AZURE_STORAGE_TOKEN", "PLACEHOLDER_TOKEN_TOKEN_NOT_AVAILABLE")

    def _create_onelake_secret(self):
        token = self._get_storage_token()
        if token != "PLACEHOLDER_TOKEN_TOKEN_NOT_AVAILABLE":
            self.con.sql(f"CREATE OR REPLACE SECRET onelake (TYPE AZURE, PROVIDER ACCESS_TOKEN, ACCESS_TOKEN '{token}')")
        else:
            print("Please login to Azure CLI")
            from azure.identity import AzureCliCredential, InteractiveBrowserCredential, ChainedTokenCredential
            credential = ChainedTokenCredential(AzureCliCredential(), InteractiveBrowserCredential())
            token = credential.get_token("https://storage.azure.com/.default")
            os.environ["AZURE_STORAGE_TOKEN"] = token.token
            self.con.sql("CREATE OR REPLACE PERSISTENT SECRET onelake (TYPE azure, PROVIDER credential_chain, CHAIN 'cli', ACCOUNT_NAME 'onelake')")

    def _attach_lakehouse(self):
        self._create_onelake_secret()
        try:
            if self.scan_all_schemas:
                # Scan all schemas
                print(f"⚠️  Scanning for Delta tables across all schemas...")
                print(f"   This may take a while for large lakehouses with many schemas/tables.")
                
                list_tables_query = f"""
                    SELECT DISTINCT
                        regexp_extract(file, 'Tables/([^/]+)/([^/]+)/_delta_log', 1) as schema_name,
                        regexp_extract(file, 'Tables/([^/]+)/([^/]+)/_delta_log', 2) as table_name
                    FROM glob("abfss://{self.workspace}@onelake.dfs.fabric.microsoft.com/{self.lakehouse_name}.Lakehouse/Tables/**")
                    WHERE file LIKE '%/_delta_log/%'
                      AND file NOT LIKE '%/metadata/%'
                      AND file NOT LIKE '%/iceberg/%'
                      AND regexp_extract(file, 'Tables/([^/]+)/([^/]+)/_delta_log', 1) IS NOT NULL
                      AND regexp_extract(file, 'Tables/([^/]+)/([^/]+)/_delta_log', 2) IS NOT NULL
                    ORDER BY schema_name, table_name
                """
            else:
                # Scan specific schema only
                print(f"Scanning for Delta tables in {self.schema}... (this may take a moment)")
                
                list_tables_query = f"""
                    SELECT DISTINCT
                        '{self.schema}' as schema_name,
                        regexp_extract(file, 'Tables/{self.schema}/([^/]+)/_delta_log', 1) as table_name
                    FROM glob("abfss://{self.workspace}@onelake.dfs.fabric.microsoft.com/{self.lakehouse_name}.Lakehouse/Tables/{self.schema}/**")
                    WHERE file LIKE '%/_delta_log/%'
                      AND file NOT LIKE '%/metadata/%'
                      AND file NOT LIKE '%/iceberg/%'
                      AND regexp_extract(file, 'Tables/{self.schema}/([^/]+)/_delta_log', 1) IS NOT NULL
                """
            
            list_tables_df = self.con.sql(list_tables_query).df()
            
            if list_tables_df.empty:
                if self.scan_all_schemas:
                    print(f"No Delta tables found in {self.lakehouse_name}.Lakehouse/Tables/")
                else:
                    print(f"No Delta tables found in {self.lakehouse_name}.Lakehouse/Tables/{self.schema}/")
                return
            
            print(f"Found {len(list_tables_df)} Delta tables. Attaching as views...\n")

            for _, row in list_tables_df.iterrows():
                schema_name = row['schema_name']
                table_name = row['table_name']
                
                # Skip Iceberg-related folders and empty names
                if not table_name or table_name in ('metadata', 'iceberg'):
                    continue
                
                try:
                    # Create view with schema prefix to avoid conflicts
                    view_name = f"{schema_name}_{table_name}" if self.scan_all_schemas else table_name
                    
                    self.con.sql(f"""
                        CREATE OR REPLACE VIEW {view_name}
                        AS SELECT * FROM delta_scan('{self.table_base_url}{schema_name}/{table_name}');
                    """)
                    print(f"  ✓ Attached: {schema_name}.{table_name} → {view_name}")
                except Exception as e:
                    print(f"  ⚠ Skipped {schema_name}.{table_name}: {str(e)[:100]}")
                    continue
            
            print("\nAttached tables (views) in DuckDB:")
            self.con.sql("SELECT name FROM (SHOW ALL TABLES) WHERE database='memory' ORDER BY name").show()
            
            if self.scan_all_schemas:
                print(f"\nNote: Tables are prefixed with schema (e.g., dbo_tablename)")
                print(f"      Default schema for operations: {self.schema}")
                
        except Exception as e:
            print(f"Error attaching lakehouse: {e}")
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

        # Auto-inject common params, merge with user params
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

    def _run_sql(self, table: str, mode: str, params: Dict) -> str:
        """Execute SQL task, write to Delta, return normalized table name"""
        self._create_onelake_secret()
        
        if mode not in {'overwrite', 'append', 'ignore'}:
            raise ValueError(f"Invalid mode '{mode}'. Use: overwrite, append, or ignore")

        sql = self._read_sql_file(table, params)
        if sql is None:
            raise RuntimeError(f"Failed to read SQL file for '{table}'")

        normalized_table = self._normalize_table_name(table)
        path = f"{self.table_base_url}{self.schema}/{normalized_table}"

        if mode == 'overwrite':
            self.con.sql(f"DROP VIEW IF EXISTS {normalized_table}")
            df = self.con.sql(sql).record_batch()
            write_deltalake(path, df, mode='overwrite')
            self.con.sql(f"CREATE OR REPLACE VIEW {normalized_table} AS SELECT * FROM delta_scan('{path}')")
            dt = DeltaTable(path)
            dt.vacuum(retention_hours=0, dry_run=False, enforce_retention_duration=False)
            dt.cleanup_metadata()

        elif mode == 'append':
            df = self.con.sql(sql).record_batch()
            write_deltalake(path, df, mode='append')
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
                write_deltalake(path, df, mode='overwrite')
                self.con.sql(f"CREATE OR REPLACE VIEW {normalized_table} AS SELECT * FROM delta_scan('{path}')")
                dt = DeltaTable(path)
                dt.vacuum(dry_run=False)
                dt.cleanup_metadata()

        print(f"✅ SQL '{table}' → '{normalized_table}' ({mode})")
        return normalized_table

    def run(self, pipeline: List[Tuple]) -> bool:
        """
        Execute pipeline of tasks.
        
        Task formats:
            - Python: ('function_name', (arg1, arg2, ...))
            - SQL:    ('table_name', 'mode') or ('table_name', 'mode', {params})
        
        Returns:
            True if all tasks succeeded
            
        Example:
            pipeline = [
                ('download', (urls, paths, depth)),
                ('staging', 'overwrite', {'run_date': '2024-06-01'}),
                ('transform', 'append'),  # {} optional!
                ('calendar', 'ignore')     # {} optional!
            ]
            dr.run(pipeline)
        """
        if self.sql_folder is None:
            raise RuntimeError("sql_folder is not configured. Cannot run pipelines. Set sql_folder when creating connection.")
        
        for i, task in enumerate(pipeline, 1):
            print(f"\n{'='*60}")
            print(f"Task {i}/{len(pipeline)}: {task[0]}")
            print('='*60)
            
            try:
                if len(task) == 2:
                    # Could be Python: ('name', (args,)) or SQL: ('table', 'mode')
                    name, second = task
                    if isinstance(second, str) and second in {'overwrite', 'append', 'ignore'}:
                        # SQL task without params: ('table', 'mode')
                        self._run_sql(name, second, {})
                    else:
                        # Python task: ('name', (args,))
                        args = second if isinstance(second, (tuple, list)) else (second,)
                        self._run_python(name, tuple(args))
                    
                elif len(task) == 3:
                    # SQL task with params: ('table', 'mode', {params})
                    table, mode, params = task
                    if not isinstance(params, dict):
                        raise ValueError(f"Expected dict for params, got {type(params)}")
                    self._run_sql(table, mode, params)
                    
                else:
                    raise ValueError(f"Invalid task format: {task}")
                    
            except Exception as e:
                print(f"\n❌ Task {i} failed: {e}")
                return False

        print(f"\n{'='*60}")
        print("✅ All tasks completed successfully")
        print('='*60)
        return True

    def sql(self, query: str):
        """
        Execute raw SQL query with Spark-style write API.
        
        Example:
            # Traditional DuckDB style
            dr.sql("SELECT * FROM table").show()
            df = dr.sql("SELECT * FROM table").df()
            
            # New Spark-style write API (format is optional, defaults to delta)
            dr.sql("SELECT 43 as value").write.mode("append").saveAsTable("test")
            dr.sql("SELECT * FROM source").write.mode("overwrite").saveAsTable("target")
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