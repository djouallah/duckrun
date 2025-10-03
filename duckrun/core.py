import duckdb
import requests
import os
import importlib.util
from deltalake import DeltaTable, write_deltalake
from typing import List, Tuple, Union, Optional, Callable, Dict, Any
from string import Template

class Duckrun:
    """
    Lakehouse task runner with clean tuple-based API.
    Powered by DuckDB for fast data processing.
    
    Task formats:
        Python: ('function_name', (arg1, arg2, ...))
        SQL:    ('table_name', 'mode', {params})
    
    Usage:
        dr = Duckrun.connect(workspace, lakehouse, schema, sql_folder)
        
        pipeline = [
            ('download', (urls, paths, depth)),
            ('staging', 'overwrite', {'run_date': '2024-06-01'}),
            ('transform', 'append')
        ]
        
        dr.run(pipeline)
    """

    def __init__(self, workspace: str, lakehouse_name: str, schema: str, 
                 sql_folder: str, compaction_threshold: int = 10):
        self.workspace = workspace
        self.lakehouse_name = lakehouse_name
        self.schema = schema
        self.sql_folder = sql_folder.strip()
        self.compaction_threshold = compaction_threshold
        self.table_base_url = f'abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{lakehouse_name}.Lakehouse/Tables/'
        self.con = duckdb.connect()
        self.con.sql("SET preserve_insertion_order = false")
        self._attach_lakehouse()

    @classmethod
    def connect(cls, workspace: str, lakehouse_name: str, schema: str, 
                sql_folder: str, compaction_threshold: int = 10):
        """Create and connect to lakehouse"""
        print("Connecting to Lakehouse...")
        return cls(workspace, lakehouse_name, schema, sql_folder, compaction_threshold)

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
            list_tables_query = f"""
                SELECT DISTINCT(split_part(file, '_delta_log', 1)) as tables
                FROM glob ("abfss://{self.workspace}@onelake.dfs.fabric.microsoft.com/{self.lakehouse_name}.Lakehouse/Tables/*/*/_delta_log/*.json")
            """
            list_tables_df = self.con.sql(list_tables_query).df()
            list_tables = list_tables_df['tables'].tolist() if not list_tables_df.empty else []

            if not list_tables:
                print(f"No Delta tables found in {self.lakehouse_name}.Lakehouse/Tables.")
                return

            print(f"Found {len(list_tables)} Delta tables. Attaching as views...")

            for table_path in list_tables:
                parts = table_path.strip("/").split("/")
                if len(parts) >= 2:
                    potential_schema = parts[-2]
                    table = parts[-1]
                    if potential_schema == self.schema:
                        try:
                            self.con.sql(f"""
                                CREATE OR REPLACE VIEW {table}
                                AS SELECT * FROM delta_scan('{self.table_base_url}{self.schema}/{table}');
                            """)
                        except Exception as e:
                            print(f"Error creating view for table {table}: {e}")
            print("\nAttached tables (views) in DuckDB:")
            self.con.sql("SELECT name FROM (SHOW ALL TABLES) WHERE database='memory'").show()
        except Exception as e:
            print(f"Error attaching lakehouse: {e}")

    def _normalize_table_name(self, name: str) -> str:
        """Extract base table name before first '__'"""
        return name.split('__', 1)[0] if '__' in name else name

    def _read_sql_file(self, table_name: str, params: Optional[Dict] = None) -> Optional[str]:
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
        Execute raw SQL query.
        
        Example:
            dr.sql("SELECT * FROM table").show()
            df = dr.sql("SELECT * FROM table").df()
        """
        return self.con.sql(query)

    def get_connection(self):
        """Get underlying DuckDB connection"""
        return self.con

    def close(self):
        """Close DuckDB connection"""
        if self.con:
            self.con.close()
            print("Connection closed")