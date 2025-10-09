import duckdb
import requests
import os
import importlib.util
from deltalake import DeltaTable, write_deltalake
from typing import List, Tuple, Union, Optional, Callable, Dict, Any
from string import Template
import obstore as obs
from obstore.store import AzureStore
from datetime import datetime
from .stats import get_stats as _get_stats
from .runner import run as _run
from .files import copy as _copy, download as _download
from .writer import QueryResult

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
                 scan_all_schemas: bool = False, storage_account: str = "onelake"):
        self.workspace = workspace
        self.lakehouse_name = lakehouse_name
        self.schema = schema
        self.sql_folder = sql_folder.strip() if sql_folder else None
        self.compaction_threshold = compaction_threshold
        self.scan_all_schemas = scan_all_schemas
        self.storage_account = storage_account
        self.table_base_url = f'abfss://{workspace}@{storage_account}.dfs.fabric.microsoft.com/{lakehouse_name}.Lakehouse/Tables/'
        self.con = duckdb.connect()
        self.con.sql("SET preserve_insertion_order = false")
        self._attach_lakehouse()

    @classmethod
    def connect(cls, connection_string: str, sql_folder: Optional[str] = None,
                compaction_threshold: int = 100, storage_account: str = "onelake"):
        """
        Create and connect to lakehouse.
        
        Uses compact format: connect("ws/lh.lakehouse/schema") or connect("ws/lh.lakehouse")
        
        Args:
            connection_string: OneLake path "ws/lh.lakehouse/schema" or "ws/lh.lakehouse"
            sql_folder: Optional path or URL to SQL files folder
            compaction_threshold: File count threshold for compaction
            storage_account: Storage account name (default: "onelake")
        
        Examples:
            dr = Duckrun.connect("ws/lh.lakehouse/schema", sql_folder="./sql")
            dr = Duckrun.connect("ws/lh.lakehouse/schema")  # no SQL folder
            dr = Duckrun.connect("ws/lh.lakehouse")  # defaults to dbo schema
            dr = Duckrun.connect("ws/lh.lakehouse", storage_account="xxx-onelake")  # custom storage
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
        
        return cls(workspace, lakehouse_name, schema, sql_folder, compaction_threshold, scan_all_schemas, storage_account)

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
        
        url = f"abfss://{self.workspace}@{self.storage_account}.dfs.fabric.microsoft.com/"
        store = AzureStore.from_url(url, bearer_token=token)
        
        base_path = f"{self.lakehouse_name}.Lakehouse/Tables/"
        tables_found = []
        
        if self.scan_all_schemas:
            # Discover all schemas first
            schemas_result = obs.list_with_delimiter(store, prefix=base_path)
            schemas = [
                prefix.rstrip('/').split('/')[-1] 
                for prefix in schemas_result['common_prefixes']
            ]
            
            # Discover tables in each schema
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
            
            # Group tables by schema for display
            schema_tables = {}
            for schema_name, table_name in tables:
                if schema_name not in schema_tables:
                    schema_tables[schema_name] = []
                schema_tables[schema_name].append(table_name)
            
            # Display tables by schema
            print(f"\n📊 Found {len(tables)} tables:")
            for schema_name in sorted(schema_tables.keys()):
                table_list = sorted(schema_tables[schema_name])
                print(f"   {schema_name}: {', '.join(table_list)}")
            
            attached_count = 0
            skipped_tables = []
            
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
                    attached_count += 1
                except Exception as e:
                    skipped_tables.append(f"{schema_name}.{table_name}")
                    continue
            
            print(f"\n{'='*60}")
            print(f"✅ Ready - {attached_count}/{len(tables)} tables available")
            if skipped_tables:
                print(f"⚠ Skipped {len(skipped_tables)} tables: {', '.join(skipped_tables[:3])}{'...' if len(skipped_tables) > 3 else ''}")
            print(f"{'='*60}\n")
                
        except Exception as e:
            print(f"❌ Error attaching lakehouse: {e}")
            print("Continuing without pre-attached tables.")

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
        return _run(self, pipeline)

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
        return _copy(self, local_folder, remote_folder, file_extensions, overwrite)

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
            dr.download()
            
            # Download only CSV files from a specific subfolder
            dr.download("daily_reports", "./reports", ['.csv'])
        """
        return _download(self, remote_folder, local_folder, file_extensions, overwrite)

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

    def get_stats(self, source: str):
        """
        Get comprehensive statistics for Delta Lake tables.
        
        Args:
            source: Can be one of:
                   - Table name: 'table_name' (uses current schema)
                   - Schema.table: 'schema.table_name' (specific table in schema)
                   - Schema only: 'schema' (all tables in schema)
        
        Returns:
            Arrow table with statistics including total rows, file count, row groups, 
            average row group size, file sizes, VORDER status, and timestamp
        
        Examples:
            con = duckrun.connect("tmp/data.lakehouse/aemo")
            
            # Single table in current schema
            stats = con.get_stats('price')
            
            # Specific table in different schema
            stats = con.get_stats('aemo.price')
            
            # All tables in a schema
            stats = con.get_stats('aemo')
        """
        return _get_stats(self, source)

    def close(self):
        """Close DuckDB connection"""
        if self.con:
            self.con.close()
            print("Connection closed")