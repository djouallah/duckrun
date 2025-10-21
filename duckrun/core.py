import duckdb
import requests
import os
import importlib.util
import json
import time
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

    def __init__(self, workspace_id: str, lakehouse_id: str, schema: str = "dbo", 
                 sql_folder: Optional[str] = None, compaction_threshold: int = 10,
                 scan_all_schemas: bool = False, storage_account: str = "onelake"):
        # Store GUIDs for internal use
        self.workspace_id = workspace_id
        self.lakehouse_id = lakehouse_id
        self.schema = schema
        self.sql_folder = sql_folder.strip() if sql_folder else None
        self.compaction_threshold = compaction_threshold
        self.scan_all_schemas = scan_all_schemas
        self.storage_account = storage_account
        
        # Construct proper ABFSS URLs
        import re
        guid_pattern = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.IGNORECASE)
        # If lakehouse_id is a GUID, use as-is
        if guid_pattern.match(lakehouse_id):
            lakehouse_url_part = lakehouse_id
        else:
            # If workspace name has no spaces, always append .lakehouse unless already present
            if " " not in workspace_id and not lakehouse_id.endswith('.lakehouse'):
                lakehouse_url_part = f'{lakehouse_id}.lakehouse'
            else:
                lakehouse_url_part = lakehouse_id
        self.table_base_url = f'abfss://{workspace_id}@{storage_account}.dfs.fabric.microsoft.com/{lakehouse_url_part}/Tables/'
        self.files_base_url = f'abfss://{workspace_id}@{storage_account}.dfs.fabric.microsoft.com/{lakehouse_url_part}/Files/'
        
        # Keep legacy properties for backward compatibility
        self.workspace = workspace_id  
        self.lakehouse_name = lakehouse_id
        
        self.con = duckdb.connect()
        self.con.sql("SET preserve_insertion_order = false")
        
        # Configure Azure transport for Colab (fixes SSL cert issues)
        try:
            import google.colab  # type: ignore
            self.con.sql("SET azure_transport_option_type = 'curl'")
            print("🔧 Colab detected - using curl transport for Azure")
        except ImportError:
            pass  # Not in Colab, use default transport
        
        self._attach_lakehouse()
        self._register_lookup_functions()

    @classmethod
    def connect(cls, connection_string: str, sql_folder: Optional[str] = None,
                compaction_threshold: int = 100, storage_account: str = "onelake"):
        """
        Create and connect to lakehouse or workspace.
        
        Smart detection based on connection string format:
        - "workspace" → workspace management only
        - "ws/lh.lakehouse/schema" → full lakehouse connection  
        - "ws/lh.lakehouse" → lakehouse connection (defaults to dbo schema)
        
        Args:
            connection_string: OneLake path or workspace name
            sql_folder: Optional path or URL to SQL files folder  
            compaction_threshold: File count threshold for compaction
            storage_account: Storage account name (default: "onelake")
        
        Examples:
            # Workspace management only (supports spaces in names)
            ws = Duckrun.connect("My Workspace Name")
            ws.list_lakehouses()
            ws.create_lakehouse_if_not_exists("New Lakehouse")
            
            # Full lakehouse connections (supports spaces in names)
            dr = Duckrun.connect("My Workspace/My Lakehouse.lakehouse/schema", sql_folder="./sql")
            dr = Duckrun.connect("Data Workspace/Sales Data.lakehouse/analytics")  # spaces supported
            dr = Duckrun.connect("My Workspace/My Lakehouse.lakehouse")  # defaults to dbo schema
            dr = Duckrun.connect("workspace/lakehouse.lakehouse", storage_account="xxx-onelake")  # custom storage
            
        Note:
            Internally resolves friendly names (with spaces) to GUIDs and constructs proper ABFSS URLs:
            "My Workspace/My Lakehouse.lakehouse/schema" becomes
            "abfss://workspace_guid@onelake.dfs.fabric.microsoft.com/lakehouse_guid/Tables/schema"
        """
        
        # Check if it's a workspace-only connection (no "/" means workspace name only)
        if "/" not in connection_string:
            return WorkspaceConnection(connection_string)
        
        scan_all_schemas = False
        
        # Parse lakehouse connection string: "ws/lh.lakehouse/schema" or "ws/lh.lakehouse"  
        # Support workspace and lakehouse names with spaces
        parts = connection_string.split("/")
        if len(parts) == 2:
            workspace_name, lakehouse_name = parts
            scan_all_schemas = True
            schema = "dbo"
        elif len(parts) == 3:
            workspace_name, lakehouse_name, schema = parts
        else:
            raise ValueError(
                f"Invalid connection string format: '{connection_string}'. "
                "Expected formats:\n"
                "  'workspace name' (workspace management only)\n"
                "  'workspace name/lakehouse name.lakehouse' (lakehouse with dbo schema)\n"
                "  'workspace name/lakehouse name.lakehouse/schema' (lakehouse with specific schema)"
            )
        
        if lakehouse_name.endswith(".lakehouse"):
            lakehouse_name = lakehouse_name[:-10]
        
        if not workspace_name or not lakehouse_name:
            raise ValueError(
                "Missing required parameters. Use one of these formats:\n"
                "  connect('workspace name')  # workspace management\n"
                "  connect('workspace name/lakehouse name.lakehouse/schema')  # full lakehouse\n"
                "  connect('workspace name/lakehouse name.lakehouse')  # defaults to dbo"
            )
        
        # Resolve friendly names to GUIDs and construct proper ABFSS path
        workspace_id, lakehouse_id = cls._resolve_names_to_guids(workspace_name, lakehouse_name)
        
        return cls(workspace_id, lakehouse_id, schema, sql_folder, compaction_threshold, scan_all_schemas, storage_account)

    @classmethod
    def _resolve_names_to_guids(cls, workspace_name: str, lakehouse_name: str) -> tuple[str, str]:
        """
        Resolve friendly workspace and lakehouse names to their GUIDs.
        
        Optimization: If names don't contain spaces, use them directly (no API calls needed).
        Only resolve to GUIDs when names contain spaces or are already GUIDs.
        
        Args:
            workspace_name: Display name of the workspace (can contain spaces)
            lakehouse_name: Display name of the lakehouse (can contain spaces)
            
        Returns:
            Tuple of (workspace_id, lakehouse_id) - either resolved GUIDs or original names
        """
        
        # Check if names are already GUIDs first
        import re
        guid_pattern = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.IGNORECASE)
        
        if guid_pattern.match(workspace_name) and guid_pattern.match(lakehouse_name):
            return workspace_name, lakehouse_name
        
        # Optimization: If workspace name has no spaces, use both names directly (old behavior)
        # Note: Lakehouse names cannot contain spaces in Microsoft Fabric, only workspace names can
        if " " not in workspace_name:
            return workspace_name, lakehouse_name
        
        # Workspace name contains spaces - need to resolve both to GUIDs for proper ABFSS URLs
        
        try:
            # Get authentication token using enhanced auth system
            from .auth import get_fabric_api_token
            token = get_fabric_api_token()
            if not token:
                raise ValueError("Failed to obtain Fabric API token")
                
            # Try to get current workspace ID if in notebook environment
            current_workspace_id = None
            try:
                import notebookutils  # type: ignore
                current_workspace_id = notebookutils.runtime.context.get("workspaceId")
            except ImportError:
                pass  # Not in notebook environment
            
            # Resolve workspace name to ID 
            if current_workspace_id:
                # In notebook environment, we could use current workspace ID
                # but we should validate it matches the requested workspace name
                workspace_id = cls._resolve_workspace_id_by_name(token, workspace_name)
                if not workspace_id:
                    # Fallback to current workspace if name resolution fails
                    print(f"⚠️ Could not validate workspace name '{workspace_name}', using current workspace")
                    workspace_id = current_workspace_id
            else:
                # External environment - must resolve by name
                workspace_id = cls._resolve_workspace_id_by_name(token, workspace_name)
                if not workspace_id:
                    raise ValueError(f"Workspace '{workspace_name}' not found")
            
            # Resolve lakehouse name to ID (required for ABFSS URLs with spaces)
            lakehouse_id = cls._resolve_lakehouse_id_by_name(token, workspace_id, lakehouse_name)
            if not lakehouse_id:
                raise ValueError(f"Lakehouse '{lakehouse_name}' not found in workspace '{workspace_name}'")
            
            return workspace_id, lakehouse_id
            
        except Exception as e:
            print(f"❌ Failed to resolve names to GUIDs: {e}")
            print(f"❌ Cannot use friendly names with spaces '{workspace_name}'/'{lakehouse_name}' in ABFSS URLs without GUID resolution")
            print("❌ Microsoft Fabric requires actual workspace and lakehouse GUIDs for ABFSS access when names contain spaces")
            raise ValueError(
                f"Unable to resolve workspace '{workspace_name}' and lakehouse '{lakehouse_name}' to GUIDs. "
                f"ABFSS URLs require actual GUIDs when names contain spaces. "
                f"Please ensure you have proper authentication and the workspace/lakehouse names are correct."
            )

    @classmethod  
    def _resolve_workspace_id_by_name(cls, token: str, workspace_name: str) -> Optional[str]:
        """Get workspace ID from display name"""
        try:
            import requests
            url = "https://api.fabric.microsoft.com/v1/workspaces"
            headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
            
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            workspaces = response.json().get("value", [])
            for workspace in workspaces:
                if workspace.get("displayName") == workspace_name:
                    return workspace.get("id")
            
            return None
        except Exception:
            return None
    
    @classmethod
    def _resolve_lakehouse_id_by_name(cls, token: str, workspace_id: str, lakehouse_name: str) -> Optional[str]:
        """Get lakehouse ID from display name within a workspace"""
        try:
            import requests
            url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/lakehouses"
            headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
            
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            lakehouses = response.json().get("value", [])
            for lakehouse in lakehouses:
                if lakehouse.get("displayName") == lakehouse_name:
                    return lakehouse.get("id")
            
            return None
        except Exception:
            return None

    @classmethod
    def connect_workspace(cls, workspace_name: str):
        """
        Connect to a workspace without a specific lakehouse.
        Used for lakehouse management operations.
        
        Args:
            workspace_name: Name of the workspace
            
        Returns:
            WorkspaceConnection object with lakehouse management methods
            
        Example:
            con = duckrun.connect_workspace("MyWorkspace")
            con.list_lakehouses()
            con.create_lakehouse_if_not_exists("newlakehouse")
        """
        return WorkspaceConnection(workspace_name)

    def _get_storage_token(self):
        from .auth import get_storage_token
        return get_storage_token()

    def _create_onelake_secret(self):
        token = self._get_storage_token()
        if token != "PLACEHOLDER_TOKEN_TOKEN_NOT_AVAILABLE":
            self.con.sql(f"CREATE OR REPLACE SECRET onelake (TYPE AZURE, PROVIDER ACCESS_TOKEN, ACCESS_TOKEN '{token}')")
        else:
            # Enhanced authentication - try all methods
            from .auth import get_token
            token = get_token()
            if token:
                os.environ["AZURE_STORAGE_TOKEN"] = token
                self.con.sql(f"CREATE OR REPLACE SECRET onelake (TYPE AZURE, PROVIDER ACCESS_TOKEN, ACCESS_TOKEN '{token}')")
            else:
                # Final fallback to persistent secret
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
            print("Authenticating with Azure for table discovery (detecting environment automatically)...")
            from .auth import get_token
            token = get_token()
            if not token:
                print("❌ Failed to authenticate for table discovery")
                return []
        
        url = f"abfss://{self.workspace}@{self.storage_account}.dfs.fabric.microsoft.com/"
        store = AzureStore.from_url(url, bearer_token=token)
        
        # Use the same lakehouse URL part logic as in __init__ to ensure .lakehouse suffix is added when needed
        import re
        guid_pattern = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.IGNORECASE)
        if guid_pattern.match(self.lakehouse_id):
            lakehouse_url_part = self.lakehouse_id
        else:
            # If workspace name has no spaces, always append .lakehouse unless already present
            if " " not in self.workspace_id and not self.lakehouse_id.endswith('.lakehouse'):
                lakehouse_url_part = f'{self.lakehouse_id}.lakehouse'
            else:
                lakehouse_url_part = self.lakehouse_id
        
        base_path = f"{lakehouse_url_part}/Tables/"
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
                return
            
            # Group tables by schema for display
            schema_tables = {}
            for schema_name, table_name in tables:
                if schema_name not in schema_tables:
                    schema_tables[schema_name] = []
                schema_tables[schema_name].append(table_name)
            
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
                
        except Exception as e:
            print(f"❌ Error attaching lakehouse: {e}")

    def _register_lookup_functions(self):
        """
        Register Fabric API lookup functions as DuckDB UDFs.
        Allows SQL queries to resolve workspace/lakehouse names from IDs and vice versa.
        
        Functions registered:
        - get_workspace_name(workspace_id) -> str
        - get_lakehouse_name(workspace_id, lakehouse_id) -> str
        - get_workspace_id_from_name(workspace_name) -> str
        - get_lakehouse_id_from_name(workspace_id, lakehouse_name) -> str
        """
        # Cache to avoid repeated API calls
        self._name_cache = {
            'workspace_id_to_name': {},
            'workspace_name_to_id': {},
            'lakehouse_id_to_name': {},
            'lakehouse_name_to_id': {}
        }
        
        def get_workspace_name(workspace_id: str) -> str:
            """Get workspace display name from workspace ID (GUID)"""
            if workspace_id in self._name_cache['workspace_id_to_name']:
                return self._name_cache['workspace_id_to_name'][workspace_id]
            
            try:
                from .auth import get_fabric_api_token
                token = get_fabric_api_token()
                if not token:
                    return None
                
                url = "https://api.fabric.microsoft.com/v1/workspaces"
                headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
                response = requests.get(url, headers=headers)
                response.raise_for_status()
                
                workspaces = response.json().get("value", [])
                for workspace in workspaces:
                    if workspace.get("id") == workspace_id:
                        name = workspace.get("displayName", "")
                        self._name_cache['workspace_id_to_name'][workspace_id] = name
                        self._name_cache['workspace_name_to_id'][name] = workspace_id
                        return name
                
                return None
            except Exception as e:
                return None
        
        def get_lakehouse_name(workspace_id: str, lakehouse_id: str) -> str:
            """Get lakehouse display name from workspace ID and lakehouse ID (GUIDs)"""
            cache_key = f"{workspace_id}/{lakehouse_id}"
            if cache_key in self._name_cache['lakehouse_id_to_name']:
                return self._name_cache['lakehouse_id_to_name'][cache_key]
            
            try:
                from .auth import get_fabric_api_token
                token = get_fabric_api_token()
                if not token:
                    return None
                
                url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/lakehouses"
                headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
                response = requests.get(url, headers=headers)
                response.raise_for_status()
                
                lakehouses = response.json().get("value", [])
                for lakehouse in lakehouses:
                    if lakehouse.get("id") == lakehouse_id:
                        name = lakehouse.get("displayName", "")
                        self._name_cache['lakehouse_id_to_name'][cache_key] = name
                        lh_cache_key = f"{workspace_id}/{name}"
                        self._name_cache['lakehouse_name_to_id'][lh_cache_key] = lakehouse_id
                        return name
                
                return None
            except Exception as e:
                return None
        
        def get_workspace_id_from_name(workspace_name: str) -> str:
            """Get workspace ID (GUID) from workspace display name"""
            if workspace_name in self._name_cache['workspace_name_to_id']:
                return self._name_cache['workspace_name_to_id'][workspace_name]
            
            try:
                from .auth import get_fabric_api_token
                token = get_fabric_api_token()
                if not token:
                    return None
                
                url = "https://api.fabric.microsoft.com/v1/workspaces"
                headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
                response = requests.get(url, headers=headers)
                response.raise_for_status()
                
                workspaces = response.json().get("value", [])
                for workspace in workspaces:
                    if workspace.get("displayName") == workspace_name:
                        workspace_id = workspace.get("id", "")
                        self._name_cache['workspace_name_to_id'][workspace_name] = workspace_id
                        self._name_cache['workspace_id_to_name'][workspace_id] = workspace_name
                        return workspace_id
                
                return None
            except Exception as e:
                return None
        
        def get_lakehouse_id_from_name(workspace_id: str, lakehouse_name: str) -> str:
            """Get lakehouse ID (GUID) from workspace ID and lakehouse display name"""
            cache_key = f"{workspace_id}/{lakehouse_name}"
            if cache_key in self._name_cache['lakehouse_name_to_id']:
                return self._name_cache['lakehouse_name_to_id'][cache_key]
            
            try:
                from .auth import get_fabric_api_token
                token = get_fabric_api_token()
                if not token:
                    return None
                
                url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/lakehouses"
                headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
                response = requests.get(url, headers=headers)
                response.raise_for_status()
                
                lakehouses = response.json().get("value", [])
                for lakehouse in lakehouses:
                    if lakehouse.get("displayName") == lakehouse_name:
                        lakehouse_id = lakehouse.get("id", "")
                        self._name_cache['lakehouse_name_to_id'][cache_key] = lakehouse_id
                        id_cache_key = f"{workspace_id}/{lakehouse_id}"
                        self._name_cache['lakehouse_id_to_name'][id_cache_key] = lakehouse_name
                        return lakehouse_id
                
                return None
            except Exception as e:
                return None
        
        # Register functions in DuckDB
        try:
            self.con.create_function("get_workspace_name", get_workspace_name)
            self.con.create_function("get_lakehouse_name", get_lakehouse_name)
            self.con.create_function("get_workspace_id_from_name", get_workspace_id_from_name)
            self.con.create_function("get_lakehouse_id_from_name", get_lakehouse_id_from_name)
        except Exception as e:
            print(f"⚠️  Warning: Could not register lookup functions: {e}")

    def get_workspace_id(self) -> str:
        """
        Get the workspace ID (GUID or name without spaces).
        Use this when passing workspace parameter to Python functions.
        
        Returns:
            Workspace ID - either a GUID or workspace name without spaces
        """
        return self.workspace_id
    
    def get_lakehouse_id(self) -> str:
        """
        Get the lakehouse ID (GUID or name).
        Use this when passing lakehouse parameter to Python functions.
        
        Returns:
            Lakehouse ID - either a GUID or lakehouse name
        """
        return self.lakehouse_id

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

    def list_lakehouses(self) -> List[str]:
        """
        List all lakehouses in the current workspace.
        
        Returns:
            List of lakehouse names
        """
        try:
            # Get authentication token using enhanced auth system
            from .auth import get_fabric_api_token
            token = get_fabric_api_token()
            if not token:
                print("❌ Failed to authenticate for listing lakehouses")
                return []
            
            # Try to get current workspace ID if in notebook environment
            workspace_id = None
            try:
                import notebookutils  # type: ignore
                workspace_id = notebookutils.runtime.context.get("workspaceId")
            except ImportError:
                pass  # Not in notebook environment
            
            if not workspace_id:
                # Get workspace ID by name
                workspace_id = self._get_workspace_id_by_name(token, self.workspace)
                if not workspace_id:
                    print(f"Workspace '{self.workspace}' not found")
                    return []
            
            # List lakehouses
            url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/lakehouses"
            headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
            
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            lakehouses = response.json().get("value", [])
            lakehouse_names = [lh.get("displayName", "") for lh in lakehouses]
            
            print(f"Found {len(lakehouse_names)} lakehouses: {lakehouse_names}")
            return lakehouse_names
            
        except Exception as e:
            print(f"Error listing lakehouses: {e}")
            return []

    def create_lakehouse_if_not_exists(self, lakehouse_name: str) -> bool:
        """
        Create a lakehouse if it doesn't already exist.
        
        Args:
            lakehouse_name: Name of the lakehouse to create
            
        Returns:
            True if lakehouse exists or was created successfully, False otherwise
        """
        try:
            # Get authentication token using enhanced auth system
            from .auth import get_fabric_api_token
            token = get_fabric_api_token()
            if not token:
                print("❌ Failed to authenticate for lakehouse creation")
                return False
            
            # Try to get current workspace ID if in notebook environment
            workspace_id = None
            try:
                import notebookutils  # type: ignore
                workspace_id = notebookutils.runtime.context.get("workspaceId")
            except ImportError:
                pass  # Not in notebook environment
            
            if not workspace_id:
                # Get workspace ID by name
                workspace_id = self._get_workspace_id_by_name(token, self.workspace)
                if not workspace_id:
                    print(f"Workspace '{self.workspace}' not found")
                    return False
            
            # Check if lakehouse already exists
            url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/lakehouses"
            headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
            
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            lakehouses = response.json().get("value", [])
            existing_names = [lh.get("displayName", "") for lh in lakehouses]
            
            if lakehouse_name in existing_names:
                print(f"Lakehouse '{lakehouse_name}' already exists")
                return True
            
            # Create lakehouse
            print(f"Creating lakehouse '{lakehouse_name}'...")
            payload = {
                "displayName": lakehouse_name,
                "description": f"Lakehouse {lakehouse_name} created via duckrun"
            }
            
            response = requests.post(url, headers=headers, json=payload)
            response.raise_for_status()
            
            print(f"✅ Lakehouse '{lakehouse_name}' created successfully")
            return True
            
        except Exception as e:
            print(f"❌ Error creating lakehouse '{lakehouse_name}': {e}")
            return False

    def deploy(self, bim_url: str, dataset_name: Optional[str] = None, 
               wait_seconds: int = 5) -> int:
        """
        Deploy a semantic model from a BIM file using DirectLake mode.
        
        Args:
            bim_url: Can be:
                - URL: "https://raw.githubusercontent.com/.../model.bim"
                - Local file: "model.bim"
                - Workspace/Model: "workspace_name/model_name"
            dataset_name: Name for the semantic model (default: source model name if workspace/model format, else lakehouse_schema)
            wait_seconds: Seconds to wait for permission propagation (default: 5)
        
        Returns:
            1 for success, 0 for failure
        
        Examples:
            dr = Duckrun.connect("My Workspace/My Lakehouse.lakehouse/dbo")
            
            # Deploy from workspace/model (uses same name by default)
            dr.deploy("Source Workspace/Source Model")  # Creates "Source Model"
            
            # Deploy with custom name
            dr.deploy("Source Workspace/Source Model", dataset_name="Sales Model Copy")
            
            # Deploy from URL or local file
            dr.deploy("https://raw.githubusercontent.com/.../model.bim", dataset_name="My Model")
        """
        from .semantic_model import deploy_semantic_model
        
        # Auto-generate dataset name if not provided
        if dataset_name is None:
            # If using workspace/model format, use the model name
            if "/" in bim_url and not bim_url.startswith(('http://', 'https://')):
                parts = bim_url.split("/")
                if len(parts) == 2:
                    dataset_name = parts[1]  # Use the model name
                else:
                    dataset_name = f"{self.lakehouse_name}_{self.schema}"
            else:
                dataset_name = f"{self.lakehouse_name}_{self.schema}"
        
        # Call the deployment function (DirectLake only)
        return deploy_semantic_model(
            workspace_name_or_id=self.workspace,
            lakehouse_name_or_id=self.lakehouse_name,
            schema_name=self.schema,
            dataset_name=dataset_name,
            bim_url_or_path=bim_url,
            wait_seconds=wait_seconds
        )

    def _get_workspace_id_by_name(self, token: str, workspace_name: str) -> Optional[str]:
        """Helper method to get workspace ID from name"""
        try:
            url = "https://api.fabric.microsoft.com/v1/workspaces"
            headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
            
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            workspaces = response.json().get("value", [])
            for workspace in workspaces:
                if workspace.get("displayName") == workspace_name:
                    return workspace.get("id")
            
            return None
            
        except Exception:
            return None

    def close(self):
        """Close DuckDB connection"""
        if self.con:
            self.con.close()
            print("Connection closed")


class WorkspaceConnection:
    """
    Simple workspace connection for lakehouse management operations.
    """
    
    def __init__(self, workspace_name: str):
        self.workspace_name = workspace_name
    
    def list_lakehouses(self) -> List[str]:
        """
        List all lakehouses in the workspace.
        
        Returns:
            List of lakehouse names
        """
        try:
            # Get authentication token using enhanced auth system
            from .auth import get_fabric_api_token
            token = get_fabric_api_token()
            if not token:
                print("❌ Failed to authenticate for listing lakehouses")
                return []
            
            # Always resolve workspace name to ID, even in notebook environment
            workspace_id = self._get_workspace_id_by_name(token, self.workspace_name)
            if not workspace_id:
                print(f"Workspace '{self.workspace_name}' not found")
                return []
            
            # List lakehouses
            url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/lakehouses"
            headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
            
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            lakehouses = response.json().get("value", [])
            lakehouse_names = [lh.get("displayName", "") for lh in lakehouses]
            
            return lakehouse_names
            
        except Exception as e:
            print(f"Error listing lakehouses: {e}")
            return []

    def create_lakehouse_if_not_exists(self, lakehouse_name: str) -> bool:
        """
        Create a lakehouse if it doesn't already exist.
        
        Args:
            lakehouse_name: Name of the lakehouse to create
            
        Returns:
            True if lakehouse exists or was created successfully, False otherwise
        """
        try:
            # Get authentication token using enhanced auth system
            from .auth import get_fabric_api_token
            token = get_fabric_api_token()
            if not token:
                print("❌ Failed to authenticate for lakehouse creation")
                return False
            
            # Always resolve workspace name to ID, even in notebook environment
            workspace_id = self._get_workspace_id_by_name(token, self.workspace_name)
            if not workspace_id:
                print(f"Workspace '{self.workspace_name}' not found")
                return False
            
            # Check if lakehouse already exists
            url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/lakehouses"
            headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
            
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            lakehouses = response.json().get("value", [])
            existing_names = [lh.get("displayName", "") for lh in lakehouses]
            
            if lakehouse_name in existing_names:
                print(f"Lakehouse '{lakehouse_name}' already exists")
                return True
            
            # Create lakehouse
            print(f"Creating lakehouse '{lakehouse_name}'...")
            payload = {
                "displayName": lakehouse_name,
                "description": f"Lakehouse {lakehouse_name} created via duckrun",
                "creationPayload": {
                    "enableSchemas": True
                }
            }
            
            response = requests.post(url, headers=headers, json=payload)
            response.raise_for_status()
            
            print(f"✅ Lakehouse '{lakehouse_name}' created successfully")
            return True
            
        except Exception as e:
            print(f"❌ Error creating lakehouse '{lakehouse_name}': {e}")
            return False
    
    def download_bim(self, semantic_model_name: str, output_path: Optional[str] = None) -> Optional[str]:
        """
        Download a semantic model as a BIM (Business Intelligence Model) file.
        
        Args:
            semantic_model_name: Name of the semantic model to download
            output_path: Optional path to save the BIM file. If not provided, returns the BIM content as JSON string
            
        Returns:
            BIM content as JSON string if output_path is None, or the file path if saved successfully
            
        Example:
            con = duckrun.connect("My Workspace")
            # Get BIM content as string
            bim_content = con.download_bim("Sales Model")
            # Or save to file
            con.download_bim("Sales Model", "sales_model.bim")
        """
        try:
            # Get authentication token
            from .auth import get_fabric_api_token
            token = get_fabric_api_token()
            if not token:
                print("❌ Failed to authenticate for downloading semantic model")
                return None
            
            # Resolve workspace name to ID
            workspace_id = self._get_workspace_id_by_name(token, self.workspace_name)
            if not workspace_id:
                print(f"❌ Workspace '{self.workspace_name}' not found")
                return None
            
            # Get semantic model ID
            print(f"🔍 Looking for semantic model '{semantic_model_name}'...")
            url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/semanticModels"
            headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
            
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            models = response.json().get("value", [])
            model = next((m for m in models if m.get("displayName") == semantic_model_name), None)
            
            if not model:
                print(f"❌ Semantic model '{semantic_model_name}' not found in workspace '{self.workspace_name}'")
                return None
            
            model_id = model.get("id")
            print(f"✓ Found semantic model: {semantic_model_name} (ID: {model_id})")
            
            # Get the model definition using the generic items API
            print("📥 Downloading BIM definition...")
            definition_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{model_id}/getDefinition"
            
            # POST request to get definition with TMSL format (which includes model.bim)
            # Note: format parameter should be in query string, not body
            response = requests.post(f"{definition_url}?format=TMSL", headers=headers)
            response.raise_for_status()
            
            # Handle long-running operation if needed
            if response.status_code == 202:
                operation_id = response.headers.get('x-ms-operation-id')
                print(f"   Waiting for operation to complete...")
                
                max_attempts = 30
                for attempt in range(max_attempts):
                    time.sleep(2)
                    
                    # Get operation result
                    result_url = f"https://api.fabric.microsoft.com/v1/operations/{operation_id}/result"
                    result_response = requests.get(result_url, headers=headers)
                    
                    # Check operation status
                    status_url = f"https://api.fabric.microsoft.com/v1/operations/{operation_id}"
                    status_response = requests.get(status_url, headers=headers)
                    status = status_response.json().get('status')
                    
                    if status == 'Succeeded':
                        result_data = result_response.json()
                        break
                    elif status == 'Failed':
                        error = status_response.json().get('error', {})
                        print(f"❌ Operation failed: {error.get('message')}")
                        return None
                    elif attempt == max_attempts - 1:
                        print("❌ Operation timed out")
                        return None
            else:
                result_data = response.json()
            
            # Extract BIM content from definition
            definition = result_data.get('definition', {})
            parts = definition.get('parts', [])
            
            # Debug: show what parts we have
            if not parts:
                print("❌ No definition parts found in response")
                print(f"   Result data keys: {list(result_data.keys())}")
                print(f"   Definition keys: {list(definition.keys()) if definition else 'None'}")
                return None
            
            print(f"   Found {len(parts)} definition parts:")
            for part in parts:
                print(f"     - {part.get('path', 'unknown')}")
            
            bim_part = next((p for p in parts if p.get('path', '').endswith('.bim')), None)
            if not bim_part:
                print("❌ No BIM file found in semantic model definition")
                print(f"   Looking for files ending with '.bim', found: {[p.get('path') for p in parts]}")
                return None
            
            # Decode the BIM content (it's base64 encoded)
            import base64
            bim_payload = bim_part.get('payload', '')
            bim_content = base64.b64decode(bim_payload).decode('utf-8')
            bim_json = json.loads(bim_content)
            
            # Format as pretty JSON
            bim_formatted = json.dumps(bim_json, indent=2)
            
            print(f"✓ BIM file downloaded successfully")
            print(f"  - Tables: {len(bim_json.get('model', {}).get('tables', []))}")
            print(f"  - Relationships: {len(bim_json.get('model', {}).get('relationships', []))}")
            
            # Save to file or return content
            if output_path:
                with open(output_path, 'w', encoding='utf-8') as f:
                    f.write(bim_formatted)
                print(f"✓ Saved to: {output_path}")
                return output_path
            else:
                return bim_formatted
                
        except Exception as e:
            print(f"❌ Error downloading semantic model: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def _get_workspace_id_by_name(self, token: str, workspace_name: str) -> Optional[str]:
        """Helper method to get workspace ID from name"""
        try:
            url = "https://api.fabric.microsoft.com/v1/workspaces"
            headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
            
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            workspaces = response.json().get("value", [])
            for workspace in workspaces:
                if workspace.get("displayName") == workspace_name:
                    return workspace.get("id")
            
            return None
            
        except Exception:
            return None