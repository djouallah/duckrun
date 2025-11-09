import duckdb
import requests
import os
import importlib.util
import json
import time
from typing import List, Tuple, Union, Optional, Callable, Dict, Any
from string import Template
from datetime import datetime
from .stats import get_stats as _get_stats
from .runner import run as _run
from .files import copy as _copy, download as _download
from .writer import QueryResult


class WorkspaceOperationsMixin:
    """
    Mixin class for workspace-level operations that work for both
    full Duckrun connections and workspace-only connections.
    """
    
    def import_notebook_from_web(self, url: str, 
                                  notebook_name: Optional[str] = None,
                                  overwrite: bool = False) -> dict:
        """
        Import a Jupyter notebook from a web URL into the workspace.
        
        Args:
            url: URL to the notebook file (e.g., GitHub raw URL). Required.
            notebook_name: Name for the imported notebook. Optional - derived from URL if not provided.
            overwrite: Whether to overwrite if notebook already exists (default: False)
            
        Returns:
            Dictionary with import result
            
        Examples:
            con = duckrun.connect("workspace/lakehouse.lakehouse")
            result = con.import_notebook_from_web(
                url="https://raw.githubusercontent.com/user/repo/main/notebook.ipynb"
            )
            
            ws = duckrun.connect("workspace")
            result = ws.import_notebook_from_web(
                url="https://raw.githubusercontent.com/user/repo/main/notebook.ipynb"
            )
        """
        from .notebook import import_notebook_from_web as _import_notebook_from_web
        
        # Get workspace name from either self.workspace or self.workspace_name
        workspace_name = getattr(self, 'workspace', None) or getattr(self, 'workspace_name', None)
        
        return _import_notebook_from_web(
            url=url,
            notebook_name=notebook_name,
            overwrite=overwrite,
            workspace_name=workspace_name
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


class Duckrun(WorkspaceOperationsMixin):
    """
    OneLake task runner with clean tuple-based API.
    Supports lakehouses, warehouses, databases, and other OneLake items.
    Powered by DuckDB for fast data processing.
    
    Task formats:
        Python: ('function_name', (arg1, arg2, ...))
        SQL:    ('table_name', 'mode', {params})
    
    Usage:
        # For pipelines:
        dr = Duckrun.connect("workspace/lakehouse.lakehouse/schema", sql_folder="./sql")
        dr = Duckrun.connect("workspace/lakehouse.lakehouse")  # defaults to dbo schema, lists all tables
        dr.run(pipeline)
        
        # For other OneLake items:
        dr = Duckrun.connect("SNOWFLAKE/ONELAKEUSEAST.SnowflakeDatabase")
        dr = Duckrun.connect("workspace/warehouse.Warehouse")
        
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
                 scan_all_schemas: bool = False, storage_account: str = "onelake",
                 token_only: bool = False):
        # Store GUIDs for internal use
        self.workspace_id = workspace_id
        self.lakehouse_id = lakehouse_id
        self.schema = schema
        self.sql_folder = sql_folder.strip() if sql_folder else None
        self.compaction_threshold = compaction_threshold
        self.scan_all_schemas = scan_all_schemas
        self.storage_account = storage_account
        self.token_only = token_only
        
        # Store both full name (with .ItemType) and display name (without .ItemType) for backward compatibility
        # lakehouse_id: Full name with suffix for API calls (e.g., "data.Lakehouse")
        # lakehouse_display_name: Name only without suffix for user code/templates (e.g., "data")
        self.lakehouse_id = lakehouse_id
        
        # Extract display name (remove .ItemType suffix if present)
        import re
        # Check if lakehouse_id has .ItemType suffix
        if not re.match(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', lakehouse_id, re.IGNORECASE):
            # Friendly name - extract base name without suffix
            for suffix in ['.Lakehouse', '.Warehouse', '.Database', '.SnowflakeDatabase']:
                if lakehouse_id.endswith(suffix):
                    self.lakehouse_display_name = lakehouse_id[:-len(suffix)]
                    break
            else:
                self.lakehouse_display_name = lakehouse_id
        else:
            # GUID - use as is
            self.lakehouse_display_name = lakehouse_id
        
        # Construct proper ABFSS URLs
        # Format: abfss://{workspace}@{storage_account}.dfs.fabric.microsoft.com/{item}/Tables/
        # where {workspace} and {item} can be:
        #   - Names with .lakehouse suffix (lakehouse optimization when no spaces in workspace)
        #   - GUIDs (when resolved via API for non-lakehouse items or items with spaces)
        guid_pattern = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.IGNORECASE)
        
        # Determine the item URL part for ABFSS
        if guid_pattern.match(lakehouse_id):
            # Already a GUID - use as-is (from API resolution)
            item_url_part = lakehouse_id
        else:
            # Friendly name - use as-is (already includes .ItemType suffix from connect())
            item_url_part = lakehouse_id
        
        self.table_base_url = f'abfss://{workspace_id}@{storage_account}.dfs.fabric.microsoft.com/{item_url_part}/Tables/'
        self.files_base_url = f'abfss://{workspace_id}@{storage_account}.dfs.fabric.microsoft.com/{item_url_part}/Files/'
        
        # Keep legacy properties for backward compatibility
        self.workspace = workspace_id  
        self.lakehouse_name = self.lakehouse_display_name  # Use display name (without suffix) for backward compatibility
        
        # Store display name without suffix for backward compatibility with user Python functions
        # Extract base name by removing .ItemType suffix if present
        import re
        suffix_pattern = re.compile(r'\.(Lakehouse|Warehouse|Database|SnowflakeDatabase)$', re.IGNORECASE)
        self.lakehouse_display_name = suffix_pattern.sub('', lakehouse_id)
        
        self.con = duckdb.connect()
        self.con.sql("SET preserve_insertion_order = false")
        
        # Configure Azure transport for Colab (fixes SSL cert issues)
        try:
            import google.colab  # type: ignore
            self.con.sql("SET azure_transport_option_type = 'curl'")
            print("🔧 Colab detected - using curl transport for Azure")
        except ImportError:
            pass  # Not in Colab, use default transport
        
        # Only attach lakehouse and register functions if not token_only mode
        if not token_only:
            self._attach_lakehouse()
            self._register_lookup_functions()
        else:
            # In token_only mode, just create the secret for authentication
            self._create_onelake_secret()
            print("✓ Token authenticated (fast mode - tables not listed)")

    @classmethod
    def connect(cls, connection_string: str, sql_folder: Optional[str] = None,
                compaction_threshold: int = 100, storage_account: str = "onelake",
                token_only: bool = False):
        """
        Create and connect to OneLake items (lakehouse, warehouse, database, etc.) or workspace.
        
        Smart detection based on connection string format:
        - "workspace" → workspace management only
        - "ws/item.lakehouse/schema" → lakehouse connection with specific schema
        - "ws/item.lakehouse" → lakehouse connection (defaults to dbo schema)
        - "ws/item.warehouse" → warehouse connection
        - "ws/item.database" → database connection
        - "ws/item.snowflakedatabase" → Snowflake database connection
        
        Args:
            connection_string: OneLake path or workspace name
            sql_folder: Optional path or URL to SQL files folder  
            compaction_threshold: File count threshold for compaction
            storage_account: Storage account name (default: "onelake")
            token_only: If True, only authenticate without listing tables (faster connection)
        
        Examples:
            # Workspace management only (supports spaces in names)
            ws = Duckrun.connect("My Workspace Name")
            ws.list_lakehouses()
            ws.create_lakehouse_if_not_exists("New Lakehouse")
            
            # Lakehouse connections (supports spaces in names)
            dr = Duckrun.connect("My Workspace/My Lakehouse.lakehouse/schema", sql_folder="./sql")
            dr = Duckrun.connect("Data Workspace/Sales Data.lakehouse/analytics")  # spaces supported
            dr = Duckrun.connect("My Workspace/My Lakehouse.lakehouse")  # defaults to dbo schema
            dr = Duckrun.connect("workspace/lakehouse.lakehouse", storage_account="xxx-onelake")  # custom storage
            
            # Warehouse and database connections (always uses API to resolve GUIDs)
            dr = Duckrun.connect("SNOWFLAKE/ONELAKEUSEAST.SnowflakeDatabase")
            dr = Duckrun.connect("My Workspace/My Warehouse.Warehouse")
            dr = Duckrun.connect("workspace/database.Database")
            
            # Fast connection without table listing (token only)
            dr = Duckrun.connect("workspace/lakehouse.lakehouse", token_only=True)
            
        Note:
            - Lakehouse items without spaces in workspace name use optimization (no API calls)
            - Non-lakehouse items always resolve to GUIDs via Fabric API
            - Internally constructs proper ABFSS URLs:
              "My Workspace/My Item.lakehouse/schema" → 
              "abfss://workspace_guid@onelake.dfs.fabric.microsoft.com/item_guid/Tables/schema"
        """
        
        # Check if it's a workspace-only connection (no "/" means workspace name only)
        if "/" not in connection_string:
            return WorkspaceConnection(connection_string)
        
        scan_all_schemas = False
        
        # Parse connection string: "ws/item_name.item_type/schema" or "ws/item_name.item_type"  
        # Support workspace and item names with spaces
        # Item types: .lakehouse, .Lakehouse, .warehouse, .Warehouse, .database, .Database, .snowflakedatabase, .SnowflakeDatabase
        parts = connection_string.split("/")
        if len(parts) == 2:
            workspace_name, item_name_with_type = parts
            scan_all_schemas = True
            schema = "dbo"
        elif len(parts) == 3:
            workspace_name, item_name_with_type, schema = parts
        else:
            raise ValueError(
                f"Invalid connection string format: '{connection_string}'. "
                "Expected formats:\n"
                "  'workspace name' (workspace management only)\n"
                "  'workspace name/item name.item_type' (item with dbo schema)\n"
                "  'workspace name/item name.item_type/schema' (item with specific schema)\n"
                "Supported item types: .lakehouse, .warehouse, .database, .snowflakedatabase (case-insensitive)"
            )
        
        # Extract item type and name
        item_type = None
        item_name = item_name_with_type
        
        # Check for known item types (case-insensitive)
        item_type_map = {
            '.lakehouse': 'Lakehouse',
            '.warehouse': 'Warehouse', 
            '.database': 'Database',
            '.snowflakedatabase': 'SnowflakeDatabase'
        }
        
        # Parse item type and normalize the suffix to proper case
        item_name_normalized = item_name_with_type
        for suffix, mapped_type in item_type_map.items():
            if item_name_with_type.lower().endswith(suffix):
                item_type = mapped_type
                item_name = item_name_with_type[:-len(suffix)]
                # Normalize to proper case: ItemName.ItemType (e.g., data.Lakehouse)
                item_name_normalized = f"{item_name}.{mapped_type}"
                break
        
        if not workspace_name or not item_name:
            raise ValueError(
                "Missing required parameters. Use one of these formats:\n"
                "  connect('workspace name')  # workspace management\n"
                "  connect('workspace name/item name.item_type/schema')  # full item connection\n"
                "  connect('workspace name/item name.item_type')  # defaults to dbo"
            )
        
        # Per OneLake API docs: Can use friendly names if no spaces/special characters
        # Otherwise must resolve to GUIDs
        # Check for spaces or special characters that would require GUID resolution
        has_special_chars = " " in workspace_name or " " in item_name
        
        if has_special_chars:
            # Names have spaces/special chars: resolve to GUIDs via API
            workspace_id, item_id = cls._resolve_names_to_guids(workspace_name, item_name, item_type)
        else:
            # No spaces/special chars: use friendly names directly (works for all item types)
            # Use normalized name with proper case for API compatibility
            workspace_id = workspace_name
            item_id = item_name_normalized  # Use normalized with proper case
        
        return cls(workspace_id, item_id, schema, sql_folder, compaction_threshold, scan_all_schemas, storage_account, token_only)

    @classmethod
    def _resolve_names_to_guids(cls, workspace_name: str, item_name: str, item_type: Optional[str] = 'Lakehouse') -> tuple[str, str]:
        """
        Resolve friendly workspace and item names to their GUIDs.
        
        Args:
            workspace_name: Display name of the workspace (can contain spaces)
            item_name: Display name of the item (can contain spaces)
            item_type: Type of item - 'Lakehouse', 'Warehouse', 'Database', 'SnowflakeDatabase', etc.
            
        Returns:
            Tuple of (workspace_id, item_id) - resolved GUIDs
        """
        
        # Check if names are already GUIDs first
        import re
        guid_pattern = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.IGNORECASE)
        
        if guid_pattern.match(workspace_name) and guid_pattern.match(item_name):
            return workspace_name, item_name
        
        # Need to resolve to GUIDs via API
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
                # In notebook environment, validate it matches the requested workspace name
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
            
            # Resolve item name to ID based on item type
            if item_type == 'Lakehouse':
                item_id = cls._resolve_lakehouse_id_by_name(token, workspace_id, item_name)
            else:
                # Use generic item resolver for non-lakehouse items
                item_id = cls._resolve_item_id_by_name(token, workspace_id, item_name, item_type)
            
            if not item_id:
                raise ValueError(f"{item_type} '{item_name}' not found in workspace '{workspace_name}'")
            
            return workspace_id, item_id
            
        except Exception as e:
            print(f"❌ Failed to resolve names to GUIDs: {e}")
            print(f"❌ Cannot resolve '{workspace_name}'/'{item_name}' ({item_type}) to GUIDs")
            print("❌ Microsoft Fabric requires actual workspace and item GUIDs for ABFSS access")
            raise ValueError(
                f"Unable to resolve workspace '{workspace_name}' and {item_type.lower()} '{item_name}' to GUIDs. "
                f"ABFSS URLs require actual GUIDs. "
                f"Please ensure you have proper authentication and the workspace/item names are correct."
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
    def _resolve_item_id_by_name(cls, token: str, workspace_id: str, item_name: str, item_type: str) -> Optional[str]:
        """
        Get item ID from display name within a workspace using generic items API.
        Works for any item type: Warehouse, Database, SnowflakeDatabase, etc.
        
        Args:
            token: Fabric API authentication token
            workspace_id: Workspace GUID
            item_name: Display name of the item
            item_type: Type of item (e.g., 'Warehouse', 'Database', 'SnowflakeDatabase')
            
        Returns:
            Item GUID if found, None otherwise
        """
        try:
            import requests
            # Use generic items API with type filter
            url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items"
            headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
            
            # Add type filter as query parameter
            params = {"type": item_type}
            
            print(f"   Searching for {item_type} '{item_name}' in workspace {workspace_id}")
            print(f"   API URL: {url}?type={item_type}")
            
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            
            result = response.json()
            items = result.get("value", [])
            
            print(f"   Found {len(items)} items of type {item_type}")
            if items:
                print(f"   Available items: {[item.get('displayName') for item in items]}")
            
            for item in items:
                if item.get("displayName") == item_name:
                    item_id = item.get("id")
                    print(f"   Found matching item: {item_name} -> {item_id}")
                    return item_id
            
            print(f"   Item '{item_name}' not found in the list")
            return None
        except Exception as e:
            print(f"   Error resolving {item_type} item: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"   Response status: {e.response.status_code}")
                print(f"   Response body: {e.response.text}")
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
        Fast table discovery using OneLake Delta Table API (Unity Catalog compatible).
        Uses: https://learn.microsoft.com/en-us/fabric/onelake/table-apis/delta-table-apis-overview
        
        Returns:
            List of tuples: [(schema, table_name), ...]
        """
        try:
            # Get storage token for OneLake
            token = self._get_storage_token()
            if token == "PLACEHOLDER_TOKEN_TOKEN_NOT_AVAILABLE":
                print("Authenticating with Azure for table discovery...")
                from .auth import get_token
                token = get_token()
                if not token:
                    print("❌ Failed to authenticate for table discovery")
                    return []
            
            # OneLake Delta Table API endpoint (Unity Catalog compatible)
            base_url = "https://onelake.table.fabric.microsoft.com/delta"
            
            # Determine workspace/item identifier for API
            # Per docs: Can use friendly names (WorkspaceName/ItemName.ItemType) if no special characters
            # Otherwise must use GUIDs (WorkspaceID/ItemID)
            import re
            guid_pattern = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.IGNORECASE)
            
            # Check if we're using GUIDs or friendly names
            if guid_pattern.match(self.workspace_id) and guid_pattern.match(self.lakehouse_id):
                # Using GUIDs - use them directly in API
                workspace_identifier = self.workspace_id
                item_identifier = self.lakehouse_id
                catalog_name = self.lakehouse_id
            else:
                # Using friendly names - lakehouse_id already includes .ItemType suffix
                workspace_identifier = self.workspace_id
                item_identifier = self.lakehouse_id
                catalog_name = self.lakehouse_id
            
            print(f"🔍 Discovering tables via OneLake Delta Table API...")
            print(f"   Using identifier: {workspace_identifier}/{item_identifier}")
            
            tables_found = []
            
            if self.scan_all_schemas:
                # First, list all schemas
                schemas_url = f"{base_url}/{workspace_identifier}/{item_identifier}/api/2.1/unity-catalog/schemas"
                params = {"catalog_name": catalog_name}
                headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
                
                schemas_response = requests.get(schemas_url, headers=headers, params=params)
                
                if schemas_response.status_code == 200:
                    schemas_result = schemas_response.json()
                    schemas = schemas_result.get("schemas", [])
                    schema_names = [s.get("name") for s in schemas if s.get("name")]
                    
                    print(f"   Found {len(schema_names)} schemas: {schema_names}")
                    
                    # Get tables from each schema
                    for schema_name in schema_names:
                        tables_url = f"{base_url}/{workspace_identifier}/{item_identifier}/api/2.1/unity-catalog/tables"
                        tables_params = {
                            "catalog_name": catalog_name,
                            "schema_name": schema_name
                        }
                        
                        tables_response = requests.get(tables_url, headers=headers, params=tables_params)
                        
                        if tables_response.status_code == 200:
                            tables_result = tables_response.json()
                            tables = tables_result.get("tables", [])
                            
                            for table in tables:
                                table_name = table.get("name", "")
                                if table_name:
                                    tables_found.append((schema_name, table_name))
                            
                            if tables:
                                print(f"   Schema '{schema_name}': {len(tables)} tables")
                else:
                    print(f"   Failed to list schemas: {schemas_response.status_code}")
                    if schemas_response.status_code != 404:
                        print(f"   Response: {schemas_response.text[:300]}")
            else:
                # Single schema mode - list tables in specific schema
                tables_url = f"{base_url}/{workspace_identifier}/{item_identifier}/api/2.1/unity-catalog/tables"
                params = {
                    "catalog_name": catalog_name,
                    "schema_name": self.schema
                }
                headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
                
                print(f"   Listing tables in schema: {self.schema}")
                tables_response = requests.get(tables_url, headers=headers, params=params)
                
                if tables_response.status_code == 200:
                    tables_result = tables_response.json()
                    tables = tables_result.get("tables", [])
                    
                    for table in tables:
                        table_name = table.get("name", "")
                        if table_name:
                            tables_found.append((self.schema, table_name))
                    
                    print(f"   Found {len(tables)} tables")
                elif tables_response.status_code == 404:
                    print(f"   Schema '{self.schema}' not found or has no tables")
                else:
                    print(f"   Failed to list tables: {tables_response.status_code}")
                    print(f"   Response: {tables_response.text[:300]}")
            
            return tables_found
            
        except Exception as e:
            print(f"❌ Error during table discovery: {e}")
            import traceback
            traceback.print_exc()
            return []

    def _attach_lakehouse(self):
        """Attach lakehouse tables as DuckDB views using fast discovery"""
        print(f"🔌 Attaching tables from schema: {self.schema if not self.scan_all_schemas else 'all schemas'}")
        self._create_onelake_secret()
        
        try:
            tables = self._discover_tables_fast()
            
            if not tables:
                if self.scan_all_schemas:
                    print(f"⚠️  No tables found in any schema")
                else:
                    print(f"⚠️  No tables found in {self.schema} schema")
                return
            
            # Collect table names for display
            table_names = []
            
            for schema_name, table_name in tables:
                try:
                    if self.scan_all_schemas:
                        # Create proper schema.table structure in DuckDB
                        self.con.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
                        view_name = f"{schema_name}.{table_name}"
                        table_names.append(view_name)
                    else:
                        # Single schema mode - use just table name
                        view_name = table_name
                        table_names.append(table_name)
                    
                    self.con.sql(f"""
                        CREATE OR REPLACE VIEW {view_name}
                        AS SELECT * FROM delta_scan('{self.table_base_url}{schema_name}/{table_name}');
                    """)
                except Exception as e:
                    print(f"⚠️  Failed to attach table {schema_name}.{table_name}: {e}")
                    continue
            
            # Print discovered tables as comma-separated list
            if table_names:
                print(", ".join(table_names))
                
        except Exception as e:
            print(f"❌ Error attaching lakehouse: {e}")
            import traceback
            traceback.print_exc()

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
            self.con.create_function("get_workspace_name", get_workspace_name, null_handling='SPECIAL')
            self.con.create_function("get_lakehouse_name", get_lakehouse_name, null_handling='SPECIAL')
            self.con.create_function("get_workspace_id_from_name", get_workspace_id_from_name, null_handling='SPECIAL')
            self.con.create_function("get_lakehouse_id_from_name", get_lakehouse_id_from_name, null_handling='SPECIAL')
        except Exception as e:
            print(f"⚠️  Warning: Could not register lookup functions: {e}")

    def get_workspace_id(self, force: bool = False) -> str:
        """
        Get the workspace ID (GUID or name without spaces).
        Use this when passing workspace parameter to Python functions.
        
        Args:
            force: If True, always resolve to actual GUID via API. If False, returns stored value (default: False)
        
        Returns:
            Workspace ID - either a GUID or workspace name without spaces
        """
        if not force:
            return self.workspace_id
        
        # Force resolution to GUID
        import re
        guid_pattern = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.IGNORECASE)
        
        # If already a GUID, return it
        if guid_pattern.match(self.workspace_id):
            return self.workspace_id
        
        # Try to get from notebook context first (fastest)
        try:
            import notebookutils  # type: ignore
            workspace_guid = notebookutils.runtime.context.get("workspaceId")
            if workspace_guid:
                return workspace_guid
        except ImportError:
            pass
        
        # Resolve via API
        try:
            from .auth import get_fabric_api_token
            token = get_fabric_api_token()
            if token:
                resolved_id = self._resolve_workspace_id_by_name(token, self.workspace_id)
                if resolved_id:
                    return resolved_id
        except Exception:
            pass
        
        # Fallback to original value
        return self.workspace_id
    
    def get_item_id(self, force: bool = False) -> str:
        """
        Get the item ID (GUID or name) - works for lakehouses, warehouses, databases, etc.
        Use this when passing lakehouse/item parameter to Python functions.
        
        Args:
            force: If True, always resolve to actual GUID via API. If False, returns stored value (default: False)
        
        Returns:
            Item ID - either a GUID or item name (supports all OneLake item types)
        """
        if not force:
            return self.lakehouse_id
        
        # Force resolution to GUID
        import re
        guid_pattern = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.IGNORECASE)
        
        # If already a GUID, return it
        if guid_pattern.match(self.lakehouse_id):
            return self.lakehouse_id
        
        # Detect item type from lakehouse_id (e.g., "data.Lakehouse" -> Lakehouse)
        item_type = None
        item_name = self.lakehouse_id
        for suffix in ['.Lakehouse', '.Warehouse', '.Database', '.SnowflakeDatabase']:
            if self.lakehouse_id.endswith(suffix):
                item_type = suffix[1:]  # Remove the leading dot
                item_name = self.lakehouse_id[:-len(suffix)]
                break
        
        # Try to get from notebook context first (only works for lakehouses)
        if item_type == 'Lakehouse' or item_type is None:
            try:
                import notebookutils  # type: ignore
                lakehouse_guid = notebookutils.lakehouse.get("id")
                if lakehouse_guid:
                    return lakehouse_guid
            except (ImportError, Exception):
                pass
        
        # Resolve via API
        try:
            from .auth import get_fabric_api_token
            token = get_fabric_api_token()
            if token:
                # First get workspace GUID
                workspace_guid = self.get_workspace_id(force=True)
                
                # Use appropriate resolver based on item type
                if item_type == 'Lakehouse' or item_type is None:
                    # Use lakehouse-specific API
                    resolved_id = self._resolve_lakehouse_id_by_name(token, workspace_guid, item_name if item_name else self.lakehouse_id)
                else:
                    # Use generic items API for warehouses, databases, etc.
                    resolved_id = self._resolve_item_id_by_name(token, workspace_guid, item_name, item_type)
                
                if resolved_id:
                    return resolved_id
        except Exception:
            pass
        
        # Fallback to original value
        return self.lakehouse_id
    
    def get_lakehouse_id(self, force: bool = False) -> str:
        """
        Deprecated: Use get_item_id() instead.
        Backward compatibility alias for get_item_id().
        """
        return self.get_item_id(force)

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

    def get_stats(self, source: str = None, detailed = False):
        """
        Get comprehensive statistics for Delta Lake tables.
        
        Args:
            source: Optional. Can be one of:
                   - None: Use all tables in the connection's schema (default)
                   - Table name: 'table_name' (uses current schema)
                   - Schema.table: 'schema.table_name' (specific table in schema)
                   - Schema only: 'schema' (all tables in schema)
            detailed: Optional. Controls the level of detail in statistics:
                     - False (default): Aggregated table-level stats
                     - True: Row group level statistics with compression details
        
        Returns:
            DataFrame with statistics based on detailed parameter:
            - If detailed=False: Aggregated table-level summary
            - If detailed=True: Granular file and row group level stats
        
        Examples:
            con = duckrun.connect("tmp/data.lakehouse/aemo")
            
            # All tables in current schema (aemo) - aggregated
            stats = con.get_stats()
            
            # Single table in current schema - aggregated
            stats = con.get_stats('price')
            
            # Single table with detailed row group statistics
            stats_detailed = con.get_stats('price', detailed=True)
            
            # Specific table in different schema
            stats = con.get_stats('aemo.price')
            
            # All tables in a schema
            stats = con.get_stats('aemo')
        """
        return _get_stats(self, source, detailed)

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
               wait_seconds: int = 5, refresh: str = "full") -> int:
        """
        Deploy a semantic model from a BIM file using DirectLake mode.
        
        Args:
            bim_url: Can be:
                - URL: "https://raw.githubusercontent.com/.../model.bim"
                - Local file: "model.bim"
                - Workspace/Model: "workspace_name/model_name"
            dataset_name: Name for the semantic model (default: schema name)
            wait_seconds: Seconds to wait for permission propagation (default: 5)
            refresh: Refresh strategy:
                - "full": Clear values and process full refresh (default)
                - "ignore": Skip refresh entirely
        
        Returns:
            1 for success, 0 for failure
        
        Examples:
            dr = Duckrun.connect("My Workspace/My Lakehouse.lakehouse/dbo")
            
            # Deploy with schema name as dataset name (dbo)
            dr.deploy("https://github.com/.../model.bim")
            
            # Deploy from workspace/model (uses same name by default)
            dr.deploy("Source Workspace/Source Model")  # Creates "Source Model"
            
            # Deploy with custom name
            dr.deploy("https://github.com/.../model.bim", dataset_name="Sales Model")
            
            # Deploy without refresh
            dr.deploy("https://github.com/.../model.bim", refresh="ignore")
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
                    dataset_name = self.schema  # Use schema name
            else:
                dataset_name = self.schema  # Use schema name
        
        # Call the deployment function (DirectLake only)
        return deploy_semantic_model(
            workspace_name_or_id=self.workspace,
            lakehouse_name_or_id=self.lakehouse_name,
            schema_name=self.schema,
            dataset_name=dataset_name,
            bim_url_or_path=bim_url,
            wait_seconds=wait_seconds,
            refresh=refresh
        )

    def rle(self, table_name: str = None, mode: str = "natural",
            min_distinct_threshold: int = 2, max_cardinality_pct: float = 0.01,
            max_ordering_depth: int = 3):
        """
        Analyze RLE (Run-Length Encoding) compression potential for Delta Lake tables.
        
        Args:
            table_name: Name of the table to analyze. Can be:
                - 'table_name' (uses current schema)
                - 'schema.table_name' (specific schema)
            mode: Analysis mode:
                - "natural": Calculate RLE for natural order only (default, fastest)
                - "auto": Natural order + cardinality-based ordering (recommended)
                - "advanced": Natural + cardinality + greedy incremental search (most thorough)
            min_distinct_threshold: Exclude columns with fewer distinct values (default: 2)
            max_cardinality_pct: Exclude columns with cardinality above this % (default: 0.01 = 1%)
            max_ordering_depth: Maximum depth for greedy search in "advanced" mode (default: 3)
        
        Returns:
            DataFrame with RLE analysis results
        
        Examples:
            # Natural order only (baseline)
            con = duckrun.connect("workspace/lakehouse.lakehouse/schema")
            con.rle("mytable")  # same as con.rle("mytable", "natural")
            
            # Auto optimization (natural + cardinality-based)
            con.rle("mytable", "auto")
            
            # Advanced optimization (greedy incremental search)
            con.rle("mytable", "advanced")
            
            # Advanced with custom depth
            con.rle("mytable", "advanced", max_ordering_depth=4)
            
            # Analyze table from different schema
            con.rle("otherschema.mytable", "auto")
            
            # Custom thresholds for small tables
            con.rle("mytable", "auto", max_cardinality_pct=0.05)
        """
        from .rle import (
            calculate_cardinality_ratio,
            test_column_orderings_smart,
            calculate_rle_for_columns
        )
        from deltalake import DeltaTable
        
        # Parse table name and construct path
        if table_name is None:
            if mode != "summary":
                print("⚠️  Table name is required for 'smart' and 'full' modes")
                return None
            # TODO: Implement all-tables summary
            print("⚠️  All-tables summary not yet implemented. Please specify a table name.")
            return None
        
        # Parse schema.table or just table
        if '.' in table_name:
            schema_name, tbl = table_name.split('.', 1)
        else:
            schema_name = self.schema
            tbl = table_name
        
        # Construct the full table path using the same logic as get_stats
        table_path = f"{self.table_base_url}{schema_name}/{tbl}"
        
        # Verify table exists and is not empty
        print(f"📊 Analyzing table: {schema_name}.{tbl}")
        
        try:
            dt = DeltaTable(table_path)
            delta_files = dt.files()
            
            if not delta_files:
                print("⚠️  Table is empty (no files)")
                return None
            
        except Exception as e:
            print(f"❌ Error accessing Delta table: {e}")
            return None
        
        # All modes now use test_column_orderings_smart with the mode parameter
        return test_column_orderings_smart(
            self.con,
            table_path,
            table_name=table_name,  # Pass table name for cardinality calculation on full dataset
            mode=mode,
            min_distinct_threshold=min_distinct_threshold,
            max_cardinality_pct=max_cardinality_pct,
            max_ordering_depth=max_ordering_depth
        )

    def close(self):
        """Close DuckDB connection"""

        if self.con:
            self.con.close()
            print("Connection closed")


class WorkspaceConnection(WorkspaceOperationsMixin):
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