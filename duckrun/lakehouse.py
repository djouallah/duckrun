import requests
import time
from typing import Optional

class FabricLakehouseManager:
    """
    Manage Microsoft Fabric Lakehouses using REST API only.
    Works on any machine with Python and internet access.
    """
    
    def __init__(self, access_token: str):
        """
        Initialize with Azure AD access token.
        
        Args:
            access_token: Bearer token for Fabric API authentication
        """
        self.base_url = "https://api.fabric.microsoft.com/v1"
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
    
    def get_workspace_id(self, workspace_name: str) -> Optional[str]:
        """
        Get workspace ID from workspace name.
        
        Args:
            workspace_name: Name of the workspace
            
        Returns:
            Workspace ID if found, None otherwise
        """
        if not workspace_name:
            return None
        
        try:
            url = f"{self.base_url}/workspaces"
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            
            workspaces = response.json().get("value", [])
            for workspace in workspaces:
                if workspace.get("displayName") == workspace_name:
                    return workspace.get("id")
            
            print(f"Workspace '{workspace_name}' not found")
            return None
            
        except Exception as e:
            print(f"Error getting workspace ID: {e}")
            return None
    
    def get_lakehouse(self, lakehouse_name: str, workspace_id: str) -> Optional[dict]:
        """
        Get lakehouse details if it exists.
        
        Args:
            lakehouse_name: Name of the lakehouse
            workspace_id: ID of the workspace
            
        Returns:
            Lakehouse details if found, None otherwise
        """
        try:
            url = f"{self.base_url}/workspaces/{workspace_id}/lakehouses"
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            
            lakehouses = response.json().get("value", [])
            for lakehouse in lakehouses:
                if lakehouse.get("displayName") == lakehouse_name:
                    return lakehouse
            
            return None
            
        except Exception as e:
            print(f"Error getting lakehouse: {e}")
            return None
    
    def create_lakehouse(self, lakehouse_name: str, workspace_id: str, 
                        enable_schemas: bool = True) -> Optional[dict]:
        """
        Create a new lakehouse.
        
        Args:
            lakehouse_name: Name of the lakehouse
            workspace_id: ID of the workspace
            enable_schemas: Whether to enable schemas
            
        Returns:
            Created lakehouse details if successful, None otherwise
        """
        try:
            url = f"{self.base_url}/workspaces/{workspace_id}/lakehouses"
            payload = {
                "displayName": lakehouse_name,
                "description": f"Lakehouse {lakehouse_name}"
            }
            
            if enable_schemas:
                payload["creationPayload"] = {
                    "enableSchemas": True
                }
            
            response = requests.post(url, headers=self.headers, json=payload)
            response.raise_for_status()
            
            # Wait a bit for the lakehouse to be fully provisioned
            time.sleep(2)
            
            return response.json()
            
        except Exception as e:
            print(f"Error creating lakehouse: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Response: {e.response.text}")
            return None
    
    def create_lakehouse_if_not_exists(self, lakehouse_name: str, 
                                       workspace_name: Optional[str] = None,
                                       workspace_id: Optional[str] = None) -> int:
        """
        Create a lakehouse if it doesn't exist.
        
        Args:
            lakehouse_name: Name of the lakehouse
            workspace_name: Optional workspace name
            workspace_id: Optional workspace ID (takes precedence over workspace_name)
            
        Returns:
            1 if successful (lakehouse exists or was created)
            0 if failed
        """
        # Resolve workspace ID
        if workspace_id is None and workspace_name:
            workspace_id = self.get_workspace_id(workspace_name)
            if workspace_id is None:
                print(f"Workspace '{workspace_name}' not found - returning 0")
                return 0
        elif workspace_id is None:
            print("No workspace specified - returning 0")
            return 0
        
        print(f"Attempting to get lakehouse '{lakehouse_name}' in workspace '{workspace_id}'")
        
        # Check if lakehouse exists
        lakehouse = self.get_lakehouse(lakehouse_name, workspace_id)
        
        if lakehouse:
            print(f"Lakehouse '{lakehouse_name}' found - returning 1")
            return 1
        
        # Create lakehouse if it doesn't exist
        print(f"Lakehouse not found, attempting to create...")
        created = self.create_lakehouse(lakehouse_name, workspace_id)
        
        if created:
            # Verify creation
            lakehouse = self.get_lakehouse(lakehouse_name, workspace_id)
            if lakehouse:
                print(f"Lakehouse '{lakehouse_name}' created successfully - returning 1")
                return 1
        
        print(f"Failed to create lakehouse '{lakehouse_name}' - returning 0")
        return 0


# Example usage with Azure Identity:
def main():
    """
    Example of how to use the FabricLakehouseManager with azure-identity.
    """
    from azure.identity import AzureCliCredential, InteractiveBrowserCredential, ChainedTokenCredential
    
    print("Authenticating with Azure (trying CLI, will fallback to browser if needed)...")
    
    # Create credential chain (CLI first, then interactive browser)
    credential = ChainedTokenCredential(
        AzureCliCredential(),
        InteractiveBrowserCredential()
    )
    
    # Get token for Fabric API (not storage!)
    # Note: Use Fabric API scope, not storage scope
    token = credential.get_token("https://api.fabric.microsoft.com/.default")
    
    print("✓ Authentication successful!")
    
    # Initialize manager with Fabric token
    manager = FabricLakehouseManager(token.token)
    
    # Create lakehouse if not exists
    result = manager.create_lakehouse_if_not_exists(
        lakehouse_name="MyLakehouse",
        workspace_name="MyWorkspace"
    )
    
    if result == 1:
        print("✓ Lakehouse operation successful!")
    else:
        print("✗ Lakehouse operation failed!")
    
    return result


def get_fabric_token():
    """
    Helper function to get Fabric API token.
    Returns the token string.
    """
    from azure.identity import AzureCliCredential, InteractiveBrowserCredential, ChainedTokenCredential
    
    credential = ChainedTokenCredential(
        AzureCliCredential(),
        InteractiveBrowserCredential()
    )
    
    # Get token for Fabric API
    token = credential.get_token("https://api.fabric.microsoft.com/.default")
    return token.token


def create_lakehouse_in_notebook(lakehouse_name: str, workspace_name: Optional[str] = None) -> int:
    """
    Create a lakehouse in a Fabric notebook environment.
    This function uses the notebook's built-in authentication.
    
    Args:
        lakehouse_name: Name of the lakehouse to create
        workspace_name: Optional workspace name (uses current workspace if None)
        
    Returns:
        1 if successful (lakehouse exists or was created)
        0 if failed
    """
    try:
        # Try to import fabric notebook utilities (only available in Fabric notebooks)
        import notebookutils  # type: ignore
        
        # Get authentication token from notebook environment
        token = notebookutils.credentials.getToken("https://api.fabric.microsoft.com/.default")
        
        # Initialize manager with notebook token
        manager = FabricLakehouseManager(token)
        
        # Get current workspace ID if no workspace specified
        workspace_id = None
        if workspace_name:
            workspace_id = manager.get_workspace_id(workspace_name)
        else:
            # In Fabric notebooks, we can get the current workspace from context
            try:
                workspace_id = notebookutils.runtime.context.get("workspaceId")
            except:
                print("Could not get current workspace ID from notebook context")
                return 0
        
        if not workspace_id:
            print(f"Could not resolve workspace ID")
            return 0
        
        # Create lakehouse if not exists
        return manager.create_lakehouse_if_not_exists(
            lakehouse_name=lakehouse_name,
            workspace_id=workspace_id
        )
        
    except ImportError:
        print("notebookutils not available - not running in Fabric notebook environment")
        print("Use FabricLakehouseManager class directly with proper authentication")
        return 0
    except Exception as e:
        print(f"Error creating lakehouse in notebook: {e}")
        return 0


def create_lakehouse_simple(lakehouse_name: str, access_token: str, workspace_id: str) -> dict:
    """
    Simple function to create a lakehouse with minimal dependencies.
    Perfect for Fabric notebook environments.
    
    Args:
        lakehouse_name: Name of the lakehouse to create
        access_token: Bearer token for authentication
        workspace_id: ID of the target workspace
        
    Returns:
        Dictionary with creation result
    """
    import requests
    import time
    
    base_url = "https://api.fabric.microsoft.com/v1"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    try:
        # First check if lakehouse already exists
        list_url = f"{base_url}/workspaces/{workspace_id}/lakehouses"
        response = requests.get(list_url, headers=headers)
        response.raise_for_status()
        
        lakehouses = response.json().get("value", [])
        for lakehouse in lakehouses:
            if lakehouse.get("displayName") == lakehouse_name:
                return {
                    "success": True,
                    "message": f"Lakehouse '{lakehouse_name}' already exists",
                    "lakehouse": lakehouse,
                    "created": False
                }
        
        # Create new lakehouse
        create_url = f"{base_url}/workspaces/{workspace_id}/lakehouses"
        payload = {
            "displayName": lakehouse_name,
            "description": f"Lakehouse {lakehouse_name} created via API"
        }
        
        response = requests.post(create_url, headers=headers, json=payload)
        response.raise_for_status()
        
        # Wait for provisioning
        time.sleep(3)
        
        created_lakehouse = response.json()
        return {
            "success": True,
            "message": f"Lakehouse '{lakehouse_name}' created successfully",
            "lakehouse": created_lakehouse,
            "created": True
        }
        
    except requests.exceptions.RequestException as e:
        error_msg = f"HTTP error creating lakehouse: {e}"
        if hasattr(e, 'response') and e.response is not None:
            error_msg += f" Response: {e.response.text}"
        
        return {
            "success": False,
            "message": error_msg,
            "lakehouse": None,
            "created": False
        }
    except Exception as e:
        return {
            "success": False,
            "message": f"Unexpected error: {e}",
            "lakehouse": None,
            "created": False
        }


if __name__ == "__main__":
    # Uncomment to run the example
    # main()
    pass


# Usage Examples:
"""
# Example 1: In a Fabric Notebook (simplest approach)
from duckrun.lakehouse import create_lakehouse_in_notebook

result = create_lakehouse_in_notebook("MyNewLakehouse")
if result == 1:
    print("Lakehouse created or already exists!")

# Example 2: In a Fabric Notebook with explicit token
import notebookutils
from duckrun.lakehouse import create_lakehouse_simple

token = notebookutils.credentials.getToken("https://api.fabric.microsoft.com/.default")
workspace_id = notebookutils.runtime.context.get("workspaceId")

result = create_lakehouse_simple("MyLakehouse", token, workspace_id)
print(f"Result: {result['message']}")

# Example 3: Outside Fabric (requires azure-identity package)
from duckrun.lakehouse import FabricLakehouseManager, get_fabric_token

token = get_fabric_token()
manager = FabricLakehouseManager(token)
result = manager.create_lakehouse_if_not_exists("MyLakehouse", workspace_name="MyWorkspace")

# Example 4: With explicit workspace and lakehouse details
from duckrun.lakehouse import FabricLakehouseManager

# Get your token however you prefer
token = "your_bearer_token_here"
manager = FabricLakehouseManager(token)

# Create lakehouse in specific workspace
workspace_id = manager.get_workspace_id("Production Workspace")
lakehouse = manager.create_lakehouse("DataLake2024", workspace_id, enable_schemas=True)

if lakehouse:
    print(f"Created lakehouse with ID: {lakehouse['id']}")
"""