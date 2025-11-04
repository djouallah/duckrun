"""
Notebook operations functionality for duckrun - Import notebooks from web using Fabric REST API
"""
import requests
import base64
from typing import Optional


def import_notebook_from_web(
    url: str,
    notebook_name: Optional[str] = None,
    overwrite: bool = False,
    workspace_name: Optional[str] = None
) -> dict:
    """
    Import a Jupyter notebook from a web URL into Microsoft Fabric workspace using REST API only.
    Uses duckrun.connect context by default or explicit workspace name.
    
    Args:
        url: URL to the notebook file (e.g., GitHub raw URL). Required.
        notebook_name: Name for the imported notebook in Fabric. Optional - will use filename from URL if not provided.
        overwrite: Whether to overwrite if notebook already exists (default: False)
        workspace_name: Target workspace name. Optional - will use current workspace from duckrun context if available.
        
    Returns:
        Dictionary with import result:
        {
            "success": bool,
            "message": str,
            "notebook": dict (if successful),
            "overwritten": bool
        }
        
    Examples:
        # Basic usage with duckrun context
        import duckrun
        dr = duckrun.connect("MyWorkspace/MyLakehouse.lakehouse")
        from duckrun.notebook import import_notebook_from_web
        
        result = import_notebook_from_web(
            url="https://raw.githubusercontent.com/user/repo/main/notebook.ipynb",
            notebook_name="MyNotebook"
        )
        
        # With explicit workspace
        result = import_notebook_from_web(
            url="https://raw.githubusercontent.com/user/repo/main/notebook.ipynb",
            notebook_name="MyNotebook",
            workspace_name="Analytics Workspace",
            overwrite=True
        )
        
        # Minimal usage - derives name from URL
        result = import_notebook_from_web(
            url="https://raw.githubusercontent.com/user/repo/main/RunPerfScenario.ipynb"
        )
    """
    try:
        # Get authentication token
        from duckrun.auth import get_fabric_api_token
        token = get_fabric_api_token()
        if not token:
            return {
                "success": False,
                "message": "Failed to get authentication token",
                "notebook": None,
                "overwritten": False
            }
        
        base_url = "https://api.fabric.microsoft.com/v1"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        # Determine workspace ID
        workspace_id = None
        
        # Try to get from duckrun context if not provided
        if not workspace_name:
            try:
                # Try to get from notebook context first
                import notebookutils  # type: ignore
                workspace_id = notebookutils.runtime.context.get("workspaceId")
                print("📓 Using current workspace from Fabric notebook context")
            except (ImportError, Exception):
                # Not in notebook, try to get from environment/last connection
                pass
        
        # If still no workspace_id, resolve from workspace_name
        if not workspace_id:
            if not workspace_name:
                return {
                    "success": False,
                    "message": "workspace_name must be provided when not in Fabric notebook context",
                    "notebook": None,
                    "overwritten": False
                }
            
            # Get workspace ID by name
            print(f"🔍 Resolving workspace: {workspace_name}")
            ws_url = f"{base_url}/workspaces"
            response = requests.get(ws_url, headers=headers)
            response.raise_for_status()
            
            workspaces = response.json().get("value", [])
            workspace = next((ws for ws in workspaces if ws.get("displayName") == workspace_name), None)
            
            if not workspace:
                return {
                    "success": False,
                    "message": f"Workspace '{workspace_name}' not found",
                    "notebook": None,
                    "overwritten": False
                }
            
            workspace_id = workspace.get("id")
            print(f"✓ Found workspace: {workspace_name}")
        
        # Derive notebook name from URL if not provided
        if not notebook_name:
            # Extract filename from URL
            notebook_name = url.split("/")[-1]
            if notebook_name.endswith(".ipynb"):
                notebook_name = notebook_name[:-6]  # Remove .ipynb extension
            print(f"📝 Using notebook name from URL: {notebook_name}")
        
        # Check if notebook already exists
        notebooks_url = f"{base_url}/workspaces/{workspace_id}/notebooks"
        response = requests.get(notebooks_url, headers=headers)
        response.raise_for_status()
        
        notebooks = response.json().get("value", [])
        existing_notebook = next((nb for nb in notebooks if nb.get("displayName") == notebook_name), None)
        
        if existing_notebook and not overwrite:
            return {
                "success": True,
                "message": f"Notebook '{notebook_name}' already exists (use overwrite=True to replace)",
                "notebook": existing_notebook,
                "overwritten": False
            }
        
        # Download notebook content from URL
        print(f"⬇️ Downloading notebook from: {url}")
        response = requests.get(url)
        response.raise_for_status()
        notebook_content = response.text
        print(f"✓ Notebook downloaded successfully")
        
        # Convert notebook content to base64
        notebook_base64 = base64.b64encode(notebook_content.encode('utf-8')).decode('utf-8')
        
        # Prepare the payload for creating/updating the notebook
        if existing_notebook and overwrite:
            # Update existing notebook
            notebook_id = existing_notebook.get("id")
            print(f"🔄 Updating existing notebook: {notebook_name}")
            
            update_url = f"{base_url}/workspaces/{workspace_id}/notebooks/{notebook_id}/updateDefinition"
            payload = {
                "definition": {
                    "format": "ipynb",
                    "parts": [
                        {
                            "path": "notebook-content.py",
                            "payload": notebook_base64,
                            "payloadType": "InlineBase64"
                        }
                    ]
                }
            }
            
            response = requests.post(update_url, headers=headers, json=payload)
            response.raise_for_status()
            
            # Handle long-running operation
            if response.status_code == 202:
                operation_id = response.headers.get('x-ms-operation-id')
                if operation_id:
                    _wait_for_operation(operation_id, headers)
            
            return {
                "success": True,
                "message": f"Notebook '{notebook_name}' updated successfully",
                "notebook": existing_notebook,
                "overwritten": True
            }
        else:
            # Create new notebook
            print(f"➕ Creating new notebook: {notebook_name}")
            
            payload = {
                "displayName": notebook_name,
                "definition": {
                    "format": "ipynb",
                    "parts": [
                        {
                            "path": "notebook-content.py",
                            "payload": notebook_base64,
                            "payloadType": "InlineBase64"
                        }
                    ]
                }
            }
            
            response = requests.post(notebooks_url, headers=headers, json=payload)
            response.raise_for_status()
            
            # Handle long-running operation
            if response.status_code == 202:
                operation_id = response.headers.get('x-ms-operation-id')
                if operation_id:
                    _wait_for_operation(operation_id, headers)
            
            created_notebook = response.json()
            
            return {
                "success": True,
                "message": f"Notebook '{notebook_name}' created successfully",
                "notebook": created_notebook,
                "overwritten": False
            }
            
    except requests.exceptions.RequestException as e:
        return {
            "success": False,
            "message": f"HTTP Error: {str(e)}",
            "notebook": None,
            "overwritten": False
        }
    except Exception as e:
        return {
            "success": False,
            "message": f"Error: {str(e)}",
            "notebook": None,
            "overwritten": False
        }


def _wait_for_operation(operation_id: str, headers: dict, max_attempts: int = 30) -> bool:
    """
    Wait for a long-running Fabric API operation to complete.
    
    Args:
        operation_id: The operation ID to monitor
        headers: Request headers with authentication
        max_attempts: Maximum number of polling attempts (default: 30)
        
    Returns:
        True if operation succeeded, False otherwise
    """
    import time
    
    status_url = f"https://api.fabric.microsoft.com/v1/operations/{operation_id}"
    
    for attempt in range(max_attempts):
        time.sleep(2)
        
        try:
            response = requests.get(status_url, headers=headers)
            response.raise_for_status()
            
            status_data = response.json()
            status = status_data.get('status')
            
            if status == 'Succeeded':
                print(f"✓ Operation completed successfully")
                return True
            elif status == 'Failed':
                error = status_data.get('error', {})
                print(f"❌ Operation failed: {error.get('message', 'Unknown error')}")
                return False
            else:
                print(f"⏳ Operation in progress... ({status})")
                
        except Exception as e:
            print(f"⚠️ Error checking operation status: {e}")
            return False
    
    print(f"⚠️ Operation timed out after {max_attempts} attempts")
    return False


# Convenience wrapper for the try-except pattern mentioned in the request
def import_notebook(
    url: str,
    notebook_name: Optional[str] = None,
    overwrite: bool = False,
    workspace_name: Optional[str] = None
) -> None:
    """
    Convenience wrapper that prints results and handles errors.
    
    Args:
        url: URL to the notebook file
        notebook_name: Name for the imported notebook
        overwrite: Whether to overwrite if exists
        workspace_name: Target workspace name
        
    Examples:
        from duckrun.notebook import import_notebook
        
        import_notebook(
            url="https://raw.githubusercontent.com/djouallah/fabric_demo/refs/heads/main/Benchmark/RunPerfScenario.ipynb",
            notebook_name="RunPerfScenario",
            overwrite=False
        )
    """
    try:
        result = import_notebook_from_web(
            url=url,
            notebook_name=notebook_name,
            overwrite=overwrite,
            workspace_name=workspace_name
        )
        
        if result["success"]:
            print(f"✅ {result['message']}")
        else:
            print(f"❌ {result['message']}")
            
    except Exception as e:
        print(f"Error: {e}")
