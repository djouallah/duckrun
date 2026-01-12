"""
Notebook operations functionality for duckrun - Import notebooks from web or local path using Fabric REST API
"""
import requests
import base64
import os
import json
from typing import Optional, Literal


def import_notebook(
    path: str,
    overwrite: bool = False,
    workspace_name: Optional[str] = None,
    runtime: Literal["pyspark", "python"] = "python"
) -> dict:
    """
    Import a Jupyter notebook from a web URL or local file path into Microsoft Fabric workspace using REST API only.
    Uses duckrun.connect context by default or explicit workspace name.
    
    Args:
        path: URL or local file path to the notebook file. Required.
              - For web: e.g., "https://raw.githubusercontent.com/user/repo/main/notebook.ipynb"
              - For local: e.g., "/path/to/notebook.ipynb" or "C:\\path\\to\\notebook.ipynb"
        overwrite: Whether to overwrite if notebook already exists (default: False)
        workspace_name: Target workspace name. Optional - will use current workspace from duckrun context if available.
        runtime: The notebook runtime - "pyspark" (default) or "python" for pure Python notebooks.
        
    Returns:
        Dictionary with import result:
        {
            "success": bool,
            "message": str,
            "notebook": dict (if successful),
            "overwritten": bool
        }
        
    Examples:
        # Basic usage with duckrun context - from web URL
        import duckrun
        dr = duckrun.connect("MyWorkspace/MyLakehouse.lakehouse")
        from duckrun.notebook import import_notebook
        
        result = import_notebook(
            path="https://raw.githubusercontent.com/user/repo/main/notebook.ipynb"
        )
        
        # From local file path with Python runtime
        result = import_notebook(
            path="/fabric_demo/analysis/analysis.ipynb",
            runtime="python"
        )
        
        # With overwrite
        result = import_notebook(
            path="https://raw.githubusercontent.com/user/repo/main/notebook.ipynb",
            overwrite=True
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
        
        # Derive notebook name from path
        notebook_name = os.path.basename(path.rstrip('/'))
        if notebook_name.endswith(".ipynb"):
            notebook_name = notebook_name[:-6]  # Remove .ipynb extension
        print(f"📝 Using notebook name: {notebook_name}")
        
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
        
        # Check if path is a URL or local file path
        is_url = path.lower().startswith(('http://', 'https://'))
        
        if is_url:
            # Download notebook content from URL
            print(f"⬇️ Downloading notebook from: {path}")
            response = requests.get(path)
            response.raise_for_status()
            notebook_content = response.text
            print(f"✓ Notebook downloaded successfully")
        else:
            # Read notebook content from local file
            print(f"📂 Reading notebook from local path: {path}")
            if not os.path.exists(path):
                return {
                    "success": False,
                    "message": f"Local file not found: {path}",
                    "notebook": None,
                    "overwritten": False
                }
            with open(path, 'r', encoding='utf-8') as f:
                notebook_content = f.read()
            print(f"✓ Notebook read successfully")
        
        # Parse and modify notebook metadata to set the correct runtime
        try:
            notebook_json = json.loads(notebook_content)
            
            # Set the kernel based on runtime parameter
            if runtime == "python":
                kernel_name = "python3"
                language_group = "jupyter_python"
            else:  # pyspark
                kernel_name = "synapse_pyspark"
                language_group = "synapse_pyspark"
            
            # Ensure metadata structure exists
            if "metadata" not in notebook_json:
                notebook_json["metadata"] = {}
            
            # Set kernelspec
            notebook_json["metadata"]["kernelspec"] = {
                "name": kernel_name,
                "display_name": kernel_name,
                "language": "python"
            }
            
            # Set kernel_info (Fabric-specific)
            notebook_json["metadata"]["kernel_info"] = {
                "name": "jupyter" if runtime == "python" else "synapse_pyspark"
            }
            
            # Set microsoft metadata (Fabric-specific)
            notebook_json["metadata"]["microsoft"] = {
                "language": "python",
                "language_group": language_group
            }
            
            # Set language_info
            notebook_json["metadata"]["language_info"] = {
                "name": "python"
            }
            
            # Convert back to string
            notebook_content = json.dumps(notebook_json, indent=2)
            print(f"✓ Set notebook runtime to: {runtime}")
            
        except json.JSONDecodeError:
            print(f"⚠️ Could not parse notebook JSON, using original content")
        
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


# Backward compatibility alias
def import_notebook_from_web(
    url: str,
    overwrite: bool = False,
    workspace_name: Optional[str] = None,
    runtime: Literal["pyspark", "python"] = "python"
) -> dict:
    """
    Alias for import_notebook for backward compatibility.
    Use import_notebook instead - it supports both URLs and local file paths.
    
    Args:
        url: URL to the notebook file (e.g., GitHub raw URL). Required.
        overwrite: Whether to overwrite if notebook already exists (default: False)
        workspace_name: Target workspace name. Optional.
        runtime: The notebook runtime - "pyspark" (default) or "python".
        
    Returns:
        Dictionary with import result
    """
    return import_notebook(
        path=url,
        overwrite=overwrite,
        workspace_name=workspace_name,
        runtime=runtime
    )
