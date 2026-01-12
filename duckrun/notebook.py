"""
Notebook operations functionality for duckrun - Import notebooks from web or local path using Fabric REST API
"""
import requests
import base64
import os
import json
from typing import Optional, Literal


def deploy_notebook(
    path: str,
    overwrite: bool = False,
    workspace_name: Optional[str] = None,
    runtime: Literal["pyspark", "python"] = "python",
    parameters: Optional[dict] = None
) -> dict:
    """
    Deploy a Jupyter notebook from a web URL or local file path into Microsoft Fabric workspace using REST API only.
    Uses duckrun.connect context by default or explicit workspace name.
    
    Args:
        path: URL or local file path to the notebook file. Required.
              - For web: e.g., "https://raw.githubusercontent.com/user/repo/main/notebook.ipynb"
              - For local: e.g., "/path/to/notebook.ipynb" or "C:\\path\\to\\notebook.ipynb"
        overwrite: Whether to overwrite if notebook already exists (default: False)
        workspace_name: Target workspace name. Optional - will use current workspace from duckrun context if available.
        runtime: The notebook runtime - "pyspark" (default) or "python" for pure Python notebooks.
        parameters: Dictionary of parameters to inject into the notebook's parameters cell.
                   The notebook must have a cell tagged with "parameters" in its metadata.
                   Values will be added/updated as variable assignments in that cell.
                   Example: {"business_logic": "abfss://...", "nbr_of_files": 10}
        
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
        
        result = dr.deploy_notebook(
            path="https://raw.githubusercontent.com/user/repo/main/notebook.ipynb"
        )
        
        # With parameters injection
        result = dr.deploy_notebook(
            path="/fabric_demo/electricity.ipynb",
            parameters={
                "business_logic": "abfss://duckrun@onelake.dfs.fabric.microsoft.com/data.Lakehouse/Files/transformation",
                "nbr_of_files": 10
            }
        )
        
        # With overwrite
        result = dr.deploy_notebook(
            path="/fabric_demo/analysis/analysis.ipynb",
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
                "success": False,
                "message": f"Notebook '{notebook_name}' already exists. Use overwrite=True to replace.",
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
            
            # Normalize all cell sources to list format (Fabric API requires this)
            cells = notebook_json.get("cells", [])
            for cell in cells:
                source = cell.get("source", [])
                if isinstance(source, str):
                    # Convert string to list of lines
                    lines = source.split('\n')
                    cell["source"] = [line + '\n' for line in lines[:-1]] + [lines[-1]] if lines else []
            
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
            
            # Inject parameters by adding a new cell right after the parameters cell
            if parameters:
                parameters_injected = False
                cells = notebook_json.get("cells", [])
                
                for i, cell in enumerate(cells):
                    cell_metadata = cell.get("metadata", {})
                    tags = cell_metadata.get("tags", [])
                    
                    if "parameters" in tags and cell.get("cell_type") == "code":
                        # Found the parameters cell - insert new cell right after it
                        # Build new parameter assignments
                        new_assignments = []
                        for key, value in parameters.items():
                            if isinstance(value, str):
                                new_assignments.append(f'{key} = "{value}"')
                            else:
                                new_assignments.append(f'{key} = {repr(value)}')
                        
                        # Create new cell with injected parameters (tagged as "injected-parameters")
                        new_cell = {
                            "cell_type": "code",
                            "execution_count": None,
                            "metadata": {
                                "tags": ["injected-parameters"]
                            },
                            "outputs": [],
                            "source": [line + '\n' for line in new_assignments[:-1]] + [new_assignments[-1]] if new_assignments else []
                        }
                        
                        # Insert new cell right after the parameters cell
                        cells.insert(i + 1, new_cell)
                        parameters_injected = True
                        print(f"✓ Injected {len(parameters)} parameter(s) in new cell after parameters cell")
                        break
                
                if not parameters_injected:
                    print(f"⚠️ No cell tagged with 'parameters' found - parameters not injected")
            
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
                            "path": "notebook-content.ipynb",
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
                    operation_succeeded = _wait_for_operation(operation_id, headers)
                    if not operation_succeeded:
                        return {
                            "success": False,
                            "message": f"Notebook '{notebook_name}' update operation failed",
                            "notebook": existing_notebook,
                            "overwritten": False
                        }
            
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
                            "path": "notebook-content.ipynb",
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
                    operation_succeeded = _wait_for_operation(operation_id, headers)
                    if not operation_succeeded:
                        return {
                            "success": False,
                            "message": f"Notebook '{notebook_name}' create operation failed",
                            "notebook": None,
                            "overwritten": False
                        }
            
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


# Backward compatibility aliases
def import_notebook(
    path: str,
    overwrite: bool = False,
    workspace_name: Optional[str] = None,
    runtime: Literal["pyspark", "python"] = "python",
    parameters: Optional[dict] = None
) -> dict:
    """
    Alias for deploy_notebook for backward compatibility.
    Use deploy_notebook instead.
    """
    return deploy_notebook(
        path=path,
        overwrite=overwrite,
        workspace_name=workspace_name,
        runtime=runtime,
        parameters=parameters
    )


def import_notebook_from_web(
    url: str,
    overwrite: bool = False,
    workspace_name: Optional[str] = None,
    runtime: Literal["pyspark", "python"] = "python"
) -> dict:
    """
    Alias for deploy_notebook for backward compatibility.
    Use deploy_notebook instead - it supports both URLs and local file paths.
    
    Args:
        url: URL to the notebook file (e.g., GitHub raw URL). Required.
        overwrite: Whether to overwrite if notebook already exists (default: False)
        workspace_name: Target workspace name. Optional.
        runtime: The notebook runtime - "pyspark" (default) or "python".
        
    Returns:
        Dictionary with import result
    """
    return deploy_notebook(
        path=url,
        overwrite=overwrite,
        workspace_name=workspace_name,
        runtime=runtime
    )


def schedule_notebook(
    notebook_name: str,
    schedule_type: Literal["interval", "daily", "weekly", "monthly"] = "daily",
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    interval_minutes: Optional[int] = None,
    times: Optional[list] = None,
    weekdays: Optional[list] = None,
    day_of_month: Optional[int] = None,
    timezone: str = "UTC",
    workspace_name: Optional[str] = None,
    enabled: bool = True,
    overwrite: bool = False
) -> dict:
    """
    Schedule a notebook to run automatically in Microsoft Fabric workspace.
    Creates or updates a scheduled job for the specified notebook.
    
    Note: Fabric does NOT support traditional cron expressions. Instead it supports:
    - interval: Run every X minutes
    - daily: Run at specific times each day  
    - weekly: Run on specific days at specific times
    - monthly: Run on specific day of month at specific times
    
    Args:
        notebook_name: Name of the notebook to schedule (without .ipynb extension). Required.
        schedule_type: Type of schedule - "interval", "daily", "weekly", or "monthly" (default: "daily")
        start_time: Start datetime in ISO 8601 format (e.g., "2024-01-15T09:00:00Z").
                   If not provided, defaults to current time.
        end_time: End datetime in ISO 8601 format. Required for all schedules.
                 If not provided, defaults to 1 year from start_time.
        interval_minutes: For "interval" type only - run every X minutes (1 to 5270400).
                         Example: 60 for hourly, 1440 for daily.
        times: List of times in "HH:mm" format for daily/weekly/monthly schedules.
               Example: ["09:00", "18:00"] to run at 9 AM and 6 PM.
               Maximum 100 time slots.
        weekdays: For "weekly" type - list of days to run.
                 Example: ["Monday", "Wednesday", "Friday"]
        day_of_month: For "monthly" type - day of month (1-31).
                     Example: 1 for first day, 15 for mid-month.
        timezone: Windows timezone ID (default: "UTC").
                 Example: "Central Standard Time", "Pacific Standard Time"
        workspace_name: Target workspace name. Optional - uses current workspace if available.
        enabled: Whether the schedule should be enabled (default: True)
        overwrite: Whether to overwrite existing schedule (default: False).
                  If False and schedule exists, returns error.
        
    Returns:
        Dictionary with schedule result:
        {
            "success": bool,
            "message": str,
            "schedule": dict (if successful),
            "notebook_id": str
        }
        
    Examples:
        # Run every 60 minutes (hourly)
        result = schedule_notebook(
            notebook_name="my_etl_notebook",
            schedule_type="interval",
            interval_minutes=60,
            start_time="2024-01-15T00:00:00Z",
            end_time="2025-01-15T00:00:00Z"
        )
        
        # Run daily at 9 AM and 6 PM
        result = schedule_notebook(
            notebook_name="daily_report",
            schedule_type="daily",
            times=["09:00", "18:00"],
            timezone="Central Standard Time"
        )
        
        # Run weekly on Monday and Friday at 8 AM
        result = schedule_notebook(
            notebook_name="weekly_report",
            schedule_type="weekly",
            weekdays=["Monday", "Friday"],
            times=["08:00"],
            timezone="Pacific Standard Time"
        )
        
        # Run monthly on the 1st at 6 AM
        result = schedule_notebook(
            notebook_name="monthly_report",
            schedule_type="monthly",
            day_of_month=1,
            times=["06:00"]
        )
    """
    try:
        from datetime import datetime, timedelta
        
        # Get authentication token
        from duckrun.auth import get_fabric_api_token
        token = get_fabric_api_token()
        if not token:
            return {
                "success": False,
                "message": "Failed to get authentication token",
                "schedule": None,
                "notebook_id": None
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
                import notebookutils  # type: ignore
                workspace_id = notebookutils.runtime.context.get("workspaceId")
                print("📓 Using current workspace from Fabric notebook context")
            except (ImportError, Exception):
                pass
        
        # If still no workspace_id, resolve from workspace_name
        if not workspace_id:
            if not workspace_name:
                return {
                    "success": False,
                    "message": "workspace_name must be provided when not in Fabric notebook context",
                    "schedule": None,
                    "notebook_id": None
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
                    "schedule": None,
                    "notebook_id": None
                }
            
            workspace_id = workspace.get("id")
            print(f"✓ Found workspace: {workspace_name}")
        
        # Find the notebook by name
        print(f"🔍 Finding notebook: {notebook_name}")
        notebooks_url = f"{base_url}/workspaces/{workspace_id}/notebooks"
        response = requests.get(notebooks_url, headers=headers)
        response.raise_for_status()
        
        notebooks = response.json().get("value", [])
        notebook = next((nb for nb in notebooks if nb.get("displayName") == notebook_name), None)
        
        if not notebook:
            return {
                "success": False,
                "message": f"Notebook '{notebook_name}' not found in workspace",
                "schedule": None,
                "notebook_id": None
            }
        
        notebook_id = notebook.get("id")
        print(f"✓ Found notebook: {notebook_name} (ID: {notebook_id})")
        
        # Set default start_time and end_time if not provided
        if not start_time:
            start_dt = datetime.utcnow()
            start_time = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        
        if not end_time:
            # Default to 1 year from now
            end_dt = datetime.utcnow() + timedelta(days=365)
            end_time = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        
        print(f"⏰ Schedule: {start_time} to {end_time}")
        
        # Build the schedule configuration based on type
        # Fabric API uses "Cron" for interval, but it's NOT a cron expression!
        config = {
            "startDateTime": start_time,
            "endDateTime": end_time,
            "localTimeZoneId": timezone
        }
        
        if schedule_type == "interval":
            if not interval_minutes:
                return {
                    "success": False,
                    "message": "interval_minutes is required for 'interval' schedule type",
                    "schedule": None,
                    "notebook_id": notebook_id
                }
            config["type"] = "Cron"  # Fabric calls interval-based schedules "Cron" (confusingly)
            config["interval"] = interval_minutes
            print(f"📅 Setting interval schedule: every {interval_minutes} minutes")
            
        elif schedule_type == "daily":
            config["type"] = "Daily"
            config["times"] = times or ["09:00"]  # Default to 9 AM
            print(f"📅 Setting daily schedule at: {', '.join(config['times'])}")
            
        elif schedule_type == "weekly":
            config["type"] = "Weekly"
            config["times"] = times or ["09:00"]
            config["weekdays"] = weekdays or ["Monday"]
            print(f"📅 Setting weekly schedule: {', '.join(config['weekdays'])} at {', '.join(config['times'])}")
            
        elif schedule_type == "monthly":
            config["type"] = "Monthly"
            config["times"] = times or ["09:00"]
            config["recurrence"] = 1  # Every month
            if day_of_month:
                config["occurrence"] = {
                    "occurrenceType": "DayOfMonth",
                    "dayOfMonth": day_of_month
                }
            else:
                config["occurrence"] = {
                    "occurrenceType": "DayOfMonth",
                    "dayOfMonth": 1  # Default to 1st of month
                }
            print(f"📅 Setting monthly schedule: day {config['occurrence']['dayOfMonth']} at {', '.join(config['times'])}")
        
        schedule_payload = {
            "enabled": enabled,
            "configuration": config
        }
        
        # Create or update the job schedule for the notebook
        # Using the Fabric Job Scheduler API - notebooks use "RunNotebook" job type
        schedules_url = f"{base_url}/workspaces/{workspace_id}/items/{notebook_id}/jobs/RunNotebook/schedules"
        
        try:
            response = requests.get(schedules_url, headers=headers)
            
            if response.status_code == 200:
                existing_schedules = response.json().get("value", [])
                
                if existing_schedules:
                    if not overwrite:
                        return {
                            "success": False,
                            "message": f"Schedule already exists for notebook '{notebook_name}'. Use overwrite=True to update.",
                            "schedule": existing_schedules[0],
                            "notebook_id": notebook_id
                        }
                    
                    # Overwrite existing schedule
                    schedule_id = existing_schedules[0].get("id")
                    print(f"🔄 Overwriting existing schedule: {schedule_id}")
                    
                    update_url = f"{schedules_url}/{schedule_id}"
                    response = requests.patch(update_url, headers=headers, json=schedule_payload)
                    response.raise_for_status()
                    
                    return {
                        "success": True,
                        "message": f"Schedule overwritten for notebook '{notebook_name}'",
                        "schedule": schedule_payload,
                        "notebook_id": notebook_id,
                        "overwritten": True
                    }
        except requests.exceptions.RequestException:
            # No existing schedule, will create new one
            pass
        
        # Create new schedule
        print(f"➕ Creating new schedule for notebook: {notebook_name}")
        
        response = requests.post(schedules_url, headers=headers, json=schedule_payload)
        response.raise_for_status()
        
        # Handle long-running operation
        if response.status_code == 202:
            operation_id = response.headers.get('x-ms-operation-id')
            if operation_id:
                operation_succeeded = _wait_for_operation(operation_id, headers)
                if not operation_succeeded:
                    return {
                        "success": False,
                        "message": f"Schedule creation operation failed for notebook '{notebook_name}'",
                        "schedule": None,
                        "notebook_id": notebook_id
                    }
        
        created_schedule = response.json() if response.status_code in [200, 201] else schedule_payload
        
        status_msg = "enabled" if enabled else "disabled (paused)"
        return {
            "success": True,
            "message": f"Schedule created for notebook '{notebook_name}' - {status_msg}",
            "schedule": created_schedule,
            "notebook_id": notebook_id
        }
        
    except requests.exceptions.RequestException as e:
        return {
            "success": False,
            "message": f"HTTP Error: {str(e)}",
            "schedule": None,
            "notebook_id": None
        }
    except Exception as e:
        return {
            "success": False,
            "message": f"Error: {str(e)}",
            "schedule": None,
            "notebook_id": None
        }
