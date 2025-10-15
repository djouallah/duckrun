"""
Semantic Model Deployer - DirectLake mode for Fabric Lakehouses
Uses duckrun's authentication. Works anywhere duckrun works.
"""

import requests
import json
import time
import base64


class FabricRestClient:
    """Fabric REST API client using duckrun's authentication."""
    
    def __init__(self):
        self.base_url = "https://api.fabric.microsoft.com"
        self.token = None
        self._get_token()
    
    def _get_token(self):
        """Get Fabric API token using duckrun's auth module"""
        from duckrun.auth import get_fabric_api_token
        self.token = get_fabric_api_token()
        if not self.token:
            raise Exception("Failed to get Fabric API token")
    
    def _get_headers(self):
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
    
    def get(self, endpoint: str):
        url = f"{self.base_url}{endpoint}"
        response = requests.get(url, headers=self._get_headers())
        response.raise_for_status()
        return response
    
    def post(self, endpoint: str, json: dict = None):
        url = f"{self.base_url}{endpoint}"
        response = requests.post(url, headers=self._get_headers(), json=json)
        response.raise_for_status()
        return response


def get_workspace_id(workspace_name, client):
    """Get workspace ID by name"""
    response = client.get("/v1/workspaces")
    workspaces = response.json().get('value', [])
    
    workspace_match = next((ws for ws in workspaces if ws.get('displayName') == workspace_name), None)
    if not workspace_match:
        raise ValueError(f"Workspace '{workspace_name}' not found")
    
    workspace_id = workspace_match['id']
    print(f"✓ Found workspace: {workspace_name}")
    return workspace_id


def get_lakehouse_id(lakehouse_name, workspace_id, client):
    """Get lakehouse ID by name"""
    response = client.get(f"/v1/workspaces/{workspace_id}/lakehouses")
    items = response.json().get('value', [])
    
    lakehouse_match = next((item for item in items if item.get('displayName') == lakehouse_name), None)
    if not lakehouse_match:
        raise ValueError(f"Lakehouse '{lakehouse_name}' not found")
    
    lakehouse_id = lakehouse_match['id']
    print(f"✓ Found lakehouse: {lakehouse_name}")
    return lakehouse_id


def get_dataset_id(dataset_name, workspace_id, client):
    """Get dataset ID by name"""
    response = client.get(f"/v1/workspaces/{workspace_id}/semanticModels")
    items = response.json().get('value', [])
    
    dataset_match = next((item for item in items if item.get('displayName') == dataset_name), None)
    if not dataset_match:
        raise ValueError(f"Dataset '{dataset_name}' not found")
    
    return dataset_match['id']


def check_dataset_exists(dataset_name, workspace_id, client):
    """Check if dataset already exists"""
    try:
        get_dataset_id(dataset_name, workspace_id, client)
        print(f"⚠️  Dataset '{dataset_name}' already exists")
        return True
    except:
        print(f"✓ Dataset name '{dataset_name}' is available")
        return False


def refresh_dataset(dataset_name, workspace_id, client):
    """Refresh a dataset and monitor progress"""
    dataset_id = get_dataset_id(dataset_name, workspace_id, client)
    
    payload = {
        "type": "full",
        "commitMode": "transactional",
        "maxParallelism": 10,
        "retryCount": 2,
        "objects": []
    }
    
    response = client.post(
        f"/v1/workspaces/{workspace_id}/semanticModels/{dataset_id}/refreshes",
        json=payload
    )
    
    if response.status_code in [200, 202]:
        print(f"✓ Refresh initiated")
        
        refresh_id = response.json().get('id')
        if refresh_id:
            print("   Monitoring refresh progress...")
            max_attempts = 60
            for attempt in range(max_attempts):
                time.sleep(5)
                
                status_response = client.get(
                    f"/v1/workspaces/{workspace_id}/semanticModels/{dataset_id}/refreshes/{refresh_id}"
                )
                status = status_response.json().get('status')
                
                if status == 'Completed':
                    print(f"✓ Refresh completed successfully")
                    return
                elif status == 'Failed':
                    error = status_response.json().get('error', {})
                    raise Exception(f"Refresh failed: {error.get('message', 'Unknown error')}")
                elif status == 'Cancelled':
                    raise Exception("Refresh was cancelled")
                
                if attempt % 6 == 0:
                    print(f"   Status: {status}...")
            
            raise Exception(f"Refresh timed out")


def download_bim_from_github(url):
    """Download BIM file from URL"""
    print(f"Downloading BIM file...")
    response = requests.get(url)
    response.raise_for_status()
    bim_content = response.json()
    print(f"✓ BIM file downloaded")
    print(f"  - Tables: {len(bim_content.get('model', {}).get('tables', []))}")
    print(f"  - Relationships: {len(bim_content.get('model', {}).get('relationships', []))}")
    return bim_content


def update_bim_for_directlake(bim_content, workspace_id, lakehouse_id, schema_name):
    """Update BIM file for DirectLake mode"""
    
    new_url = f"https://onelake.dfs.fabric.microsoft.com/{workspace_id}/{lakehouse_id}"
    expression_name = None
    
    # Update or create DirectLake expression
    if 'model' in bim_content and 'expressions' in bim_content['model']:
        for expr in bim_content['model']['expressions']:
            if 'DirectLake' in expr['name'] or expr.get('kind') == 'm':
                expression_name = expr['name']
                expr['expression'] = [
                    "let",
                    f"    Source = AzureStorage.DataLake(\"{new_url}\", [HierarchicalNavigation=true])",
                    "in",
                    "    Source"
                ]
                break
    
    if not expression_name:
        expression_name = f"DirectLake - {schema_name}"
        if 'expressions' not in bim_content['model']:
            bim_content['model']['expressions'] = []
        
        bim_content['model']['expressions'].append({
            "name": expression_name,
            "kind": "m",
            "expression": [
                "let",
                f"    Source = AzureStorage.DataLake(\"{new_url}\", [HierarchicalNavigation=true])",
                "in",
                "    Source"
            ],
            "lineageTag": f"directlake-{schema_name}-source"
        })
    
    # Update table partitions for DirectLake
    if 'tables' in bim_content['model']:
        for table in bim_content['model']['tables']:
            if 'partitions' in table:
                for partition in table['partitions']:
                    if 'source' in partition:
                        partition['mode'] = 'directLake'
                        partition['source'] = {
                            "type": "entity",
                            "entityName": partition['source'].get('entityName', table['name']),
                            "expressionSource": expression_name,
                            "schemaName": schema_name
                        }
    
    print(f"✓ Updated BIM for DirectLake")
    print(f"  - OneLake URL: {new_url}")
    print(f"  - Schema: {schema_name}")
    
    return bim_content


def create_dataset_from_bim(dataset_name, bim_content, workspace_id, client):
    """Create semantic model from BIM using Fabric REST API"""
    # Convert to base64
    bim_json = json.dumps(bim_content, indent=2)
    bim_base64 = base64.b64encode(bim_json.encode('utf-8')).decode('utf-8')
    
    pbism_content = {"version": "1.0"}
    pbism_json = json.dumps(pbism_content)
    pbism_base64 = base64.b64encode(pbism_json.encode('utf-8')).decode('utf-8')
    
    payload = {
        "displayName": dataset_name,
        "definition": {
            "parts": [
                {
                    "path": "model.bim",
                    "payload": bim_base64,
                    "payloadType": "InlineBase64"
                },
                {
                    "path": "definition.pbism",
                    "payload": pbism_base64,
                    "payloadType": "InlineBase64"
                }
            ]
        }
    }
    
    response = client.post(
        f"/v1/workspaces/{workspace_id}/semanticModels",
        json=payload
    )
    
    print(f"✓ Semantic model created")
    
    # Handle long-running operation
    if response.status_code == 202:
        operation_id = response.headers.get('x-ms-operation-id')
        print(f"   Waiting for operation to complete...")
        
        max_attempts = 30
        for attempt in range(max_attempts):
            time.sleep(2)
            status_response = client.get(f"/v1/operations/{operation_id}")
            status = status_response.json().get('status')
            
            if status == 'Succeeded':
                print(f"✓ Operation completed")
                break
            elif status == 'Failed':
                error = status_response.json().get('error', {})
                raise Exception(f"Operation failed: {error.get('message')}")
            elif attempt == max_attempts - 1:
                raise Exception(f"Operation timed out")


def deploy_semantic_model(workspace_name, lakehouse_name, schema_name, dataset_name, 
                         bim_url, wait_seconds=5):
    """
    Deploy a semantic model using DirectLake mode.
    
    Args:
        workspace_name: Name of the target workspace
        lakehouse_name: Name of the lakehouse
        schema_name: Schema name (e.g., 'dbo', 'staging')
        dataset_name: Name for the semantic model
        bim_url: URL to the BIM file
        wait_seconds: Seconds to wait before refresh (default: 5)
    
    Returns:
        1 for success, 0 for failure
    
    Examples:
        dr = Duckrun.connect("My Workspace/My Lakehouse.lakehouse/dbo")
        dr.deploy("https://raw.githubusercontent.com/.../model.bim")
    """
    print("=" * 70)
    print("Semantic Model Deployment (DirectLake)")
    print("=" * 70)
    
    client = FabricRestClient()
    
    try:
        # Step 1: Get workspace ID
        print("\n[Step 1/6] Getting workspace information...")
        workspace_id = get_workspace_id(workspace_name, client)
        
        # Step 2: Check if dataset exists
        print(f"\n[Step 2/6] Checking if dataset '{dataset_name}' exists...")
        dataset_exists = check_dataset_exists(dataset_name, workspace_id, client)
        
        if dataset_exists:
            print(f"\n✓ Dataset exists - refreshing...")
            
            if wait_seconds > 0:
                print(f"   Waiting {wait_seconds} seconds...")
                time.sleep(wait_seconds)
            
            print("\n[Step 6/6] Refreshing semantic model...")
            refresh_dataset(dataset_name, workspace_id, client)
            
            print("\n" + "=" * 70)
            print("🎉 Refresh Completed!")
            print("=" * 70)
            print(f"Dataset: {dataset_name}")
            print("=" * 70)
            return 1
        
        # Step 3: Get lakehouse ID
        print(f"\n[Step 3/6] Finding lakehouse...")
        lakehouse_id = get_lakehouse_id(lakehouse_name, workspace_id, client)
        
        # Step 4: Download and update BIM
        print("\n[Step 4/6] Downloading and configuring BIM file...")
        bim_content = download_bim_from_github(bim_url)
        
        modified_bim = update_bim_for_directlake(bim_content, workspace_id, lakehouse_id, schema_name)
        modified_bim['name'] = dataset_name
        modified_bim['id'] = dataset_name
        
        # Step 5: Deploy
        print("\n[Step 5/6] Deploying semantic model...")
        create_dataset_from_bim(dataset_name, modified_bim, workspace_id, client)
        
        if wait_seconds > 0:
            print(f"   Waiting {wait_seconds} seconds for permissions...")
            time.sleep(wait_seconds)
        
        # Step 6: Refresh
        print("\n[Step 6/6] Refreshing semantic model...")
        refresh_dataset(dataset_name, workspace_id, client)
        
        print("\n" + "=" * 70)
        print("🎉 Deployment Completed!")
        print("=" * 70)
        print(f"Dataset: {dataset_name}")
        print(f"Workspace: {workspace_name}")
        print(f"Lakehouse: {lakehouse_name}")
        print(f"Schema: {schema_name}")
        print("=" * 70)
        
        return 1
        
    except Exception as e:
        print("\n" + "=" * 70)
        print("❌ Deployment Failed")
        print("=" * 70)
        print(f"Error: {str(e)}")
        print("\n💡 Troubleshooting:")
        print(f"  - Verify workspace '{workspace_name}' exists")
        print(f"  - Verify lakehouse '{lakehouse_name}' exists")
        print(f"  - Ensure tables exist in '{schema_name}' schema")
        print(f"  - Check tables are in Delta format")
        print("=" * 70)
        return 0
