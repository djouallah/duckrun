"""
Enhanced authentication module for duckrun - supports multiple notebook environments
"""
import os
import sys
from typing import Optional, Tuple


def safe_print(message: str):
    """Print message with safe encoding handling for Windows"""
    try:
        print(message)
    except UnicodeEncodeError:
        # Fallback: remove emojis and special chars
        import re
        clean_message = re.sub(r'[^\x00-\x7F]+', '', message)
        print(clean_message)


def get_token() -> Optional[str]:
    """
    Smart authentication that works across multiple environments:
    - Microsoft Fabric notebooks (uses notebookutils)
    - Local environments with Azure CLI (uses CLI + browser fallback)
    - Google Colab (uses device code flow) 
    - Other headless environments (uses device code flow)
    - Existing token from environment (uses cached token)
    
    Returns:
        Azure Storage token string or None if authentication fails
    """
    # Check if we already have a cached token
    token_env = os.environ.get("AZURE_STORAGE_TOKEN")
    if token_env and token_env != "PLACEHOLDER_TOKEN_TOKEN_NOT_AVAILABLE":
        return token_env

    print("🔐 Starting Azure authentication...")
    
    # Try Fabric notebook environment first
    try:
        import notebookutils  # type: ignore
        print("📓 Microsoft Fabric notebook detected - using notebookutils")
        token = notebookutils.credentials.getToken("pbi")
        os.environ["AZURE_STORAGE_TOKEN"] = token
        print("✅ Fabric notebook authentication successful!")
        return token
    except ImportError:
        pass  # Not in Fabric notebook
    except Exception as e:
        print(f"⚠️ Fabric notebook authentication failed: {e}")

    # Try local/VS Code authentication (Azure CLI + browser)
    print("🖥️ Trying local authentication (Azure CLI + browser fallback)...")
    token = _get_local_token()
    if token:
        return token
    
    # If local auth failed, fall back to device code flow
    print("🔐 Falling back to device code flow for remote/headless environment...")
    try:
        return _get_device_code_token()
    except Exception as e:
        print(f"❌ Authentication failed: {e}")
        print("💡 Try refreshing and running again, or check your Azure permissions")
        return None


def _get_device_code_token() -> Optional[str]:
    """Get token using device code flow for headless environments"""
    try:
        from azure.identity import DeviceCodeCredential
        
        # Use Azure CLI client ID for device code flow
        credential = DeviceCodeCredential(
            client_id="04b07795-8ddb-461a-bbee-02f9e1bf7b46",  # Azure CLI client ID
            tenant_id="common"
        )
        
        print("🔐 Follow the authentication prompts in your browser...")
        token_obj = credential.get_token("https://storage.azure.com/.default")
        
        os.environ["AZURE_STORAGE_TOKEN"] = token_obj.token
        print("✅ Device code authentication successful!")
        return token_obj.token
        
    except Exception as e:
        print(f"❌ Device code authentication failed: {e}")
        return None


def _get_local_token() -> Optional[str]:
    """Get token using CLI first, then browser fallback for local environments"""
    # First try Azure CLI directly
    try:
        from azure.identity import AzureCliCredential
        print("🔐 Trying Azure CLI authentication...")
        
        cli_credential = AzureCliCredential()
        token_obj = cli_credential.get_token("https://storage.azure.com/.default")
        
        os.environ["AZURE_STORAGE_TOKEN"] = token_obj.token
        print("✅ Azure CLI authentication successful!")
        return token_obj.token
        
    except Exception as cli_error:
        print(f"⚠️ Azure CLI authentication failed: {cli_error}")
        print("🔐 Falling back to interactive browser authentication...")
        
        # Fallback to interactive browser
        try:
            from azure.identity import InteractiveBrowserCredential
            
            browser_credential = InteractiveBrowserCredential()
            token_obj = browser_credential.get_token("https://storage.azure.com/.default")
            
            os.environ["AZURE_STORAGE_TOKEN"] = token_obj.token
            print("✅ Interactive browser authentication successful!")
            return token_obj.token
            
        except Exception as browser_error:
            print(f"❌ Interactive browser authentication failed: {browser_error}")
            return None


def get_fabric_api_token() -> Optional[str]:
    """
    Get token for Fabric API operations (different scope than storage)
    
    Returns:
        Fabric API token string or None if authentication fails
    """
    # Check if we already have a cached Fabric API token
    fabric_token_env = os.environ.get("FABRIC_API_TOKEN")
    if fabric_token_env:
        print("✅ Using cached Fabric API token")
        return fabric_token_env
    
    print("🔐 Getting Fabric API token...")
    
    # Try Fabric notebook environment first
    try:
        import notebookutils  # type: ignore
        print("📓 Microsoft Fabric notebook detected - using notebookutils")
        token = notebookutils.credentials.getToken("pbi")
        os.environ["FABRIC_API_TOKEN"] = token
        print("✅ Fabric API token obtained!")
        return token
    except ImportError:
        pass  # Not in Fabric notebook
    except Exception as e:
        print(f"⚠️ Fabric notebook token failed: {e}")

    # Fallback to azure-identity for external environments
    try:
        # Check if we're in Google Colab
        try:
            import google.colab
            print("💻 Using device code flow for Fabric API (Colab)")
            from azure.identity import DeviceCodeCredential
            credential = DeviceCodeCredential(
                client_id="04b07795-8ddb-461a-bbee-02f9e1bf7b46",
                tenant_id="common"
            )
        except ImportError:
            # For all other environments, try CLI first then browser
            print("🖥️ Using CLI + browser fallback for Fabric API")
            
            # Try CLI first
            try:
                from azure.identity import AzureCliCredential
                print("🔐 Trying Azure CLI for Fabric API...")
                credential = AzureCliCredential()
                token_obj = credential.get_token("https://api.fabric.microsoft.com/.default")
                os.environ["FABRIC_API_TOKEN"] = token_obj.token
                print("✅ Fabric API token obtained via Azure CLI!")
                return token_obj.token
            except Exception as cli_error:
                print(f"⚠️ Azure CLI failed for Fabric API: {cli_error}")
                print("🔐 Falling back to interactive browser for Fabric API...")
                from azure.identity import InteractiveBrowserCredential
                credential = InteractiveBrowserCredential()
        
        token_obj = credential.get_token("https://api.fabric.microsoft.com/.default")
        os.environ["FABRIC_API_TOKEN"] = token_obj.token
        print("✅ Fabric API token obtained!")
        return token_obj.token
        
    except Exception as e:
        print(f"❌ Fabric API authentication failed: {e}")
        return None


def authenticate_for_environment() -> Tuple[bool, Optional[str]]:
    """
    Main authentication entry point - detects environment and authenticates appropriately
    
    Returns:
        Tuple of (success: bool, token: Optional[str])
    """
    print("\n🔍 Detecting execution environment...")
    
    # Check environment
    try:
        import notebookutils  # type: ignore
        env_type = "Microsoft Fabric Notebook"
    except ImportError:
        try:
            import google.colab
            env_type = "Google Colab"
        except ImportError:
            # For all other environments (VS Code, local Python, etc.)
            # we'll treat as local and try Azure CLI first
            env_type = "Local/VS Code Environment"
    
    print(f"📍 Environment: {env_type}")
    
    token = get_token()
    if token:
        print(f"✅ Authentication successful for {env_type}")
        return True, token
    else:
        print(f"❌ Authentication failed for {env_type}")
        return False, None


# For backward compatibility - expose the same interface as before
def get_storage_token() -> str:
    """
    Backward compatible method - returns token or placeholder
    """
    token = get_token()
    return token if token else "PLACEHOLDER_TOKEN_TOKEN_NOT_AVAILABLE"


# Example usage function for testing
def test_authentication():
    """
    Test authentication in current environment
    """
    print("=" * 60)
    print("🧪 TESTING DUCKRUN AUTHENTICATION")
    print("=" * 60)
    
    success, token = authenticate_for_environment()
    
    if success:
        print("\n✅ Authentication test successful!")
        print(f"Token length: {len(token) if token else 0} characters")
        print(f"Token starts with: {token[:20] if token else 'None'}...")
    else:
        print("\n❌ Authentication test failed!")
        print("Please check your Azure setup and permissions.")
    
    print("=" * 60)
    return success