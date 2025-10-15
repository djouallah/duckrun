"""
File operations functionality for duckrun - OneLake Files copy and download
"""
import os
from typing import Optional, List
import obstore as obs
from obstore.store import AzureStore


def copy(duckrun_instance, local_folder: str, remote_folder: str, 
         file_extensions: Optional[List[str]] = None, 
         overwrite: bool = False) -> bool:
    """
    Copy files from a local folder to OneLake Files section.
    
    Args:
        duckrun_instance: The Duckrun connection instance
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
    if not os.path.exists(local_folder):
        print(f"❌ Local folder not found: {local_folder}")
        return False
        
    if not os.path.isdir(local_folder):
        print(f"❌ Path is not a directory: {local_folder}")
        return False
        
    # Get Azure token using enhanced auth system
    from .auth import get_token
    token = duckrun_instance._get_storage_token()
    if token == "PLACEHOLDER_TOKEN_TOKEN_NOT_AVAILABLE":
        print("Authenticating with Azure for file upload (detecting environment automatically)...")
        token = get_token()
        if not token:
            print("❌ Failed to authenticate for file upload")
            return False
    
    # Setup OneLake Files URL (use correct format without .Lakehouse suffix)
    files_base_url = duckrun_instance.files_base_url
    store = AzureStore.from_url(files_base_url, bearer_token=token)
    
    # Collect files to upload
    files_to_upload = []
    for root, dirs, files in os.walk(local_folder):
        for file in files:
            local_file_path = os.path.join(root, file)
            
            # Filter by extensions if specified
            if file_extensions:
                _, ext = os.path.splitext(file)
                if ext.lower() not in [e.lower() for e in file_extensions]:
                    continue
            
            # Calculate relative path from local_folder
            rel_path = os.path.relpath(local_file_path, local_folder)
            
            # Build remote path in OneLake Files (remote_folder is now mandatory)
            remote_path = f"{remote_folder.strip('/')}/{rel_path}".replace("\\", "/")
            
            files_to_upload.append((local_file_path, remote_path))
    
    if not files_to_upload:
        print(f"No files found to upload in {local_folder}")
        if file_extensions:
            print(f"  (filtered by extensions: {file_extensions})")
        return True
    
    print(f"📁 Uploading {len(files_to_upload)} files from '{local_folder}' to OneLake Files...")
    print(f"   Target folder: {remote_folder}")
    
    uploaded_count = 0
    failed_count = 0
    
    for local_path, remote_path in files_to_upload:
        try:
            # Check if file exists (if not overwriting)
            if not overwrite:
                try:
                    obs.head(store, remote_path)
                    print(f"  ⏭ Skipped (exists): {remote_path}")
                    continue
                except Exception:
                    # File doesn't exist, proceed with upload
                    pass
            
            # Read local file
            with open(local_path, 'rb') as f:
                file_data = f.read()
            
            # Upload to OneLake Files
            obs.put(store, remote_path, file_data)
            
            file_size = len(file_data)
            size_mb = file_size / (1024 * 1024) if file_size > 1024*1024 else file_size / 1024
            size_unit = "MB" if file_size > 1024*1024 else "KB"
            
            print(f"  ✓ Uploaded: {local_path} → {remote_path} ({size_mb:.1f} {size_unit})")
            uploaded_count += 1
            
        except Exception as e:
            print(f"  ❌ Failed: {local_path} → {remote_path} | Error: {str(e)[:100]}")
            failed_count += 1
    
    print(f"\n{'='*60}")
    if failed_count == 0:
        print(f"✅ Successfully uploaded all {uploaded_count} files to OneLake Files")
    else:
        print(f"⚠ Uploaded {uploaded_count} files, {failed_count} failed")
    print(f"{'='*60}")
    
    return failed_count == 0


def download(duckrun_instance, remote_folder: str = "", local_folder: str = "./downloaded_files",
             file_extensions: Optional[List[str]] = None,
             overwrite: bool = False) -> bool:
    """
    Download files from OneLake Files section to a local folder.
    
    Args:
        duckrun_instance: The Duckrun connection instance
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
    # Get Azure token using enhanced auth system
    from .auth import get_token
    token = duckrun_instance._get_storage_token()
    if token == "PLACEHOLDER_TOKEN_TOKEN_NOT_AVAILABLE":
        print("Authenticating with Azure for file download (detecting environment automatically)...")
        token = get_token()
        if not token:
            print("❌ Failed to authenticate for file download")
            return False
    
    # Setup OneLake Files URL (use correct format without .Lakehouse suffix)
    files_base_url = duckrun_instance.files_base_url
    store = AzureStore.from_url(files_base_url, bearer_token=token)
    
    # Create local directory
    os.makedirs(local_folder, exist_ok=True)
    
    # List files in OneLake Files
    print(f"📁 Discovering files in OneLake Files...")
    if remote_folder:
        print(f"   Source folder: {remote_folder}")
        prefix = f"{remote_folder.strip('/')}/"
    else:
        prefix = ""
    
    try:
        list_stream = obs.list(store, prefix=prefix)
        files_to_download = []
        
        for batch in list_stream:
            for obj in batch:
                remote_path = obj["path"]
                
                # Filter by extensions if specified
                if file_extensions:
                    _, ext = os.path.splitext(remote_path)
                    if ext.lower() not in [e.lower() for e in file_extensions]:
                        continue
                
                # Calculate local path
                if remote_folder:
                    rel_path = os.path.relpath(remote_path, remote_folder.strip('/'))
                else:
                    rel_path = remote_path
                
                local_path = os.path.join(local_folder, rel_path).replace('/', os.sep)
                files_to_download.append((remote_path, local_path))
        
        if not files_to_download:
            print(f"No files found to download")
            if file_extensions:
                print(f"  (filtered by extensions: {file_extensions})")
            return True
        
        print(f"📥 Downloading {len(files_to_download)} files to '{local_folder}'...")
        
        downloaded_count = 0
        failed_count = 0
        
        for remote_path, local_path in files_to_download:
            try:
                # Check if local file exists (if not overwriting)
                if not overwrite and os.path.exists(local_path):
                    print(f"  ⏭ Skipped (exists): {local_path}")
                    continue
                
                # Ensure local directory exists
                os.makedirs(os.path.dirname(local_path), exist_ok=True)
                
                # Download file
                data = obs.get(store, remote_path).bytes()
                
                # Write to local file
                with open(local_path, 'wb') as f:
                    f.write(data)
                
                file_size = len(data)
                size_mb = file_size / (1024 * 1024) if file_size > 1024*1024 else file_size / 1024
                size_unit = "MB" if file_size > 1024*1024 else "KB"
                
                print(f"  ✓ Downloaded: {remote_path} → {local_path} ({size_mb:.1f} {size_unit})")
                downloaded_count += 1
                
            except Exception as e:
                print(f"  ❌ Failed: {remote_path} → {local_path} | Error: {str(e)[:100]}")
                failed_count += 1
        
        print(f"\n{'='*60}")
        if failed_count == 0:
            print(f"✅ Successfully downloaded all {downloaded_count} files from OneLake Files")
        else:
            print(f"⚠ Downloaded {downloaded_count} files, {failed_count} failed")
        print(f"{'='*60}")
        
        return failed_count == 0
        
    except Exception as e:
        print(f"❌ Error listing files from OneLake: {e}")
        return False