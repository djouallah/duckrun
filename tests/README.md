# DuckRun Tests and Examples

This folder contains tests, examples, and demonstrations for the duckrun library's file upload/download functionality.

## Files Overview

### Test Files
- **`test_signatures.py`** - Tests method signatures without requiring Azure authentication
- **`test_new_methods.py`** - Comprehensive tests for copy() and download_from_files() methods
- **`real_test.py`** - Real-world test that uploads/downloads files to/from OneLake (requires Azure auth)
- **`quick_test.py`** - Quick validation test for basic upload functionality

### Demo Files  
- **`demo_usage.py`** - Usage examples and demonstration of the new methods
- **`downloaded_from_onelake/`** - Sample files downloaded from OneLake during testing

## Usage

### Running Tests (No Authentication Required)
```bash
# Test method signatures and basic functionality
python test_signatures.py
```

### Running Real Tests (Azure Authentication Required)
```bash
# Make sure you're logged in to Azure CLI
az login

# Run comprehensive tests
python test_new_methods.py

# Run real OneLake upload/download test
python real_test.py

# Quick functionality test
python quick_test.py
```

### View Usage Examples
```bash
# See demonstration of new methods
python demo_usage.py
```

## New Methods Tested

### `copy()` Method
Upload files from local folder to OneLake Files section:
```python
import duckrun
con = duckrun.connect("temp/power.lakehouse")

# Upload all files (remote_folder is required)
con.copy("./local_folder", "target_folder")

# Upload only CSV files
con.copy("./data", "csv_files", ['.csv'])

# Upload with overwrite enabled
con.copy("./backup", "backups", overwrite=True)
```

### `download_from_files()` Method  
Download files from OneLake Files section to local folder:
```python
# Download all files from a folder
con.download_from_files("target_folder", "./local_download")

# Download only specific file types
con.download_from_files("csv_files", "./csv_data", ['.csv'])
```

## Key Changes

- ✅ `copy_to_files()` → `copy()`
- ✅ `remote_folder` parameter is now **REQUIRED**
- ✅ `overwrite` defaults to **False** (safer)
- ✅ Files go to **OneLake Files** section (not Tables)
- ✅ Supports file extension filtering
- ✅ Preserves folder structure

## Requirements

- Python 3.8+
- Azure CLI (for real tests)
- duckrun library with new file methods
- Access to Microsoft Fabric OneLake