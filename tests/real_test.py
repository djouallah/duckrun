#!/usr/bin/env python3
"""
Real test that uploads files to OneLake Files section
"""
import os
import sys
import tempfile
import shutil
from pathlib import Path

# Add the local duckrun module to the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def create_real_test_files():
    """Create test files that we'll upload to OneLake"""
    print("📁 Creating test files for OneLake upload...")
    
    # Create a test directory
    test_dir = "test_onelake_upload"
    os.makedirs(test_dir, exist_ok=True)
    
    # Create a sample CSV file
    csv_content = """id,name,department,salary
1,Alice Johnson,Engineering,85000
2,Bob Smith,Marketing,65000
3,Charlie Brown,Engineering,90000
4,Diana Prince,Sales,75000
5,Eve Wilson,HR,70000"""
    
    with open(os.path.join(test_dir, "employees.csv"), "w") as f:
        f.write(csv_content)
    
    # Create a JSON file
    json_content = """{
  "project": "OneLake File Upload Test",
  "version": "1.0",
  "created": "2025-10-05",
  "description": "Test file for duckrun copy method",
  "features": ["file upload", "azure integration", "onelake files"]
}"""
    
    with open(os.path.join(test_dir, "metadata.json"), "w") as f:
        f.write(json_content)
    
    # Create a text report
    report_content = """OneLake File Upload Test Report
==============================

This file was created to test the new duckrun copy() method.

Test Details:
- Date: October 5, 2025
- Method: copy() 
- Target: OneLake Files section
- Expected: Files should appear in Fabric OneLake

Files uploaded:
1. employees.csv - Sample employee data
2. metadata.json - Project metadata
3. test_report.txt - This report

Status: Testing in progress...
"""
    
    with open(os.path.join(test_dir, "test_report.txt"), "w") as f:
        f.write(report_content)
    
    # Create a subfolder with additional files
    reports_dir = os.path.join(test_dir, "reports")
    os.makedirs(reports_dir, exist_ok=True)
    
    # Monthly report
    monthly_content = """month,revenue,expenses,profit
Jan,100000,75000,25000
Feb,110000,80000,30000
Mar,120000,85000,35000"""
    
    with open(os.path.join(reports_dir, "monthly_summary.csv"), "w") as f:
        f.write(monthly_content)
    
    # List all created files
    print("✅ Created test files:")
    for root, dirs, files in os.walk(test_dir):
        for file in files:
            full_path = os.path.join(root, file)
            rel_path = os.path.relpath(full_path, test_dir)
            size = os.path.getsize(full_path)
            print(f"  - {rel_path} ({size} bytes)")
    
    return test_dir

def test_real_upload():
    """Test real file upload to OneLake"""
    print("=" * 60)
    print("🚀 REAL ONELAKE FILE UPLOAD TEST")
    print("=" * 60)
    
    test_dir = None
    
    try:
        # Step 1: Create test files
        test_dir = create_real_test_files()
        
        # Step 2: Connect to lakehouse
        print("\n🔗 Connecting to lakehouse...")
        import duckrun
        
        con = duckrun.connect("temp/power.lakehouse")
        print("✅ Connected successfully!")
        
        # Step 3: Upload all files to OneLake Files
        print("\n📤 Uploading files to OneLake Files section...")
        print("   Target folder: 'duckrun_test_files'")
        
        success = con.copy(test_dir, "duckrun_test_files")
        
        if success:
            print("✅ All files uploaded successfully!")
            print("\n🎯 CHECK YOUR ONELAKE NOW!")
            print("   Go to: Fabric -> Lakehouse -> Files -> duckrun_test_files/")
            print("   You should see:")
            print("   - employees.csv")
            print("   - metadata.json") 
            print("   - test_report.txt")
            print("   - reports/monthly_summary.csv")
        else:
            print("❌ Upload failed!")
            return False
            
        # Step 4: Upload only CSV files to a different folder
        print("\n📤 Uploading only CSV files to 'csv_data' folder...")
        
        success_csv = con.copy(test_dir, "csv_data", ['.csv'])
        
        if success_csv:
            print("✅ CSV files uploaded successfully!")
            print("   Check: Fabric -> Lakehouse -> Files -> csv_data/")
            print("   You should see only:")
            print("   - employees.csv")
            print("   - reports/monthly_summary.csv")
        
        # Step 5: Test download
        print("\n📥 Testing download from OneLake...")
        download_dir = "downloaded_from_onelake"
        
        success_download = con.download_from_files("duckrun_test_files", download_dir)
        
        if success_download:
            print("✅ Files downloaded successfully!")
            print(f"   Check local folder: {download_dir}/")
            
            if os.path.exists(download_dir):
                print("   Downloaded files:")
                for root, dirs, files in os.walk(download_dir):
                    for file in files:
                        full_path = os.path.join(root, file)
                        rel_path = os.path.relpath(full_path, download_dir)
                        size = os.path.getsize(full_path)
                        print(f"     - {rel_path} ({size} bytes)")
        
        print("\n" + "=" * 60)
        print("✅ REAL TEST COMPLETED!")
        print("🎉 Check your Fabric OneLake Files section!")
        print("=" * 60)
        
        return True
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        # Cleanup local test files
        if test_dir and os.path.exists(test_dir):
            print(f"\n🧹 Cleaning up local test files: {test_dir}")
            try:
                shutil.rmtree(test_dir)
                print("✅ Local cleanup complete")
            except Exception as e:
                print(f"⚠ Cleanup warning: {e}")

if __name__ == "__main__":
    print("🚀 Starting real OneLake file upload test...")
    print("⚠ This requires Azure CLI authentication!")
    print("   Make sure you're logged in: az login")
    
    input("\n Press Enter to continue (or Ctrl+C to cancel)...")
    
    success = test_real_upload()
    
    if success:
        print("\n🎯 SUCCESS! Check these locations:")
        print("   1. Fabric OneLake -> temp workspace -> power.Lakehouse -> Files")
        print("      - duckrun_test_files/ (all files)")
        print("      - csv_data/ (only CSV files)")
        print("   2. Local folder: downloaded_from_onelake/ (downloaded files)")
    else:
        print("\n❌ Test failed - check authentication and permissions")