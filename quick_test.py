#!/usr/bin/env python3
"""
Quick test to upload files to OneLake (run after Azure auth)
"""
import os
import duckrun

def quick_upload_test():
    """Quick test assuming you're already authenticated"""
    
    # Create a simple test file
    test_file_content = """Quick Test File
===============

This file was uploaded using duckrun.copy() method.
Time: October 5, 2025

If you can see this file in OneLake Files, the upload worked!
"""
    
    # Create test directory and file
    test_dir = "quick_test"
    os.makedirs(test_dir, exist_ok=True)
    
    with open(os.path.join(test_dir, "quick_test.txt"), "w") as f:
        f.write(test_file_content)
    
    print("📁 Created quick test file")
    print(f"   Location: {test_dir}/quick_test.txt")
    
    # Connect and upload
    print("\n🔗 Connecting to lakehouse...")
    con = duckrun.connect("temp/power.lakehouse")
    
    print("\n📤 Uploading to OneLake Files...")
    success = con.copy(test_dir, "quick_test_folder")
    
    if success:
        print("✅ SUCCESS!")
        print("\n🎯 GO CHECK YOUR ONELAKE:")
        print("   Fabric -> temp workspace -> power.Lakehouse -> Files -> quick_test_folder/")
        print("   You should see: quick_test.txt")
    else:
        print("❌ Upload failed")
    
    # Cleanup
    import shutil
    shutil.rmtree(test_dir, ignore_errors=True)
    
    return success

if __name__ == "__main__":
    print("🚀 Quick OneLake upload test...")
    try:
        success = quick_upload_test()
        if success:
            print("\n🎉 Files should now be visible in OneLake Files section!")
    except Exception as e:
        print(f"❌ Error: {e}")
        print("Make sure you're authenticated with: az login")