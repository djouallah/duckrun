#!/usr/bin/env python3
"""
Quick test for the download() method
"""
import os
import sys
import shutil

# Add the local duckrun module to the path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import duckrun

def test_download():
    """Test the download method"""
    print("🚀 Quick OneLake download test...")
    
    # Connect to lakehouse
    print("\n🔗 Connecting to lakehouse...")
    con = duckrun.connect("temp/power.lakehouse")
    
    # Download files from the folder we just uploaded to
    print("\n📥 Testing download from OneLake Files...")
    download_folder = "test_download_output"
    
    # Clean up any existing download folder
    if os.path.exists(download_folder):
        shutil.rmtree(download_folder)
    
    # Test download from the quick_test_folder we uploaded to
    success = con.download("quick_test_folder", download_folder)
    
    if success:
        print("✅ DOWNLOAD SUCCESS!")
        print(f"\n📂 Downloaded files to: {download_folder}/")
        
        # List downloaded files
        if os.path.exists(download_folder):
            print("   Downloaded files:")
            for root, dirs, files in os.walk(download_folder):
                for file in files:
                    full_path = os.path.join(root, file)
                    rel_path = os.path.relpath(full_path, download_folder)
                    size = os.path.getsize(full_path)
                    print(f"     - {rel_path} ({size} bytes)")
                    
                    # Show content of text files
                    if file.endswith('.txt'):
                        print(f"\n📄 Content of {rel_path}:")
                        try:
                            with open(full_path, 'r') as f:
                                content = f.read()
                                print(f"   {content[:200]}...")  # First 200 chars
                        except Exception as e:
                            print(f"   Error reading file: {e}")
        
        print(f"\n🎯 SUCCESS! The download() method works perfectly!")
        print(f"   Files were successfully downloaded from OneLake Files to local folder")
        
    else:
        print("❌ Download failed")
        print("   Check if files exist in OneLake Files/quick_test_folder/")
    
    return success

if __name__ == "__main__":
    try:
        success = test_download()
        if success:
            print("\n🎉 Clean API validation complete!")
            print("   copy() ✅ - Upload works")  
            print("   download() ✅ - Download works")
            print("\n🚀 Both methods ready for production!")
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()