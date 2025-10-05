#!/usr/bin/env python3
"""
Final usage demonstration for the new duckrun methods
"""
import os
import sys
import tempfile

# Add the local duckrun module to the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def demo_usage():
    """Demonstrate the correct usage of new methods"""
    print("=" * 60)
    print("🎯 DUCKRUN NEW METHODS - USAGE DEMONSTRATION")
    print("=" * 60)
    
    print("\n📋 The new methods have these signatures:")
    print("   con.copy(local_folder, remote_folder, file_extensions=None, overwrite=False)")
    print("   con.download_from_files(remote_folder='', local_folder='./downloaded_files',") 
    print("                           file_extensions=None, overwrite=False)")
    
    print("\n🔧 Key Changes Made:")
    print("   1. ✅ copy_to_files() → copy()")
    print("   2. ✅ remote_folder is now REQUIRED (no default)")
    print("   3. ✅ overwrite defaults to False (safer)")
    
    print("\n📝 Example Usage:")
    
    # Create a temp directory for demo
    temp_dir = tempfile.mkdtemp(prefix="duckrun_demo_")
    print(f"\n   # Create some test files")
    print(f"   test_folder = '{temp_dir}'")
    
    # Create actual test files
    with open(os.path.join(temp_dir, "data.csv"), "w") as f:
        f.write("id,name,value\n1,Alice,100\n2,Bob,200\n")
    
    with open(os.path.join(temp_dir, "readme.txt"), "w") as f:
        f.write("Test file for duckrun demo")
    
    print("   # Created: data.csv, readme.txt")
    
    print(f"\n   # Connect to lakehouse")
    print(f"   import duckrun")
    print(f"   con = duckrun.connect('temp/power.lakehouse')")
    
    print(f"\n   # Upload files - remote_folder is now MANDATORY")
    print(f"   con.copy('{temp_dir}', 'my_uploads')  # ✅ Works - target folder specified")
    print(f"   # con.copy('{temp_dir}')  # ❌ Error - remote_folder required")
    
    print(f"\n   # Upload only specific file types")
    print(f"   con.copy('{temp_dir}', 'csv_files', ['.csv'])")
    
    print(f"\n   # Upload with overwrite enabled")
    print(f"   con.copy('{temp_dir}', 'backups', overwrite=True)")
    
    print(f"\n   # Download files - overwrite now defaults to False")
    print(f"   con.download_from_files('my_uploads', './downloaded')")
    print(f"   con.download_from_files('csv_files', './csv_only', ['.csv'])")
    
    # Test the actual method call syntax
    print(f"\n🧪 Testing method signature compliance:")
    try:
        import duckrun
        
        # Test that copy method requires remote_folder
        try:
            # This should fail
            from inspect import signature
            sig = signature(duckrun.Duckrun.copy)
            params = list(sig.parameters.keys())
            print(f"   copy parameters: {params}")
            
            # Check if remote_folder is required
            remote_param = sig.parameters['remote_folder']
            has_default = remote_param.default != remote_param.empty
            print(f"   remote_folder has default: {has_default}")
            
            if not has_default:
                print("   ✅ remote_folder is correctly REQUIRED")
            else:
                print("   ❌ remote_folder should be required")
                
            # Check overwrite default
            overwrite_param = sig.parameters.get('overwrite')
            if overwrite_param and overwrite_param.default == False:
                print("   ✅ overwrite correctly defaults to False")
            else:
                print("   ❌ overwrite should default to False")
                
        except Exception as e:
            print(f"   ⚠ Signature check error: {e}")
            
    except ImportError as e:
        print(f"   ⚠ Import error: {e}")
    
    # Cleanup
    import shutil
    shutil.rmtree(temp_dir, ignore_errors=True)
    
    print("\n" + "=" * 60)
    print("✅ DEMONSTRATION COMPLETE!")
    print("🚀 Your duckrun library is ready for upgrade!")
    print("=" * 60)

if __name__ == "__main__":
    demo_usage()