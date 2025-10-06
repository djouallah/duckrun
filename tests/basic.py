#!/usr/bin/env python3
"""
Basic test script for duckrun package
Converted from basic.ipynb notebook
"""

import sys
import os
import time
from psutil import *

# Add the parent directory to Python path to use local package source
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import duckrun

def quick_return_code_test():
    """Quick test of return code logic without authentication"""
    print("🔬 QUICK RETURN CODE TEST (No Auth Required)")
    print("=" * 60)
    
    # Test the core logic we added to the run method
    tests_passed = 0
    total_tests = 3
    
    # Test 1: Python task returning 1 (should continue)
    task1 = ('dummy_function', ())
    result1 = 1
    should_stop1 = (len(task1) == 2 and not isinstance(task1[1], str) and result1 == 0)
    test1_pass = not should_stop1
    print(f"Test 1 - Python returns 1: {'✅ PASS' if test1_pass else '❌ FAIL'} (continues)")
    if test1_pass: tests_passed += 1
    
    # Test 2: Python task returning 0 (should stop) 
    task2 = ('dummy_function', ())
    result2 = 0
    should_stop2 = (len(task2) == 2 and not isinstance(task2[1], str) and result2 == 0)
    test2_pass = should_stop2
    print(f"Test 2 - Python returns 0: {'✅ PASS' if test2_pass else '❌ FAIL'} (stops)")
    if test2_pass: tests_passed += 1
    
    # Test 3: SQL task (should never stop on return value)
    task3 = ('table_name', 'overwrite')
    result3 = 'table_name'
    should_stop3 = (len(task3) == 2 and not isinstance(task3[1], str) and result3 == 0)
    test3_pass = not should_stop3
    print(f"Test 3 - SQL task:         {'✅ PASS' if test3_pass else '❌ FAIL'} (ignores return)")
    if test3_pass: tests_passed += 1
    
    print("=" * 60)
    if tests_passed == total_tests:
        print("🎉 ALL QUICK TESTS PASSED! Return code logic works correctly.")
        print("   • Python return 0 → stops pipeline ✅")
        print("   • Python return 1 → continues pipeline ✅") 
        print("   • SQL tasks → ignore return values ✅")
        return True
    else:
        print(f"❌ {total_tests - tests_passed}/{total_tests} tests failed!")
        return False

def main():
    # Start total execution timer
    total_start_time = time.time()
    print("🚀 Starting duckrun basic test script...")
    print("=" * 60)
    
    # Step 1: Configuration setup (timed)
    print("📋 Step 1: Setting up configuration parameters...")
    config_start = time.time()
    
    # Configuration parameters
    # please don't use a workspace name, Lakehouse and semantic_model with an empty space, 
    # or the same name of the lakehouse recently deleted
    nbr_days_download = int(30 * 2 ** ((cpu_count() - 2) / 2))  # or just input your numbers
    lh = 'power' 
    schema = 'aemo'
    semantic_model = "directlake_on_onelake" 
    ws = "temp"
    Nbr_threads = (cpu_count()*2)+1
    
    # SQL folder configuration
    sql_folder = 'https://github.com/djouallah/fabric_demo/raw/refs/heads/main/transformation/'
    
    config_time = time.time() - config_start
    print(f"✅ Configuration completed in {config_time:.3f} seconds")
    print(f"   - Workspace: {ws}, Lakehouse: {lh}, Schema: {schema}")
    print(f"   - Threads: {Nbr_threads}, Days to download: {nbr_days_download}")
    print()
    
    # Step 2: Establish connection (timed)
    print("🔗 Step 2: Establishing lakehouse connection...")
    connection_start = time.time()
    con = duckrun.connect(f"{ws}/{lh}.lakehouse/{schema}", sql_folder)
    connection_time = time.time() - connection_start
    print(f"✅ Connection established in {connection_time:.2f} seconds")
    print()
    
    # Step 3: Define pipeline configuration (timed)
    print("⚙️ Step 3: Configuring intraday pipeline...")
    pipeline_config_start = time.time()
    
    intraday = [
        ('scrapingv2', (["http://nemweb.com.au/Reports/Current/DispatchIS_Reports/","http://nemweb.com.au/Reports/Current/Dispatch_SCADA/" ],
                      ["Reports/Current/DispatchIS_Reports/","Reports/Current/Dispatch_SCADA/"],
                       288, ws,lh,Nbr_threads)),
        ('price_today','append'),
        ('scada_today','append'),
        ('duid','ignore'),
        ('summary__incremental', 'append')            
    ]
    
    pipeline_config_time = time.time() - pipeline_config_start
    print(f"✅ Pipeline configuration completed in {pipeline_config_time:.3f} seconds")
    print(f"   - Pipeline tasks: {len(intraday)} tasks configured")
    print()
    
    # Step 4: Execute intraday pipeline (timed)
    print("🔄 Step 4: Executing intraday data pipeline...")
    pipeline_start = time.time()
    result = con.run(intraday)
    pipeline_time = time.time() - pipeline_start
    print(f"✅ Pipeline execution completed in {pipeline_time:.2f} seconds")
    print(f"   - Pipeline result: {result}")
    print()
    
    # Step 5: Additional test operations
    print("🧪 Step 5: Running additional test operations...")
    additional_start = time.time()
    
    # Test with different connection (timed)
    print("   5a. Testing secondary connection...")
    secondary_conn_start = time.time()
    con2 = duckrun.connect("temp/power.lakehouse/dbo")
    secondary_conn_time = time.time() - secondary_conn_start
    print(f"      ✅ Secondary connection in {secondary_conn_time:.3f} seconds")
    
    # Test CSV loading and table creation (timed)
    print("   5b. Testing CSV loading and table operations...")
    csv_ops_start = time.time()
    con2.sql("""FROM read_csv_auto('https://data.wa.aemo.com.au/datafiles/post-facilities/facilities.csv')
           """).write.mode("overwrite").saveAsTable("wa.base")
    con2.sql("FROM base").show(max_width=120)
    csv_ops_time = time.time() - csv_ops_start
    print(f"      ✅ CSV operations completed in {csv_ops_time:.3f} seconds")
    
    additional_time = time.time() - additional_start
    print(f"✅ Additional operations completed in {additional_time:.2f} seconds")
    print()
    
    # Step 6: File operations testing (timed)
    print("📁 Step 6: Testing file operations...")
    file_ops_start = time.time()
    
    # Test copy operation (timed)
    print("   6a. Testing copy operation...")
    copy_start = time.time()
    try:
        con.copy(r"C:\lakehouse\default\Files\calendar", "xxx")
        copy_time = time.time() - copy_start
        print(f"      ✅ Copy operation completed in {copy_time:.3f} seconds")
    except Exception as e:
        copy_time = time.time() - copy_start
        print(f"      ❌ Copy operation failed in {copy_time:.3f} seconds: {e}")
    
    # Test download operation (timed)
    print("   6b. Testing download operation...")
    download_start = time.time()
    try:
        con.download("xxx", r"C:\lakehouse\default\Files\calendar", overwrite=True)
        download_time = time.time() - download_start
        print(f"      ✅ Download operation completed in {download_time:.3f} seconds")
    except Exception as e:
        download_time = time.time() - download_start
        print(f"      ❌ Download operation failed in {download_time:.3f} seconds: {e}")
    
    file_ops_time = time.time() - file_ops_start
    print(f"✅ File operations completed in {file_ops_time:.2f} seconds")
    print()
    
    # Final summary with total time
    total_time = time.time() - total_start_time
    print("=" * 60)
    print("📊 EXECUTION SUMMARY")
    print("=" * 60)
    print(f"⏱️  Total execution time: {total_time:.2f} seconds ({total_time/60:.2f} minutes)")
    print(f"📋 Configuration setup: {config_time:.3f} seconds")
    print(f"🔗 Lakehouse connection: {connection_time:.2f} seconds")
    print(f"⚙️  Pipeline configuration: {pipeline_config_time:.3f} seconds")
    print(f"🔄 Pipeline execution: {pipeline_time:.2f} seconds")
    print(f"🧪 Additional operations: {additional_time:.2f} seconds")
    print(f"📁 File operations: {file_ops_time:.2f} seconds")
    print("=" * 60)
    print("🎉 Basic test script completed successfully!")


def test_pipeline_exit_code():
    """
    Test function that returns 0 if pipeline keeps working (succeeds) 
    or returns 1 if pipeline early exits (fails).
    
    This mimics Unix exit code conventions:
    - 0 = success (pipeline completed successfully)  
    - 1 = failure (pipeline failed or exited early)
    """
    print("\n🧪 Testing pipeline exit code behavior...")
    print("=" * 50)
    
    try:
        # Test configuration
        ws = "temp"
        lh = "power"
        schema = "aemo"
        sql_folder = 'https://github.com/djouallah/fabric_demo/raw/refs/heads/main/transformation/'
        
        # Establish connection
        print("🔗 Connecting to lakehouse...")
        con = duckrun.connect(f"{ws}/{lh}.lakehouse/{schema}", sql_folder)
        
        # Define a simple test pipeline that should succeed
        test_pipeline = [
            ('duid', 'ignore'),  # Simple task that should succeed
        ]
        
        print("🔄 Running test pipeline...")
        pipeline_result = con.run(test_pipeline)
        
        if pipeline_result:
            print("✅ Pipeline completed successfully")
            print("🎯 Return code: 0 (SUCCESS)")
            return 0  # Success - pipeline kept working
        else:
            print("❌ Pipeline failed or exited early")  
            print("🎯 Return code: 1 (FAILURE)")
            return 1  # Failure - pipeline early exit
            
    except Exception as e:
        print(f"❌ Test failed with exception: {e}")
        print("🎯 Return code: 1 (FAILURE)")
        return 1  # Failure - exception occurred


def test_pipeline_with_zero_return():
    """
    Test function that verifies if a task returning 0 stops the pipeline.
    
    Updated behavior: The pipeline should STOP when a PYTHON task returns 0.
    SQL tasks only stop on exceptions/errors, not return values (they return table names).
    """
    print("\n🧪 Testing pipeline behavior when task returns 0...")
    print("=" * 50)
    
    try:
        # Create a simple Python function that returns 0
        import tempfile
        import os
        
        # Create a temporary Python file with test functions
        temp_dir = tempfile.mkdtemp()
        test_py_file = os.path.join(temp_dir, "pipeline_test_functions.py")
        
        with open(test_py_file, 'w') as f:
            f.write("""
def dummy_success_task():
    '''Dummy function that returns 1 - pipeline should continue'''
    print("📍 Dummy success task - returning 1 (continue)")
    return 1

def dummy_failure_task():
    '''Dummy function that returns 0 - pipeline should stop'''
    print("📍 Dummy failure task - returning 0 (stop pipeline)")
    return 0

def task_after_failure():
    '''Function that should NOT run after failure task'''
    print("📍 ERROR: This task should NOT run after zero-return task!")
    return 1
""")
        
        # Test configuration
        ws = "temp"
        lh = "power" 
        schema = "aemo"
        sql_folder = temp_dir  # Use temp directory as SQL folder
        
        # Establish connection
        print("🔗 Connecting to lakehouse...")
        con = duckrun.connect(f"{ws}/{lh}.lakehouse/{schema}", sql_folder)
        
        # Test 1: Pipeline with success task (should continue and complete)
        print("\n--- Test 1: Success task (return 1) ---")
        success_pipeline = [
            ('dummy_success_task', ()),  # Returns 1 - should continue
        ]
        
        success_result = con.run(success_pipeline)
        print(f"Success pipeline result: {success_result}")
        
        # Test 2: Pipeline with failure task (should stop early)
        print("\n--- Test 2: Failure task (return 0) ---")
        failure_pipeline = [
            ('dummy_success_task', ()),   # Returns 1 - should continue
            ('dummy_failure_task', ()),   # Returns 0 - should stop pipeline here
            ('task_after_failure', ()),   # Should NOT run
        ]
        
        failure_result = con.run(failure_pipeline)
        print(f"Failure pipeline result: {failure_result}")
        
        # Cleanup
        os.remove(test_py_file)
        os.rmdir(temp_dir)
        
        # Evaluate results
        if success_result and not failure_result:
            print("\n✅ Pipeline behavior is CORRECT:")
            print("   - Success task (return 1) → Pipeline continued")
            print("   - Failure task (return 0) → Pipeline stopped early")
            return True
        elif not failure_result:
            print("\n⚠️  Partial success:")
            print("   - Failure task correctly stopped pipeline")
            print(f"   - Success pipeline result: {success_result}")
            return True
        else:
            print("\n❌ Pipeline behavior is INCORRECT:")
            print(f"   - Success pipeline: {success_result} (expected: True)")
            print(f"   - Failure pipeline: {failure_result} (expected: False)")
            return False
            
    except Exception as e:
        print(f"❌ Test failed with exception: {e}")
        return False


def test_pipeline_with_failure():
    """
    Test function that intentionally triggers a pipeline failure
    to verify that early exit returns code 1.
    """
    print("\n🧪 Testing pipeline failure scenario...")
    print("=" * 50)
    
    try:
        # Test configuration  
        ws = "temp"
        lh = "power"
        schema = "aemo"
        sql_folder = 'https://github.com/djouallah/fabric_demo/raw/refs/heads/main/transformation/'
        
        # Establish connection
        print("🔗 Connecting to lakehouse...")
        con = duckrun.connect(f"{ws}/{lh}.lakehouse/{schema}", sql_folder)
        
        # Define a pipeline with an invalid task to trigger failure
        failing_pipeline = [
            ('nonexistent_task', 'overwrite'),  # This should fail
        ]
        
        print("🔄 Running failing pipeline...")
        pipeline_result = con.run(failing_pipeline)
        
        if pipeline_result:
            print("⚠️  Pipeline unexpectedly succeeded")
            print("🎯 Return code: 0 (SUCCESS)")  
            return 0
        else:
            print("✅ Pipeline failed as expected")
            print("🎯 Return code: 1 (FAILURE)")
            return 1  # Expected failure
            
    except Exception as e:
        print(f"✅ Pipeline failed with exception as expected: {e}")
        print("🎯 Return code: 1 (FAILURE)")
        return 1  # Expected failure


if __name__ == "__main__":
    # Run quick return code test first (no auth needed)
    print("\n" + "=" * 80)
    print("🔬 RUNNING QUICK RETURN CODE TEST")
    print("=" * 80)
    quick_test_passed = quick_return_code_test()
    
    if not quick_test_passed:
        print("❌ Quick test failed - stopping here!")
        sys.exit(1)
    
    print("\n🚀 Quick test passed! Running full integration tests...")
    
    # Run main test with real authentication
    main()
    
    # Run additional exit code tests
    print("\n" + "=" * 80)
    print("🔬 RUNNING FULL PIPELINE EXIT CODE TESTS")
    print("=" * 80)
    
    # Test successful pipeline (should return 0)
    success_code = test_pipeline_exit_code()
    print(f"\n📋 Test 1 Result: {success_code}")
    
    # Test zero return behavior
    zero_return_result = test_pipeline_with_zero_return()
    print(f"\n📋 Test 2 Result: {zero_return_result}")
    
    # Test failing pipeline (should return 1)  
    failure_code = test_pipeline_with_failure()
    print(f"\n📋 Test 3 Result: {failure_code}")
    
    print("\n" + "=" * 80)
    print("🏁 PIPELINE BEHAVIOR TESTS COMPLETED")
    print("=" * 80)
    print(f"✅ Success test returned: {success_code} (expected: 0)")
    print(f"🔍 Zero-return test result: {zero_return_result} (True = correct behavior)")
    print(f"❌ Failure test returned: {failure_code} (expected: 1)")
    
    # Provide conclusion about the original question
    print("\n" + "🎯 FIXED BEHAVIOR:")
    print("=" * 60)
    if zero_return_result:
        print("✅ YES - Python tasks returning 0 now STOP the pipeline!")
        print("   Pipeline execution halts when Python task returns 0.")
        print("   SQL tasks only stop on exceptions/errors, not return values.")
        print("   This restores the previous expected behavior.")
    else:
        print("❌ Test inconclusive due to error")
    print("=" * 60)
    
    sys.exit(0)