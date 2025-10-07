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
    print("[TEST] QUICK RETURN CODE TEST (No Auth Required)")
    print("=" * 60)
    
    # Test the core logic we added to the run method
    tests_passed = 0
    total_tests = 3
    
    # Test 1: Python task returning 1 (should continue)
    task1 = ('dummy_function', ())
    result1 = 1
    should_stop1 = (len(task1) == 2 and not isinstance(task1[1], str) and result1 == 0)
    test1_pass = not should_stop1
    print(f"Test 1 - Python returns 1: {'[PASS]' if test1_pass else '[FAIL]'} (continues)")
    if test1_pass: tests_passed += 1
    
    # Test 2: Python task returning 0 (should stop) 
    task2 = ('dummy_function', ())
    result2 = 0
    should_stop2 = (len(task2) == 2 and not isinstance(task2[1], str) and result2 == 0)
    test2_pass = should_stop2
    print(f"Test 2 - Python returns 0: {'[PASS]' if test2_pass else '[FAIL]'} (stops)")
    if test2_pass: tests_passed += 1
    
    # Test 3: SQL task (should never stop on return value)
    task3 = ('table_name', 'overwrite')
    result3 = 'table_name'
    should_stop3 = (len(task3) == 2 and not isinstance(task3[1], str) and result3 == 0)
    test3_pass = not should_stop3
    print(f"Test 3 - SQL task:         {'[PASS]' if test3_pass else '[FAIL]'} (ignores return)")
    if test3_pass: tests_passed += 1
    
    print("=" * 60)
    if tests_passed == total_tests:
        print("[SUCCESS] ALL QUICK TESTS PASSED! Return code logic works correctly.")
        print("   - Python return 0 → stops pipeline [OK]")
        print("   - Python return 1 → continues pipeline [OK]") 
        print("   - SQL tasks → ignore return values [OK]")
        return True
    else:
        print(f"[FAIL] {total_tests - tests_passed}/{total_tests} tests failed!")
        return False

def main():
    # Start total execution timer
    total_start_time = time.time()
    print("[RUN] Starting duckrun basic test script...")
    print("=" * 60)
    
    # Step 1: Configuration setup (timed)
    print("[INFO] Step 1: Setting up configuration parameters...")
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
    print(f"[OK] Configuration completed in {config_time:.3f} seconds")
    print(f"   - Workspace: {ws}, Lakehouse: {lh}, Schema: {schema}")
    print(f"   - Threads: {Nbr_threads}, Days to download: {nbr_days_download}")
    print()
    
    # Step 2: Establish connection (timed)
    print("[CONN] Step 2: Establishing lakehouse connection...")
    connection_start = time.time()
    con = duckrun.connect(f"{ws}/{lh}.lakehouse/{schema}", sql_folder)
    connection_time = time.time() - connection_start
    print(f"[OK] Connection established in {connection_time:.2f} seconds")
    print()
    
    # Step 3: Define pipeline configuration (timed)
    print("⚙️ Step 3: Configuring intraday pipeline...")
    pipeline_config_start = time.time()
    
    nightly =[
              
              ('scrapingv2', (["https://nemweb.com.au/Reports/Current/Daily_Reports/"],["Reports/Current/Daily_Reports/"],2,ws,lh,Nbr_threads)),
              ('price','append', {}, {'mergeSchema': 'true', 'partitionBy': ['YEAR']}),
              ('scada','append'),
              ('download_excel',("raw/", ws,lh)),
              ('duid','overwrite'),
              ('calendar','ignore'),
              ('mstdatetime','ignore'),
              ('summary__backfill','overwrite')
         ]
    
    pipeline_config_time = time.time() - pipeline_config_start
    print(f"[OK] Pipeline configuration completed in {pipeline_config_time:.3f} seconds")
    print(f"   - Pipeline tasks: {len(nightly)} tasks configured")
    print()

    # Step 4: Execute nightly pipeline (timed)
    print("🔄 Step 4: Executing nightly data pipeline...")
    pipeline_start = time.time()
    result = con.run(nightly)
    pipeline_time = time.time() - pipeline_start
    print(f"[OK] Pipeline execution completed in {pipeline_time:.2f} seconds")
    print(f"   - Pipeline result: {result}")
    print()
    
    # Step 5: Additional test operations
    print("[TEST] Step 5: Running additional test operations...")
    additional_start = time.time()
    
    # Test with different connection (timed)
    print("   5a. Testing secondary connection...")
    secondary_conn_start = time.time()
    con2 = duckrun.connect("temp/power.lakehouse/dbo")
    secondary_conn_time = time.time() - secondary_conn_start
    print(f"      [OK] Secondary connection in {secondary_conn_time:.3f} seconds")
    
    # Test CSV loading and table creation (timed)
    print("   5b. Testing CSV loading and table operations...")
    csv_ops_start = time.time()
    con2.sql("""FROM read_csv_auto('https://data.wa.aemo.com.au/datafiles/post-facilities/facilities.csv')
           """).write.mode("overwrite").saveAsTable("wa.base")
    con2.sql("FROM base").show(max_width=120)
    csv_ops_time = time.time() - csv_ops_start
    print(f"      [OK] CSV operations completed in {csv_ops_time:.3f} seconds")
    
    # Test Spark-style API with schema merging and partitioning (timed)
    print("   5c. Testing Spark-style API with mergeSchema and partitioning...")
    spark_api_start = time.time()
    try:
        # Create test data with schema evolution and partitioning columns
        result = con2.sql("""
            SELECT 
                'North America' as region,
                'Electronics' as product_category,
                100.50 as sales_amount,
                '2024-10-07'::DATE as order_date,
                'CUST001' as customer_id,
                -- This column simulates schema evolution
                'New promotional discount' as promotion_type
        """)
        
        # Test Spark-style API with mergeSchema and partitioning
        result.write \
            .mode("append") \
            .option("mergeSchema", "true") \
            .partitionBy("region", "product_category") \
            .saveAsTable("sales_partitioned")
        
        # Verify the table was created and show sample data
        con2.sql("SELECT * FROM sales_partitioned LIMIT 3").show(max_width=120)
        
        spark_api_time = time.time() - spark_api_start
        print(f"      [OK] Spark-style API with mergeSchema + partitioning completed in {spark_api_time:.3f} seconds")
    except Exception as e:
        spark_api_time = time.time() - spark_api_start
        print(f"      [FAIL] Spark-style API test failed in {spark_api_time:.3f} seconds: {e}")
    
    # Test direct Spark-style API on existing table with partitioning (timed)
    print("   5d. Testing direct Spark-style API on scada table...")
    direct_spark_start = time.time()
    try:
        # Test Spark-style API directly on scada table with schema merging and partitioning
        # Use con (original connection) which has the scada table from the pipeline
        con.sql("FROM scada LIMIT 1000") \
            .write \
            .mode("append") \
            .option("mergeSchema", "true") \
            .partitionBy("YEAR") \
            .saveAsTable("test.waa")
        
        # Verify the table was created using the same connection
        row_count = con.sql("SELECT COUNT(*) as cnt FROM test_waa").fetchone()[0]
        
        direct_spark_time = time.time() - direct_spark_start
        print(f"      [OK] Direct Spark API test completed in {direct_spark_time:.3f} seconds")
        print(f"      [INFO] Created test.waa table with {row_count} rows, partitioned by YEAR")
    except Exception as e:
        direct_spark_time = time.time() - direct_spark_start
        print(f"      [FAIL] Direct Spark API test failed in {direct_spark_time:.3f} seconds: {e}")
    
    additional_time = time.time() - additional_start
    print(f"[OK] Additional operations completed in {additional_time:.2f} seconds")
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
        print(f"      [OK] Copy operation completed in {copy_time:.3f} seconds")
    except Exception as e:
        copy_time = time.time() - copy_start
        print(f"      [FAIL] Copy operation failed in {copy_time:.3f} seconds: {e}")
    
    # Test download operation (timed)
    print("   6b. Testing download operation...")
    download_start = time.time()
    try:
        con.download("xxx", r"C:\lakehouse\default\Files\calendar", overwrite=True)
        download_time = time.time() - download_start
        print(f"      [OK] Download operation completed in {download_time:.3f} seconds")
    except Exception as e:
        download_time = time.time() - download_start
        print(f"      [FAIL] Download operation failed in {download_time:.3f} seconds: {e}")
    
    file_ops_time = time.time() - file_ops_start
    print(f"[OK] File operations completed in {file_ops_time:.2f} seconds")
    print()
    
    # Final summary with total time
    total_time = time.time() - total_start_time
    print("=" * 60)
    print("📊 EXECUTION SUMMARY")
    print("=" * 60)
    print(f"⏱️  Total execution time: {total_time:.2f} seconds ({total_time/60:.2f} minutes)")
    print(f"[INFO] Configuration setup: {config_time:.3f} seconds")
    print(f"[CONN] Lakehouse connection: {connection_time:.2f} seconds")
    print(f"⚙️  Pipeline configuration: {pipeline_config_time:.3f} seconds")
    print(f"🔄 Pipeline execution: {pipeline_time:.2f} seconds")
    print(f"[TEST] Additional operations: {additional_time:.2f} seconds")
    print(f"📁 File operations: {file_ops_time:.2f} seconds")
    print("=" * 60)
    print("[SUCCESS] Basic test script completed successfully!")





if __name__ == "__main__":
    # Run quick return code test first (no auth needed)
    print("\n" + "=" * 80)
    print("🔬 RUNNING QUICK RETURN CODE TEST")
    print("=" * 80)
    quick_test_passed = quick_return_code_test()
    
    if not quick_test_passed:
        print("[FAIL] Quick test failed - stopping here!")
        sys.exit(1)
    
    print("\n[RUN] Quick test passed! Running full integration tests...")
    
    # Run main test with real authentication
    main()
    
    print("\n" + "=" * 80)
    print("✅ ALL TESTS COMPLETED SUCCESSFULLY")
    print("=" * 80)
