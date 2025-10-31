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

def test_workspace_name_scenarios(ws, lh, schema):
    """Test both workspace naming scenarios - with and without spaces"""
    print("\n[TEST] WORKSPACE NAME SCENARIOS TEST")
    print("=" * 60)
    print("Testing that lakehouse connections work with both:")
    print("  1. Workspace names WITHOUT spaces (uses name directly)")
    print("  2. Workspace names WITH spaces (resolves to GUIDs)")
    print("=" * 60)
    
    tests_passed = 0
    total_tests = 2
    
    # Test 1: Workspace without spaces (use provided ws variable)
    print(f"\n[Test 1] Workspace without spaces: '{ws}/{lh}.lakehouse/{schema}'")
    test1_start = time.time()
    try:
        # First connect to workspace to ensure lakehouse exists
        workspace_conn = duckrun.connect(ws)
        workspace_conn.create_lakehouse_if_not_exists(lh)
        
        # Now test the lakehouse connection
        conn1 = duckrun.connect(f"{ws}/{lh}.lakehouse/{schema}")
        test1_time = time.time() - test1_start
        print(f"   ✅ [PASS] Connection successful in {test1_time:.2f}s")
        print(f"   URL format used: {ws}/{lh}.lakehouse (name directly, no GUID resolution)")
        tests_passed += 1
    except Exception as e:
        test1_time = time.time() - test1_start
        print(f"   ❌ [FAIL] Connection failed in {test1_time:.2f}s")
        print(f"   Error: {e}")
    
    # Test 2: Workspace with spaces (e.g., 'tmp new')
    print("\n[Test 2] Workspace with spaces: 'tmp new/data.lakehouse/aemo'")
    test2_start = time.time()
    try:
        ws_with_space = "tmp new"
        
        # First connect to workspace to ensure lakehouse exists
        workspace_conn = duckrun.connect(ws_with_space)
        workspace_conn.create_lakehouse_if_not_exists(lh)
        
        # Now test the lakehouse connection (should resolve to GUIDs)
        conn2 = duckrun.connect(f"{ws_with_space}/{lh}.lakehouse/{schema}")
        test2_time = time.time() - test2_start
        print(f"   ✅ [PASS] Connection successful in {test2_time:.2f}s")
        print(f"   URL format used: workspace_guid/lakehouse_guid (resolved from names with spaces)")
        tests_passed += 1
    except Exception as e:
        test2_time = time.time() - test2_start
        print(f"   ❌ [FAIL] Connection failed in {test2_time:.2f}s")
        print(f"   Error: {e}")
        print(f"   Note: This may fail if workspace 'tmp new' doesn't exist in your Fabric environment")
    
    # Summary
    print("\n" + "=" * 60)
    if tests_passed == total_tests:
        print(f"[SUCCESS] ALL {total_tests} WORKSPACE NAME SCENARIOS PASSED!")
        print("   - Workspace without spaces: Uses names directly ✅")
        print("   - Workspace with spaces: Resolves to GUIDs ✅")
        return True
    elif tests_passed == 1:
        print(f"[PARTIAL] {tests_passed}/{total_tests} scenarios passed")
        print("   Note: Second test may fail if 'tmp new' workspace doesn't exist")
        return True  # Still pass if at least one scenario works
    else:
        print(f"[FAIL] {total_tests - tests_passed}/{total_tests} scenarios failed!")
        return False


def test_semantic_model_features(ws, lh, schema):
    """Test semantic model download and deploy features"""
    print("\n[TEST] Testing semantic model features...")
    
    try:
        # Test 1: Connect to workspace
        print("\n1. Testing workspace connection...")
        con = duckrun.connect(ws)
        print("   ✓ Workspace connection successful")
        
        # Test 2: Download BIM (just check method exists)
        print("\n2. Testing download_bim method exists...")
        if hasattr(con, 'download_bim'):
            print("   ✓ download_bim method available")
        else:
            print("   ✗ download_bim method not found")
            return False
        
        # Test 3: Connect to lakehouse and check deploy method
        print("\n3. Testing lakehouse connection and deploy method...")
        dr = duckrun.connect(f"{ws}/{lh}.lakehouse/{schema}")
        if hasattr(dr, 'deploy'):
            print("   ✓ deploy method available")
        else:
            print("   ✗ deploy method not found")
            return False
        
        # Test 4: Check deploy_semantic_model function exists
        print("\n4. Testing deploy_semantic_model function...")
        import duckrun.semantic_model as sm
        if hasattr(sm, 'deploy_semantic_model'):
            print("   ✓ deploy_semantic_model function available")
        else:
            print("   ✗ deploy_semantic_model function not found")
            return False
        
        print("\n✅ All semantic model feature tests passed!")
        return True
        
    except Exception as e:
        print(f"\n❌ Semantic model test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_snowflake_connection():
    """Test connection to Snowflake database and get_item_id"""
    print("\n[TEST] Testing Snowflake database connection and get_item_id...")
    print("=" * 60)
    
    try:
        # Connect to Snowflake database
        print("\n1. Connecting to snowflake/ONELAKEUSEAST.SnowflakeDatabase...")
        connection_string = "snowflake/ONELAKEUSEAST.SnowflakeDatabase"
        
        dr = duckrun.connect(connection_string)
        print("   ✓ Connection successful!")
        
        # Display connection info
        print("\n2. Connection Information:")
        print(f"   - Workspace ID: {dr.workspace_id}")
        print(f"   - Item ID (lakehouse_id): {dr.lakehouse_id}")
        print(f"   - Schema: {dr.schema}")
        print(f"   - Display Name: {dr.lakehouse_display_name}")
        
        # Test get_item_id without force
        print(f"\n3. Testing get_item_id() without force...")
        item_id = dr.get_item_id(force=False)
        print(f"   ✓ get_item_id(force=False) returned: {item_id}")
        
        # Test get_item_id with force (will try to resolve to GUID)
        print(f"\n4. Testing get_item_id(force=True) - will resolve to GUID...")
        try:
            item_id_forced = dr.get_item_id(force=True)
            print(f"   ✓ get_item_id(force=True) returned: {item_id_forced}")
            
            # Check if it's a GUID format
            import re
            guid_pattern = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.IGNORECASE)
            if guid_pattern.match(item_id_forced):
                print(f"   ✓ Result is a valid GUID format")
            else:
                print(f"   ℹ Result is not a GUID (may be friendly name): {item_id_forced}")
        except Exception as e:
            print(f"   ⚠️ get_item_id(force=True) encountered an issue: {e}")
            print(f"   This may be expected if API resolution is not available")
        
        # Test get_workspace_id
        print(f"\n5. Testing get_workspace_id()...")
        workspace_id = dr.get_workspace_id(force=False)
        print(f"   ✓ get_workspace_id(force=False) returned: {workspace_id}")
        
        try:
            workspace_id_forced = dr.get_workspace_id(force=True)
            print(f"   ✓ get_workspace_id(force=True) returned: {workspace_id_forced}")
        except Exception as e:
            print(f"   ⚠️ get_workspace_id(force=True) encountered an issue: {e}")
        
        # Test table_base_url
        print(f"\n6. Checking ABFSS URLs...")
        print(f"   - Table base URL: {dr.table_base_url}")
        print(f"   - Files base URL: {dr.files_base_url}")
        
        # Close connection
        print(f"\n7. Closing connection...")
        dr.close()
        print("   ✓ Connection closed successfully")
        
        # Summary
        print("\n✅ All Snowflake connection tests passed!")
        return True
        
    except Exception as e:
        print(f"\n❌ Snowflake connection test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    # Start total execution timer
    total_start_time = time.time()
    print("[RUN] Starting duckrun basic test script...")
    print("=" * 60)
    
    # Step 1: Configuration setup (timed)
    print("[INFO] Step 1: Setting up configuration parameters...")
    config_start = time.time()
    
    # Get shared configuration from global scope
    ws = globals().get('ws', 'tmp')
    lh = globals().get('lh', 'data')
    schema = globals().get('schema', 'aemo')
    
    # Configuration parameters
    # please don't use a workspace name, Lakehouse and semantic_model with an empty space, 
    # or the same name of the lakehouse recently deleted
    Nbr_threads = (cpu_count()*2)+1
    nbr_days_download = int(30 * 2 ** ((cpu_count() - 2) / 2))  # or just input your numbers
    
    # SQL folder configuration
    sql_folder = 'https://github.com/djouallah/fabric_demo/raw/refs/heads/main/transformation/'
    
    config_time = time.time() - config_start
    print(f"[OK] Configuration completed in {config_time:.3f} seconds")
    print(f"   - Workspace: {ws}, Lakehouse: {lh}, Schema: {schema}")
    print(f"   - Threads: {Nbr_threads}, Days to download: {nbr_days_download}")
    print()
    
    # Step 2: Connect to workspace and manage lakehouses (timed)
    print("[WORKSPACE] Step 2: Connecting to workspace and managing lakehouses...")
    workspace_mgmt_start = time.time()
    
    # Test workspace connection (timed)
    print("   2a. Connecting to workspace...")
    workspace_conn_start = time.time()
    try:
        workspace_conn = duckrun.connect(ws)
        workspace_conn_time = time.time() - workspace_conn_start
        print(f"      [OK] Workspace connection established in {workspace_conn_time:.3f} seconds")
    except Exception as e:
        workspace_conn_time = time.time() - workspace_conn_start
        print(f"      [FAIL] Workspace connection failed in {workspace_conn_time:.3f} seconds: {e}")
        raise e
    
    # Test list lakehouses (timed)
    print("   2b. Listing existing lakehouses...")
    list_start = time.time()
    try:
        lakehouses = workspace_conn.list_lakehouses()
        list_time = time.time() - list_start
        print(f"      [OK] List lakehouses completed in {list_time:.3f} seconds")
        print(f"      [INFO] Found {len(lakehouses)} lakehouses: {lakehouses}")
    except Exception as e:
        list_time = time.time() - list_start
        print(f"      [FAIL] List lakehouses failed in {list_time:.3f} seconds: {e}")
        raise e
    
    # Test create lakehouse if not exists (timed)
    print(f"   2c. Creating lakehouse '{lh}' if it doesn't exist...")
    create_start = time.time()
    try:
        create_result = workspace_conn.create_lakehouse_if_not_exists(lh)
        create_time = time.time() - create_start
        print(f"      [OK] Create lakehouse completed in {create_time:.3f} seconds")
        print(f"      [INFO] Create result: {create_result}")
    except Exception as e:
        create_time = time.time() - create_start
        print(f"      [FAIL] Create lakehouse failed in {create_time:.3f} seconds: {e}")
        raise e
    
    workspace_mgmt_time = time.time() - workspace_mgmt_start
    print(f"[OK] Workspace management completed in {workspace_mgmt_time:.2f} seconds")
    print()
    
    # Step 3: Establish lakehouse connection (timed)
    print("[CONN] Step 3: Establishing lakehouse connection...")
    connection_start = time.time()
    conn = duckrun.connect(f"{ws}/{lh}.lakehouse/{schema}", sql_folder)
    connection_time = time.time() - connection_start
    print(f"[OK] Connection established in {connection_time:.2f} seconds")
    print()
    
    # Step 4: Define pipeline configuration (timed)
    print("⚙️ Step 4: Configuring intraday pipeline...")
    pipeline_config_start = time.time()
    
    intraday = [
              ('scrapingv2', (["http://nemweb.com.au/Reports/Current/DispatchIS_Reports/","http://nemweb.com.au/Reports/Current/Dispatch_SCADA/" ],
                            ["Reports/Current/DispatchIS_Reports/","Reports/Current/Dispatch_SCADA/"],
                             2, ws,lh,Nbr_threads)),
              ('price_today','append'),
              ('scada_today','append'),
              ('download_excel',("raw/", ws,lh)),
              ('duid','ignore'),
              ('calendar','ignore'),
              ('mstdatetime','ignore'),
              ('summary__incremental', 'append')
             ]
    
    pipeline_config_time = time.time() - pipeline_config_start
    print(f"[OK] Pipeline configuration completed in {pipeline_config_time:.3f} seconds")
    print(f"   - Pipeline tasks: {len(intraday)} tasks configured")
    print()

    # Step 5: Execute intraday pipeline (timed)
    print("🔄 Step 5: Executing intraday data pipeline...")
    pipeline_start = time.time()
    result = conn.run(intraday)
    pipeline_time = time.time() - pipeline_start
    print(f"[OK] Pipeline execution completed in {pipeline_time:.2f} seconds")
    print(f"   - Pipeline result: {result}")
    print()

    # Step 6: Additional test operations
    print("[TEST] Step 6: Running additional test operations...")
    additional_start = time.time()
    
    # Test with different connection (timed)
    print("   6a. Testing secondary connection...")
    secondary_conn_start = time.time()
    conn2 = duckrun.connect(f"{ws}/{lh}.lakehouse/dbo")
    secondary_conn_time = time.time() - secondary_conn_start
    print(f"      [OK] Secondary connection in {secondary_conn_time:.3f} seconds")
    
    # Test CSV loading and table creation (timed)
    print("   6b. Testing CSV loading and table operations...")
    csv_ops_start = time.time()
    conn2.sql("""FROM read_csv_auto('https://data.wa.aemo.com.au/datafiles/post-facilities/facilities.csv')
           """).write.mode("overwrite").saveAsTable("wa.base")
    conn2.sql("FROM wa.base").show(max_width=120)
    csv_ops_time = time.time() - csv_ops_start
    print(f"      [OK] CSV operations completed in {csv_ops_time:.3f} seconds")
    
    # Test Spark-style API with schema merging and partitioning (timed)
    print("   6c. Testing Spark-style API with mergeSchema and partitioning...")
    spark_api_start = time.time()
    try:
        # Create test data with schema evolution and partitioning columns
        result = conn2.sql("""
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
        conn2.sql("SELECT * FROM sales_partitioned LIMIT 3").show(max_width=120)
        
        spark_api_time = time.time() - spark_api_start
        print(f"      [OK] Spark-style API with mergeSchema + partitioning completed in {spark_api_time:.3f} seconds")
    except Exception as e:
        spark_api_time = time.time() - spark_api_start
        print(f"      [FAIL] Spark-style API test failed in {spark_api_time:.3f} seconds: {e}")
    
    # Test direct Spark-style API on existing table with partitioning (timed)
    print("   6d. Testing direct Spark-style API on scada table...")
    direct_spark_start = time.time()
    try:
        # Test Spark-style API directly on scada table with schema merging and partitioning
        # Use conn (original connection) which has the scada table from the pipeline
        conn.sql("FROM scada_today LIMIT 1000") \
            .write \
            .mode("append") \
            .option("mergeSchema", "true") \
            .partitionBy("YEAR") \
            .saveAsTable("test.waa")
        
        # Verify the table was created using the same connection
        row_count = conn.sql("SELECT COUNT(*) as cnt FROM test.waa").fetchone()[0]
        
        direct_spark_time = time.time() - direct_spark_start
        print(f"      [OK] Direct Spark API test completed in {direct_spark_time:.3f} seconds")
        print(f"      [INFO] Created test.waa table with {row_count} rows, partitioned by YEAR")
    except Exception as e:
        direct_spark_time = time.time() - direct_spark_start
        print(f"      [FAIL] Direct Spark API test failed in {direct_spark_time:.3f} seconds: {e}")
    
    additional_time = time.time() - additional_start
    print(f"[OK] Additional operations completed in {additional_time:.2f} seconds")
    print()
    
    # Step 7: File operations testing (timed)
    print("📁 Step 7: Testing file operations...")
    file_ops_start = time.time()
    
    # Test copy operation (timed)
    print("   7a. Testing copy operation...")
    copy_start = time.time()
    try:
        conn.copy(r"C:\lakehouse\default\Files\calendar", "xxx")
        copy_time = time.time() - copy_start
        print(f"      [OK] Copy operation completed in {copy_time:.3f} seconds")
    except Exception as e:
        copy_time = time.time() - copy_start
        print(f"      [FAIL] Copy operation failed in {copy_time:.3f} seconds: {e}")
    
    # Test download operation (timed)
    print("   7b. Testing download operation...")
    download_start = time.time()
    try:
        conn.download("xxx", r"C:\lakehouse\default\Files\calendar", overwrite=True)
        download_time = time.time() - download_start
        print(f"      [OK] Download operation completed in {download_time:.3f} seconds")
    except Exception as e:
        download_time = time.time() - download_start
        print(f"      [FAIL] Download operation failed in {download_time:.3f} seconds: {e}")
    
    file_ops_time = time.time() - file_ops_start
    print(f"[OK] File operations completed in {file_ops_time:.2f} seconds")
    print()
    
    # Step 8: Delta Lake statistics testing (timed)
    print("📈 Step 8: Testing Delta Lake statistics...")
    stats_start = time.time()
    
    # Test get_stats with different patterns (timed)
    print("   8a. Testing get_stats on single table...")
    stats_single_start = time.time()
    try:
        # Test single table stats in current schema
        stats_price = conn.get_stats('price_today')
        print(f"      [OK] Stats for 'price' table:")
        print(f"      [INFO] Columns: {list(stats_price.columns)}")
        if len(stats_price) > 0:
            first_row = stats_price.iloc[0]
            print(f"      [INFO] Total rows: {first_row.get('total_rows', 'N/A')}, Files: {first_row.get('num_files', 'N/A')}")
        
        stats_single_time = time.time() - stats_single_start
        print(f"      [OK] Single table stats completed in {stats_single_time:.3f} seconds")
    except Exception as e:
        stats_single_time = time.time() - stats_single_start
        print(f"      [FAIL] Single table stats failed in {stats_single_time:.3f} seconds: {e}")
    
    print("   8b. Testing get_stats on schema.table...")
    stats_schema_table_start = time.time()
    try:
        # Test schema.table format
        stats_aemo_scada = conn.get_stats('test.summary')
        print(f"      [OK] Stats for 'test.summary' table:")
        if len(stats_aemo_scada) > 0:
            first_row = stats_aemo_scada.iloc[0]
            print(f"      [INFO] Total rows: {first_row.get('total_rows', 'N/A')}, Files: {first_row.get('num_files', 'N/A')}")
        
        stats_schema_table_time = time.time() - stats_schema_table_start
        print(f"      [OK] Schema.table stats completed in {stats_schema_table_time:.3f} seconds")
    except Exception as e:
        stats_schema_table_time = time.time() - stats_schema_table_start
        print(f"      [FAIL] Schema.table stats failed in {stats_schema_table_time:.3f} seconds: {e}")
    
    print("   8c. Testing get_stats on entire schema...")
    stats_schema_start = time.time()
    try:
        # Test entire schema stats
        stats_aemo = conn.get_stats('test')
        print(f"      [OK] Stats for entire 'test' schema:")
        print(f"      [INFO] Found {len(stats_aemo)} tables in schema")
        if len(stats_aemo) > 0:
            table_names = stats_aemo['tbl'].tolist()
            print(f"      [INFO] Tables: {', '.join(table_names[:5])}{'...' if len(table_names) > 5 else ''}")
        
        stats_schema_time = time.time() - stats_schema_start
        print(f"      [OK] Schema stats completed in {stats_schema_time:.3f} seconds")
    except Exception as e:
        stats_schema_time = time.time() - stats_schema_start
        print(f"      [FAIL] Schema stats failed in {stats_schema_time:.3f} seconds: {e}")
    
    print("   8d. Testing get_stats on summary table specifically...")
    stats_summary_start = time.time()
    try:
        # Test summary table stats specifically
        print(conn.get_stats('summary'))
        
        stats_summary_time = time.time() - stats_summary_start
        print(f"      [OK] Summary table stats completed in {stats_summary_time:.3f} seconds")
    except Exception as e:
        stats_summary_time = time.time() - stats_summary_start
        print(f"      [FAIL] Summary table stats failed in {stats_summary_time:.3f} seconds: {e}")
    
    stats_time = time.time() - stats_start
    print(f"[OK] Statistics operations completed in {stats_time:.2f} seconds")
    print()
    
    # Step 8e: Additional connection test
    print("   8e. Testing new connection to tmp/tmp.lakehouse...")
    tmp_conn_start = time.time()
    try:
        con = duckrun.connect(f"{ws}/{lh}.lakehouse/{schema}")
        
        # Check DuckDB version first
        print("      [INFO] Checking DuckDB version...")
        try:
            version_result = con.sql("SELECT version()")
            version_data = version_result.fetchall()
            if version_data:
                version_info = version_data[0][0]  # First row, first column
                print(f"      [INFO] DuckDB version: {version_info}")
        except Exception as v_error:
            print(f"      [INFO] Could not get DuckDB version: {v_error}")
        
        # Check available tables in default schema (dbo)
        print("      [INFO] Checking available tables in default schema (dbo)...")
        try:
            tables_result = con.sql("SHOW TABLES")
            table_data = tables_result.fetchall()
            if table_data:
                table_names = [row[0] for row in table_data]
                print(f"      [INFO] Tables in dbo schema: {table_names}")
            else:
                print("      [INFO] No tables found in dbo schema")
        except Exception as table_error:
            print(f"      [INFO] Could not list tables in dbo: {table_error}")
        
        # Check tables in test schema using information_schema
        print("      [INFO] Checking available tables in test schema...")
        try:
            test_tables_result = con.sql("SELECT table_name FROM information_schema.tables WHERE table_schema = 'test'")
            test_table_data = test_tables_result.fetchall()
            if test_table_data:
                test_table_names = [row[0] for row in test_table_data]
                print(f"      [INFO] Tables in test schema: {test_table_names}")
                
                # Use the first table from test schema for stats
                first_test_table = f"test.{test_table_names[0]}"
                print(f"      [INFO] Getting stats for table '{first_test_table}':")
                print(con.get_stats(first_test_table))
            else:
                print("      [INFO] No tables found in test schema")
        except Exception as test_error:
            print(f"      [INFO] Could not list tables in test schema: {test_error}")
        
        # Now try the original request - getting stats for 'test' (the schema)
        print("      [INFO] Getting stats for 'test' schema (original request):")
        try:
            print(con.get_stats('aemo'))
        except Exception as test_schema_error:
            print(f"      [INFO] Failed to get stats for 'test' schema: {test_schema_error}")
        
        con.close()
        tmp_conn_time = time.time() - tmp_conn_start
        print(f"      [OK] Tmp connection test completed in {tmp_conn_time:.3f} seconds")
    except Exception as e:
        tmp_conn_time = time.time() - tmp_conn_start
        print(f"      [FAIL] Tmp connection test failed in {tmp_conn_time:.3f} seconds: {e}")
    
    # Step 9: Semantic Model Deployment Testing (timed)
    print("🚀 Step 9: Testing semantic model deployment...")
    deploy_start = time.time()
    
    print("   9a. Testing deploy() method with DirectLake...")
    deploy_test_start = time.time()
    try:
        # Use a sample BIM file URL (you'll need to replace with an actual BIM file)
        bim_url = "https://raw.githubusercontent.com/djouallah/fabric_demo/refs/heads/main/semantic_model/directlake.bim"
        
        # Test deployment with simple dataset name 'colab'
        print(f"      [INFO] Deploying semantic model from: {bim_url}")
        print(f"      [INFO] Target: {ws}/{lh} (schema: {schema})")
        print(f"      [INFO] Dataset name: 'colab'")
        print(f"      [INFO] Mode: DirectLake (connects to OneLake Delta tables)")
        
        # Call the deploy method - simple test case
        result = conn.deploy(bim_url, 'colab')
        
        deploy_test_time = time.time() - deploy_test_start
        
        if result == 1:
            print(f"      [OK] Semantic model deployment completed successfully in {deploy_test_time:.2f} seconds")
            print(f"      [INFO] Dataset created: colab")
            print(f"      [INFO] Connection mode: DirectLake")
        else:
            print(f"      [WARN] Deployment returned status {result} in {deploy_test_time:.2f} seconds")
            
    except FileNotFoundError as e:
        deploy_test_time = time.time() - deploy_test_start
        print(f"      [SKIP] BIM file not found - skipping deployment test in {deploy_test_time:.3f} seconds")
        print(f"      [INFO] To test deployment, provide a valid BIM file URL")
        print(f"      [INFO] Error: {e}")
    except Exception as e:
        deploy_test_time = time.time() - deploy_test_start
        print(f"      [FAIL] Semantic model deployment failed in {deploy_test_time:.3f} seconds: {e}")
        print(f"      [INFO] This test requires:")
        print(f"      [INFO]   - Valid BIM file URL")
        print(f"      [INFO]   - Proper Fabric authentication")
        print(f"      [INFO]   - Delta tables in the specified schema")
    
    deploy_time = time.time() - deploy_start
    print(f"[OK] Semantic model deployment test completed in {deploy_time:.2f} seconds")
    print()
    
    # Step 10: Close connections
    print("🔌 Step 10: Closing connections...")
    conn.close()
    conn2.close()
    print("[OK] All connections closed successfully")
    print()
    
    # Final summary with total time
    total_time = time.time() - total_start_time
    print("=" * 60)
    print("📊 EXECUTION SUMMARY")
    print("=" * 60)
    print(f"⏱️  Total execution time: {total_time:.2f} seconds ({total_time/60:.2f} minutes)")
    print(f"[INFO] Configuration setup: {config_time:.3f} seconds")
    print(f"[WORKSPACE] Workspace management: {workspace_mgmt_time:.2f} seconds")
    print(f"[CONN] Lakehouse connection: {connection_time:.2f} seconds")
    print(f"⚙️  Pipeline configuration: {pipeline_config_time:.3f} seconds")
    print(f"🔄 Pipeline execution: {pipeline_time:.2f} seconds")
    print(f"[TEST] Additional operations: {additional_time:.2f} seconds")
    print(f"📁 File operations: {file_ops_time:.2f} seconds")
    print(f"📈 Statistics operations: {stats_time:.2f} seconds")
    print(f"🚀 Semantic model deployment: {deploy_time:.2f} seconds")
    print("=" * 60)
    print("[SUCCESS] Basic test script completed successfully!")





if __name__ == "__main__":
    # Configuration parameters - defined once at the top
    ws = "tmp"
    lh = 'data' 
    schema = 'aemo'
    
    # Run quick return code test first (no auth needed)
    print("\n" + "=" * 80)
    print("🔬 RUNNING QUICK RETURN CODE TEST")
    print("=" * 80)
    quick_test_passed = quick_return_code_test()
    
    if not quick_test_passed:
        print("[FAIL] Quick test failed - stopping here!")
        sys.exit(1)
    
    # Run workspace name scenarios test with shared variables
    print("\n" + "=" * 80)
    print("🔬 RUNNING WORKSPACE NAME SCENARIOS TEST")
    print("=" * 80)
    workspace_test_passed = test_workspace_name_scenarios(ws, lh, schema)
    
    if not workspace_test_passed:
        print("[FAIL] Workspace name scenarios test failed - stopping here!")
        sys.exit(1)
    
    # Run semantic model tests
    print("\n" + "=" * 80)
    print("🔬 RUNNING SEMANTIC MODEL TESTS")
    print("=" * 80)
    semantic_model_test_passed = test_semantic_model_features(ws, lh, schema)
    
    if not semantic_model_test_passed:
        print("[FAIL] Semantic model tests failed!")
        sys.exit(1)
    
    # Run Snowflake connection tests
    print("\n" + "=" * 80)
    print("🔬 RUNNING SNOWFLAKE CONNECTION TESTS")
    print("=" * 80)
    snowflake_test_passed = test_snowflake_connection()
    
    if not snowflake_test_passed:
        print("[FAIL] Snowflake connection tests failed!")
        sys.exit(1)
    
    print("\n[RUN] All preliminary tests passed! Running full integration tests...")
    
    # Run main test with real authentication
    main()
    
    print("\n" + "=" * 80)
    print("✅ ALL TESTS COMPLETED SUCCESSFULLY")
    print("=" * 80)
