#!/usr/bin/env python3
"""
Basic test script for duckrun package
Converted from basic.ipynb notebook
"""

import sys
import os
import time
from psutil import *

# Add the parent directory to Python path to use local package
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import duckrun

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

if __name__ == "__main__":
    main()