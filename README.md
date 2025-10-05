<img src="https://raw.githubusercontent.com/djouallah/duckrun/main/duckrun.png" width="400" alt="Duckrun">

A helper package for stuff that made my life easier when working with Fabric Python notebooks. Just the things that actually made sense to me - nothing fancy

## Important Notes

**Requirements:**
- Lakehouse must have a schema (e.g., `dbo`, `sales`, `analytics`)
- Workspace and lakehouse names cannot contain spaces

**Why no spaces?** Duckrun uses simple name-based paths instead of GUIDs. This keeps the code clean and readable, which is perfect for data engineering workspaces where naming conventions are already well-established. Just use underscores or hyphens instead: `my_workspace` or `my-lakehouse`.

## What It Does

It does orchestration, arbitrary SQL statements, and file manipulation. That's it - just stuff I encounter in my daily workflow when working with Fabric notebooks.

## Installation

```bash
pip install duckrun
```
for local usage, Note: When running locally, your internet speed will be the main bottleneck.

```bash
pip install duckrun[local]
```

## Quick Start

```python
import duckrun

# Connect to your Fabric lakehouse with a specific schema
con = duckrun.connect("my_workspace/my_lakehouse.lakehouse/dbo")

# Schema defaults to 'dbo' if not specified (scans all schemas)
# ⚠️ WARNING: Scanning all schemas can be slow for large lakehouses!
con = duckrun.connect("my_workspace/my_lakehouse.lakehouse")

# Explore data
con.sql("SELECT * FROM my_table LIMIT 10").show()

# Write to Delta tables (Spark-style API)
con.sql("SELECT * FROM source").write.mode("overwrite").saveAsTable("target")

# Upload/download files to/from OneLake Files
con.copy("./local_folder", "target_folder")  # Upload files
con.download("target_folder", "./downloaded")  # Download files
```

That's it! No `sql_folder` needed for data exploration.

## Connection Format

```python
# With schema (recommended for better performance)
con = duckrun.connect("workspace/lakehouse.lakehouse/schema")

# Without schema (defaults to 'dbo', scans all schemas)
# ⚠️ This can be slow for large lakehouses!
con = duckrun.connect("workspace/lakehouse.lakehouse")

# With options
con = duckrun.connect("workspace/lakehouse.lakehouse/dbo", sql_folder="./sql")
```

### Multi-Schema Support

When you don't specify a schema, Duckrun will:
- **Default to `dbo`** for write operations
- **Scan all schemas** to discover and attach all Delta tables
- **Prefix table names** with schema to avoid conflicts (e.g., `dbo_customers`, `bronze_raw_data`)

**Performance Note:** Scanning all schemas requires listing all files in the lakehouse, which can be slow for large lakehouses with many tables. For better performance, always specify a schema when possible.

```python
# Fast: scans only 'dbo' schema
con = duckrun.connect("workspace/lakehouse.lakehouse/dbo")

# Slower: scans all schemas
con = duckrun.connect("workspace/lakehouse.lakehouse")

# Query tables from different schemas (when scanning all)
con.sql("SELECT * FROM dbo_customers").show()
con.sql("SELECT * FROM bronze_raw_data").show()
```

## Three Ways to Use Duckrun

### 1. Data Exploration (Spark-Style API)

Perfect for ad-hoc analysis and interactive notebooks:

```python
con = duckrun.connect("workspace/lakehouse.lakehouse/dbo")

# Query existing tables
con.sql("SELECT * FROM sales WHERE year = 2024").show()

# Get DataFrame
df = con.sql("SELECT COUNT(*) FROM orders").df()

# Write results to Delta tables
con.sql("""
    SELECT 
        customer_id,
        SUM(amount) as total
    FROM orders
    GROUP BY customer_id
""").write.mode("overwrite").saveAsTable("customer_totals")

# Append mode
con.sql("SELECT * FROM new_orders").write.mode("append").saveAsTable("orders")
```

**Note:** `.format("delta")` is optional - Delta is the default format!

### 2. File Management (OneLake Files)

Upload and download files to/from OneLake Files section (not Delta tables):

```python
con = duckrun.connect("workspace/lakehouse.lakehouse/dbo")

# Upload files to OneLake Files (remote_folder is required)
con.copy("./local_data", "uploaded_data")

# Upload only specific file types
con.copy("./reports", "daily_reports", ['.csv', '.parquet'])

# Upload with overwrite enabled (default is False for safety)
con.copy("./backup", "backups", overwrite=True)

# Download files from OneLake Files
con.download("uploaded_data", "./downloaded")

# Download only CSV files from a specific folder
con.download("daily_reports", "./reports", ['.csv'])
```

**Key Features:**
- ✅ **Files go to OneLake Files section** (not Delta Tables)
- ✅ **`remote_folder` parameter is required** for uploads (prevents accidental uploads)  
- ✅ **`overwrite=False` by default** (safer - prevents accidental overwrites)
- ✅ **File extension filtering** (e.g., only `.csv` or `.parquet` files)
- ✅ **Preserves folder structure** during upload/download
- ✅ **Progress reporting** with file sizes and upload status

### 3. Pipeline Orchestration

For production workflows with reusable SQL and Python tasks:

```python
con = duckrun.connect(
    "my_workspace/my_lakehouse.lakehouse/dbo",
    sql_folder="./sql"  # folder with .sql and .py files
)

# Define pipeline
pipeline = [
    ('download_data', (url, path)),    # Python task
    ('clean_data', 'overwrite'),       # SQL task  
    ('aggregate', 'append')            # SQL task
]

# Run it
con.run(pipeline)
```

## Pipeline Tasks

### Python Tasks

**Format:** `('function_name', (arg1, arg2, ...))`

Create `sql_folder/function_name.py`:

```python
# sql_folder/download_data.py
def download_data(url, path):
    # your code here
    return 1  # 1 = success, 0 = failure
```

### SQL Tasks

**Format:** `('table_name', 'mode')` or `('table_name', 'mode', {params})`

Create `sql_folder/table_name.sql`:

```sql
-- sql_folder/clean_data.sql
SELECT 
    id,
    TRIM(name) as name,
    date
FROM raw_data
WHERE date >= '2024-01-01'
```

**Write Modes:**
- `overwrite` - Replace table completely
- `append` - Add to existing table  
- `ignore` - Create only if doesn't exist

### Parameterized SQL

Built-in parameters (always available):
- `$ws` - workspace name
- `$lh` - lakehouse name
- `$schema` - schema name

Custom parameters:

```python
pipeline = [
    ('sales', 'append', {'start_date': '2024-01-01', 'end_date': '2024-12-31'})
]
```

```sql
-- sql_folder/sales.sql
SELECT * FROM transactions
WHERE date BETWEEN '$start_date' AND '$end_date'
```

## Advanced Features

### Table Name Variants

Use `__` to create multiple versions of the same table:

```python
pipeline = [
    ('sales__initial', 'overwrite'),     # writes to 'sales'
    ('sales__incremental', 'append'),    # appends to 'sales'
]
```

Both tasks write to the `sales` table but use different SQL files (`sales__initial.sql` and `sales__incremental.sql`).

### Remote SQL Files

Load tasks from GitHub or any URL:

```python
con = duckrun.connect(
    "Analytics/Sales.lakehouse/dbo",
    sql_folder="https://raw.githubusercontent.com/user/repo/main/sql"
)
```

### Early Exit on Failure

**Pipelines automatically stop when any task fails** - subsequent tasks won't run.

For **SQL tasks**, failure is automatic:
- If the query has a syntax error or runtime error, the task fails
- The pipeline stops immediately

For **Python tasks**, you control success/failure by returning:
- `1` = Success → pipeline continues to next task
- `0` = Failure → pipeline stops, remaining tasks are skipped

Example:

```python
# sql_folder/download_data.py
def download_data(url, path):
    try:
        response = requests.get(url)
        response.raise_for_status()
        # save data...
        return 1  # Success - pipeline continues
    except Exception as e:
        print(f"Download failed: {e}")
        return 0  # Failure - pipeline stops here
```

```python
pipeline = [
    ('download_data', (url, path)),     # If returns 0, stops here
    ('clean_data', 'overwrite'),        # Won't run if download failed
    ('aggregate', 'append')             # Won't run if download failed
]

success = con.run(pipeline)  # Returns True only if ALL tasks succeed
```

This prevents downstream tasks from processing incomplete or corrupted data.

### Delta Lake Optimization

Duckrun automatically:
- Compacts small files when file count exceeds threshold (default: 100)
- Vacuums old versions on overwrite
- Cleans up metadata

Customize compaction threshold:

```python
con = duckrun.connect(
    "workspace/lakehouse.lakehouse/dbo",
    compaction_threshold=50  # compact after 50 files
)
```

## File Management API Reference

### `copy(local_folder, remote_folder, file_extensions=None, overwrite=False)`

Upload files from a local folder to OneLake Files section.

**Parameters:**
- `local_folder` (str): Path to local folder containing files to upload
- `remote_folder` (str): **Required** target folder path in OneLake Files  
- `file_extensions` (list, optional): Filter by file extensions (e.g., `['.csv', '.parquet']`)
- `overwrite` (bool, optional): Whether to overwrite existing files (default: False)

**Returns:** `True` if all files uploaded successfully, `False` otherwise

**Examples:**
```python
# Upload all files to a target folder
con.copy("./data", "processed_data")

# Upload only CSV and Parquet files
con.copy("./reports", "monthly_reports", ['.csv', '.parquet'])

# Upload with overwrite enabled
con.copy("./backup", "daily_backup", overwrite=True)
```

### `download(remote_folder="", local_folder="./downloaded_files", file_extensions=None, overwrite=False)`

Download files from OneLake Files section to a local folder.

**Parameters:**
- `remote_folder` (str, optional): Source folder path in OneLake Files (default: root)
- `local_folder` (str, optional): Local destination folder (default: "./downloaded_files")  
- `file_extensions` (list, optional): Filter by file extensions (e.g., `['.csv', '.json']`)
- `overwrite` (bool, optional): Whether to overwrite existing local files (default: False)

**Returns:** `True` if all files downloaded successfully, `False` otherwise

**Examples:**
```python
# Download all files from OneLake Files root
con.download()

# Download from specific folder
con.download("processed_data", "./local_data")

# Download only JSON files
con.download("config", "./configs", ['.json'])
```

**Important Notes:**
- Files are uploaded/downloaded to/from the **OneLake Files section**, not Delta Tables
- The `remote_folder` parameter is **required** for uploads to prevent accidental uploads
- Both methods default to `overwrite=False` for safety
- Folder structure is preserved during upload/download operations
- Progress is reported with file names, sizes, and upload/download status

## Complete Example

```python
import duckrun

# Connect (specify schema for best performance)
con = duckrun.connect("Analytics/Sales.lakehouse/dbo", sql_folder="./sql")

# 1. Upload raw data files to OneLake Files
con.copy("./raw_data", "raw_uploads", ['.csv', '.json'])

# 2. Pipeline with mixed tasks
pipeline = [
    # Download raw data (Python)
    ('fetch_api_data', ('https://api.example.com/sales', 'raw')),
    
    # Clean and transform (SQL)
    ('clean_sales', 'overwrite'),
    
    # Aggregate by region (SQL with params)
    ('regional_summary', 'overwrite', {'min_amount': 1000}),
    
    # Append to history (SQL)
    ('sales_history', 'append')
]

# Run pipeline
success = con.run(pipeline)

# 3. Explore results using DuckDB
con.sql("SELECT * FROM regional_summary").show()

# 4. Export to new Delta table
con.sql("""
    SELECT region, SUM(total) as grand_total
    FROM regional_summary
    GROUP BY region
""").write.mode("overwrite").saveAsTable("region_totals")

# 5. Download processed files for external systems
con.download("processed_reports", "./exports", ['.csv'])
```

**This example demonstrates:**
- 📁 **File uploads** to OneLake Files section
- 🔄 **Pipeline orchestration** with SQL and Python tasks  
- ⚡ **Fast data exploration** with DuckDB
- 💾 **Delta table creation** with Spark-style API
- 📤 **File downloads** from OneLake Files

## How It Works

1. **Connection**: Duckrun connects to your Fabric lakehouse using OneLake and Azure authentication
2. **Table Discovery**: Automatically scans for Delta tables in your schema (or all schemas) and creates DuckDB views
3. **Query Execution**: Run SQL queries directly against Delta tables using DuckDB's speed
4. **Write Operations**: Results are written back as Delta tables with automatic optimization
5. **Pipelines**: Orchestrate complex workflows with reusable SQL and Python tasks

## Real-World Example

For a complete production example, see [fabric_demo](https://github.com/djouallah/fabric_demo).

## License

MIT