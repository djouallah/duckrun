<img src="https://raw.githubusercontent.com/djouallah/duckrun/main/duckrun.png" width="400" alt="Duckrun">

[![PyPI version](https://badge.fury.io/py/duckrun.svg)](https://badge.fury.io/py/duckrun)
[![Downloads](https://pepy.tech/badge/duckrun)](https://pepy.tech/project/duckrun)

A helper package for working with Microsoft Fabric lakehouses - orchestration, SQL queries, and file management powered by DuckDB/Delta_rs.

## Important Notes

**Requirements:**
- Lakehouses without schema are not supported 

**Delta Lake Version:** This package uses an older version of deltalake to maintain row size control capabilities, which is crucial for Power BI performance optimization. The newer Rust-based deltalake versions don't yet support the row group size parameters that are essential for optimal DirectLake performance.

## What It Does

It does orchestration, arbitrary SQL statements, and file manipulation. That's it - just stuff I encounter in my daily workflow when working with Fabric notebooks.

## Installation

```bash
pip install duckrun
```

For local usage (requires Azure CLI or interactive browser auth):

```bash
pip install duckrun[local]
```

Note: When running locally, your internet speed will be the main bottleneck.

## Getting Started

```python
import duckrun

# Connect to a lakehouse and start querying
con = duckrun.connect("My Workspace/MyLakehouse.lakehouse/dbo")
con.sql("SELECT * FROM my_table LIMIT 10").show()

# Write results to a new table
con.sql("SELECT * FROM source").write.mode("overwrite").saveAsTable("target")
```

That's it! Connect to your lakehouse and run SQL queries with DuckDB's speed.

## Core Functionalities

### 1. **Data Exploration & Querying**
Query Delta tables using SQL with DuckDB performance:
```python
con = duckrun.connect("workspace/lakehouse.lakehouse/dbo")
con.sql("SELECT * FROM sales WHERE year = 2024").show()
df = con.sql("SELECT COUNT(*) FROM orders").df()
```

### 2. **Write to Delta Tables**
Use Spark-style API to write query results:
```python
con.sql("SELECT * FROM source") \
    .write \
    .mode("overwrite") \
    .saveAsTable("target")
```

### 3. **Workspace Management**
List and create lakehouses:
```python
ws = duckrun.connect("My Workspace")
ws.list_lakehouses()
ws.create_lakehouse_if_not_exists("New Lakehouse")
```

### 4. **File Management**
Upload/download files to OneLake Files:
```python
con.copy("./local_folder", "remote_folder")
con.download("remote_folder", "./local_folder")
```

### 5. **Pipeline Orchestration**
Run SQL and Python tasks in sequence:
```python
con = duckrun.connect("workspace/lakehouse.lakehouse/dbo", sql_folder="./sql")
pipeline = [
    ('clean_data', 'overwrite'),
    ('aggregate', 'append')
]
con.run(pipeline)
```

### 6. **Semantic Model Deployment**
Deploy Power BI models with DirectLake:
```python
con.deploy("https://github.com/user/repo/model.bim")
con.deploy("./local_model.bim", dataset_name="Sales Model")
```

### 7. **Download Semantic Models**
Download BIM files from deployed models:
```python
bim_content = con.download_bim("Sales Model")
con.download_bim("Sales Model", "sales_model.bim")
```

### 8. **Schema Evolution & Partitioning**
Handle evolving schemas and optimize with partitioning:
```python
con.sql("SELECT * FROM source") \
    .write \
    .mode("append") \
    .option("mergeSchema", "true") \
    .partitionBy("region", "year") \
    .saveAsTable("target")
```

### 9. **SQL Lookup Functions**
Resolve workspace/lakehouse names from GUIDs in SQL:
```python
con.sql("""
    SELECT 
        workspace_id,
        get_workspace_name(workspace_id) as workspace_name,
        get_lakehouse_name(workspace_id, lakehouse_id) as lakehouse_name
    FROM storage_logs
""").show()
```

## Connection Format

```python
# Lakehouse connection with schema (recommended)
con = duckrun.connect("My Workspace/My Lakehouse.lakehouse/dbo")

# Workspace names with spaces are supported!
con = duckrun.connect("Data Analytics/Sales Data.lakehouse/analytics")

# Without schema (defaults to 'dbo', scans all schemas)
# ⚠️ This can be slow for large lakehouses!
con = duckrun.connect("My Workspace/My Lakehouse.lakehouse")

# With SQL folder for pipeline orchestration
con = duckrun.connect("My Workspace/My Lakehouse.lakehouse/dbo", sql_folder="./sql")
```

## Detailed Usage

### Data Exploration

## Detailed Documentation

### Data Exploration

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

# Schema evolution and partitioning (exact Spark API compatibility)
con.sql("""
    SELECT 
        customer_id,
        order_date,
        region,
        product_category,
        sales_amount,
        new_column_added_later  -- This column might not exist in target table
    FROM source_table
""").write \
    .mode("append") \
    .option("mergeSchema", "true") \
    .partitionBy("region", "product_category") \
    .saveAsTable("sales_partitioned")
```

**Note:** `.format("delta")` is optional - Delta is the default format!

### File Management (OneLake Files)

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

### Pipeline Orchestration

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

**Formats:**
- `('table_name', 'mode')` - Simple SQL with no parameters
- `('table_name', 'mode', {params})` - SQL with template parameters  
- `('table_name', 'mode', {params}, {delta_options})` - SQL with Delta Lake options

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

### Delta Lake Options (Schema Evolution & Partitioning)

Use the 4-tuple format for advanced Delta Lake features:

```python
pipeline = [
    # SQL with empty params but Delta options
    ('evolving_table', 'append', {}, {'mergeSchema': 'true'}),
    
    # SQL with both params AND Delta options
    ('sales_data', 'append', 
     {'region': 'North America'}, 
     {'mergeSchema': 'true', 'partitionBy': ['region', 'year']}),
     
    # Partitioning without schema merging
    ('time_series', 'overwrite', 
     {'start_date': '2024-01-01'}, 
     {'partitionBy': ['year', 'month']})
]
```

**Available Delta Options:**
- `mergeSchema: 'true'` - Automatically handle schema evolution (new columns)
- `partitionBy: ['col1', 'col2']` - Partition data by specified columns

## Advanced Features

### SQL Lookup Functions

Duckrun automatically registers helper functions that allow you to resolve workspace and lakehouse names from GUIDs directly in SQL queries. These are especially useful when working with storage logs or audit data that contains workspace/lakehouse IDs.

**Available Functions:**

```python
con = duckrun.connect("workspace/lakehouse.lakehouse/dbo")

# ID → Name lookups (most common use case)
con.sql("""
    SELECT 
        workspace_id,
        get_workspace_name(workspace_id) as workspace_name,
        lakehouse_id,
        get_lakehouse_name(workspace_id, lakehouse_id) as lakehouse_name
    FROM storage_logs
""").show()

# Name → ID lookups (reverse)
con.sql("""
    SELECT 
        workspace_name,
        get_workspace_id_from_name(workspace_name) as workspace_id,
        lakehouse_name,
        get_lakehouse_id_from_name(workspace_id, lakehouse_name) as lakehouse_id
    FROM configuration_table
""").show()
```

**Function Reference:**

- `get_workspace_name(workspace_id)` - Convert workspace GUID to display name
- `get_lakehouse_name(workspace_id, lakehouse_id)` - Convert lakehouse GUID to display name
- `get_workspace_id_from_name(workspace_name)` - Convert workspace name to GUID
- `get_lakehouse_id_from_name(workspace_id, lakehouse_name)` - Convert lakehouse name to GUID

**Features:**
- ✅ **Automatic Caching**: Results are cached to avoid repeated API calls
- ✅ **NULL on Error**: Returns `NULL` instead of errors for missing or inaccessible items
- ✅ **Fabric API Integration**: Resolves names using Microsoft Fabric REST API
- ✅ **Always Available**: Functions are automatically registered on connection

**Example Use Case:**

```python
# Enrich OneLake storage logs with friendly names
con = duckrun.connect("Analytics/Monitoring.lakehouse/dbo")

result = con.sql("""
    SELECT 
        workspace_id,
        get_workspace_name(workspace_id) as workspace_name,
        lakehouse_id,
        get_lakehouse_name(workspace_id, lakehouse_id) as lakehouse_name,
        operation_name,
        COUNT(*) as operation_count,
        SUM(bytes_transferred) as total_bytes
    FROM onelake_storage_logs
    WHERE log_date = CURRENT_DATE
    GROUP BY ALL
    ORDER BY workspace_name, lakehouse_name
""").show()
```

This makes it easy to create human-readable reports from GUID-based log data!

### Schema Evolution & Partitioning

Handle evolving schemas and optimize query performance with partitioning:

```python
# Using Spark-style API
con.sql("""
    SELECT 
        customer_id,
        region,
        product_category,
        sales_amount,
        -- New column that might not exist in target table
        discount_percentage
    FROM raw_sales
""").write \
    .mode("append") \
    .option("mergeSchema", "true") \
    .partitionBy("region", "product_category") \
    .saveAsTable("sales_partitioned")

# Using pipeline format
pipeline = [
    ('sales_summary', 'append', 
     {'batch_date': '2024-10-07'}, 
     {'mergeSchema': 'true', 'partitionBy': ['region', 'year']})
]
```

**Benefits:**
- 🔄 **Schema Evolution**: Automatically handles new columns without breaking existing queries
- ⚡ **Query Performance**: Partitioning improves performance for filtered queries

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

### Semantic Model Deployment

Deploy Power BI semantic models directly from BIM files using DirectLake mode:

```python
# Connect to lakehouse
con = duckrun.connect("Analytics/Sales.lakehouse/dbo")

# Deploy with auto-generated name (lakehouse_schema)
con.deploy("https://raw.githubusercontent.com/user/repo/main/model.bim")

# Deploy with custom name
con.deploy(
    "https://raw.githubusercontent.com/user/repo/main/sales_model.bim",
    dataset_name="Sales Analytics Model",
    wait_seconds=10  # Wait for permission propagation
)
```

**Features:**
- 🚀 **DirectLake Mode**: Deploys semantic models with DirectLake connection
- 🔄 **Automatic Configuration**: Auto-configures workspace, lakehouse, and schema connections
- 📦 **BIM from URL**: Load model definitions from GitHub or any accessible URL
- ⏱️ **Permission Handling**: Configurable wait time for permission propagation

**Use Cases:**
- Deploy semantic models as part of CI/CD pipelines
- Version control your semantic models in Git
- Automated model deployment across environments
- Streamline DirectLake model creation

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
    
    # Append to history with schema evolution (SQL with Delta options)
    ('sales_history', 'append', {}, {'mergeSchema': 'true', 'partitionBy': ['year', 'region']})
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

# 6. Deploy semantic model for Power BI
con.deploy(
    "https://raw.githubusercontent.com/user/repo/main/sales_model.bim",
    dataset_name="Sales Analytics"
)
```

**This example demonstrates:**
- 📁 **File uploads** to OneLake Files section
- 🔄 **Pipeline orchestration** with SQL and Python tasks  
- ⚡ **Fast data exploration** with DuckDB
- 💾 **Delta table creation** with Spark-style API
- 🔀 **Schema evolution** and partitioning
- 📤 **File downloads** from OneLake Files
- 📊 **Semantic model deployment** with DirectLake

## Schema Evolution & Partitioning Guide

### When to Use Schema Evolution

Use `mergeSchema: 'true'` when:
- Adding new columns to existing tables
- Source data schema changes over time  
- Working with evolving data pipelines
- Need backward compatibility

### When to Use Partitioning

Use `partitionBy` when:
- Queries frequently filter by specific columns (dates, regions, categories)
- Tables are large and need performance optimization
- Want to organize data logically for maintenance

### Best Practices

```python
# ✅ Good: Partition by commonly filtered columns
.partitionBy("year", "region")  # Often filtered: WHERE year = 2024 AND region = 'US'

# ❌ Avoid: High cardinality partitions  
.partitionBy("customer_id")  # Creates too many small partitions

# ✅ Good: Schema evolution for append operations
.mode("append").option("mergeSchema", "true")

# ✅ Good: Combined approach for data lakes
pipeline = [
    ('daily_sales', 'append', 
     {'batch_date': '2024-10-07'}, 
     {'mergeSchema': 'true', 'partitionBy': ['year', 'month', 'region']})
]
```

### Task Format Reference

```python
# 2-tuple: Simple SQL/Python
('task_name', 'mode')                    # SQL: no params, no Delta options
('function_name', (args))                # Python: function with arguments

# 3-tuple: SQL with parameters  
('task_name', 'mode', {'param': 'value'})

# 4-tuple: SQL with parameters AND Delta options
('task_name', 'mode', {'param': 'value'}, {'mergeSchema': 'true', 'partitionBy': ['col']})

# 4-tuple: Empty parameters but Delta options
('task_name', 'mode', {}, {'mergeSchema': 'true'})
```

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
