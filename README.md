<img src="https://raw.githubusercontent.com/djouallah/duckrun/main/duckrun.png" width="400" alt="Duckrun"><img src="https://raw.githubusercontent.com/djouallah/duckrun/main/duckrun.png" width="400" alt="Duckrun">



A helper package for working with Microsoft Fabric lakehouses - orchestration, SQL queries, and file management powered by DuckDB.A helper package for stuff that made my life easier when working with Fabric Python notebooks. Just the things that actually made sense to me - nothing fancy



## Installation## Important Notes



```bash**Requirements:**

pip install duckrun- Lakehouse must have a schema (e.g., `dbo`, `sales`, `analytics`)

```- **Workspace names with spaces are fully supported!** ✅



For local usage (requires Azure CLI or interactive browser auth):

```bash**Delta Lake Version:** This package uses an older version of deltalake to maintain row size control capabilities, which is crucial for Power BI performance optimization. The newer Rust-based deltalake versions don't yet support the row group size parameters that are essential for optimal DirectLake performance.

pip install duckrun[local]

```## What It Does



## Quick StartIt does orchestration, arbitrary SQL statements, and file manipulation. That's it - just stuff I encounter in my daily workflow when working with Fabric notebooks.



### Basic Usage## Installation



```python```bash

import duckrunpip install duckrun

```

# Connect to a lakehouse and query datafor local usage, Note: When running locally, your internet speed will be the main bottleneck.

con = duckrun.connect("My Workspace/data.lakehouse/dbo")

con.sql("SELECT * FROM my_table LIMIT 10").show()```bash

pip install duckrun[local]

# Write query results to a new table```

con.sql("SELECT * FROM source WHERE year = 2024") \

   .write.mode("overwrite").saveAsTable("filtered_data")## Quick Start



# Upload/download files### Simple Example for New Users

con.copy("./local_data", "remote_folder")  # Upload

con.download("remote_folder", "./local")    # Download```python

```import duckrun



### Complete Example# Connect to a workspace and manage lakehouses

con = duckrun.connect('My Workspace')

```pythoncon.list_lakehouses()                           # See what lakehouses exist

import duckruncon.create_lakehouse_if_not_exists('data')      # Create if needed



# 1. Connect to lakehouse# Connect to a specific lakehouse and query data

con = duckrun.connect("Analytics/Sales.lakehouse/dbo")con = duckrun.connect("My Workspace/data.lakehouse/dbo")

con.sql("SELECT * FROM my_table LIMIT 10").show()

# 2. Query and explore data```

result = con.sql("""

    SELECT region, SUM(amount) as total### Full Feature Overview

    FROM sales

    WHERE year = 2024```python

    GROUP BY regionimport duckrun

""").show()

# 1. Workspace Management (list and create lakehouses)

# 3. Create new tables from queriesws = duckrun.connect("My Workspace")

con.sql("SELECT * FROM sales WHERE region = 'US'") \lakehouses = ws.list_lakehouses()  # Returns list of lakehouse names

   .write.mode("overwrite").saveAsTable("us_sales")ws.create_lakehouse_if_not_exists("New_Lakehouse")



# 4. Upload files to OneLake# 2. Connect to lakehouse with a specific schema

con.copy("./reports", "monthly_reports", ['.csv'])con = duckrun.connect("My Workspace/MyLakehouse.lakehouse/dbo")



# 5. Run data pipeline# Workspace names with spaces are supported!

pipeline = [con = duckrun.connect("Data Analytics/SalesData.lakehouse/analytics")

    ('clean_data', 'overwrite'),

    ('aggregate', 'append', {'min_amount': 1000})# Schema defaults to 'dbo' if not specified (scans all schemas)

]# ⚠️ WARNING: Scanning all schemas can be slow for large lakehouses!

con.run(pipeline)con = duckrun.connect("My Workspace/My_Lakehouse.lakehouse")

```

# 3. Explore data

---con.sql("SELECT * FROM my_table LIMIT 10").show()



## Core Functions# 4. Write to Delta tables (Spark-style API)

con.sql("SELECT * FROM source").write.mode("overwrite").saveAsTable("target")

### Connection

# 5. Upload/download files to/from OneLake Files

#### `connect(connection_string, sql_folder=None, compaction_threshold=100)`con.copy("./local_folder", "target_folder")  # Upload files

con.download("target_folder", "./downloaded")  # Download files

Connect to a workspace or lakehouse.```



**Parameters:**That's it! No `sql_folder` needed for data exploration.

- `connection_string` (str): Connection path

  - Workspace only: `"My Workspace"`## Connection Format

  - Lakehouse with schema: `"My Workspace/lakehouse.lakehouse/dbo"`

  - Lakehouse without schema: `"My Workspace/lakehouse.lakehouse"` (scans all schemas)```python

- `sql_folder` (str, optional): Path to SQL/Python files for pipelines# Workspace management (list and create lakehouses)

- `compaction_threshold` (int): File count before auto-compaction (default: 100)ws = duckrun.connect("My Workspace")

ws.list_lakehouses()  # Returns: ['lakehouse1', 'lakehouse2', ...]

**Returns:** `Duckrun` instance or `WorkspaceConnection` instancews.create_lakehouse_if_not_exists("New Lakehouse")



**Examples:**# Lakehouse connection with schema (recommended for best performance)

```pythoncon = duckrun.connect("My Workspace/My Lakehouse.lakehouse/dbo")

# Workspace management

ws = duckrun.connect("My Workspace")# Supports workspace names with spaces!

ws.list_lakehouses()con = duckrun.connect("Data Analytics/Sales Data.lakehouse/analytics")

ws.create_lakehouse_if_not_exists("new_lakehouse")

# Without schema (defaults to 'dbo', scans all schemas)

# Lakehouse connection (recommended - specify schema)# ⚠️ This can be slow for large lakehouses!

con = duckrun.connect("My Workspace/data.lakehouse/dbo")con = duckrun.connect("My Workspace/My Lakehouse.lakehouse")



# With SQL folder for pipelines# With SQL folder for pipeline orchestration

con = duckrun.connect("My Workspace/data.lakehouse/dbo", sql_folder="./sql")con = duckrun.connect("My Workspace/My Lakehouse.lakehouse/dbo", sql_folder="./sql")

``````



**Notes:**### Multi-Schema Support

- Workspace names with spaces are fully supported ✅

- Specifying schema improves connection speedWhen you don't specify a schema, Duckrun will:

- Without schema, all schemas are scanned (slower for large lakehouses)- **Default to `dbo`** for write operations

- **Scan all schemas** to discover and attach all Delta tables

---- **Prefix table names** with schema to avoid conflicts (e.g., `dbo_customers`, `bronze_raw_data`)



### Query & Write**Performance Note:** Scanning all schemas requires listing all files in the lakehouse, which can be slow for large lakehouses with many tables. For better performance, always specify a schema when possible.



#### `sql(query)````python

# Fast: scans only 'dbo' schema

Execute SQL query with Spark-style write API.con = duckrun.connect("workspace/lakehouse.lakehouse/dbo")



**Parameters:**# Slower: scans all schemas

- `query` (str): SQL query to executecon = duckrun.connect("workspace/lakehouse.lakehouse")



**Returns:** `QueryResult` object with methods:# Query tables from different schemas (when scanning all)

- `.show(max_width=None)` - Display results in consolecon.sql("SELECT * FROM dbo_customers").show()

- `.df()` - Get pandas DataFramecon.sql("SELECT * FROM bronze_raw_data").show()

- `.write` - Access write API (see below)```



**Examples:**## Three Ways to Use Duckrun

```python

# Show results### 1. Data Exploration (Spark-Style API)

con.sql("SELECT * FROM sales LIMIT 10").show()

Perfect for ad-hoc analysis and interactive notebooks:

# Get DataFrame

df = con.sql("SELECT COUNT(*) FROM orders").df()```python

con = duckrun.connect("workspace/lakehouse.lakehouse/dbo")

# Write to table

con.sql("SELECT * FROM source").write.mode("overwrite").saveAsTable("target")# Query existing tables

```con.sql("SELECT * FROM sales WHERE year = 2024").show()



#### Write API# Get DataFrame

df = con.sql("SELECT COUNT(*) FROM orders").df()

**Methods:**

- `.mode(mode)` - Set write mode: `"overwrite"`, `"append"`, or `"ignore"`# Write results to Delta tables

- `.option(key, value)` - Set Delta Lake optioncon.sql("""

- `.partitionBy(*cols)` - Partition by columns    SELECT 

- `.saveAsTable(table_name)` - Write to Delta table        customer_id,

        SUM(amount) as total

**Examples:**    FROM orders

```python    GROUP BY customer_id

# Simple write""").write.mode("overwrite").saveAsTable("customer_totals")

con.sql("SELECT * FROM data").write.mode("overwrite").saveAsTable("target")

# Append mode

# With schema evolutioncon.sql("SELECT * FROM new_orders").write.mode("append").saveAsTable("orders")

con.sql("SELECT * FROM source") \

   .write.mode("append") \# Schema evolution and partitioning (exact Spark API compatibility)

   .option("mergeSchema", "true") \con.sql("""

   .saveAsTable("evolving_table")    SELECT 

        customer_id,

# With partitioning        order_date,

con.sql("SELECT * FROM sales") \        region,

   .write.mode("overwrite") \        product_category,

   .partitionBy("region", "year") \        sales_amount,

   .saveAsTable("partitioned_sales")        new_column_added_later  -- This column might not exist in target table

    FROM source_table

# Combined""").write \

con.sql("SELECT * FROM data") \    .mode("append") \

   .write.mode("append") \    .option("mergeSchema", "true") \

   .option("mergeSchema", "true") \    .partitionBy("region", "product_category") \

   .partitionBy("date", "category") \    .saveAsTable("sales_partitioned")

   .saveAsTable("final_table")```

```

**Note:** `.format("delta")` is optional - Delta is the default format!

---

### 2. File Management (OneLake Files)

### File Operations

Upload and download files to/from OneLake Files section (not Delta tables):

#### `copy(local_folder, remote_folder, file_extensions=None, overwrite=False)`

```python

Upload files from local folder to OneLake Files section.con = duckrun.connect("workspace/lakehouse.lakehouse/dbo")



**Parameters:**# Upload files to OneLake Files (remote_folder is required)

- `local_folder` (str): Local source folder pathcon.copy("./local_data", "uploaded_data")

- `remote_folder` (str): Remote target folder in OneLake Files (required)

- `file_extensions` (list, optional): Filter by extensions (e.g., `['.csv', '.parquet']`)# Upload only specific file types

- `overwrite` (bool): Overwrite existing files (default: `False`)con.copy("./reports", "daily_reports", ['.csv', '.parquet'])



**Returns:** `True` if successful, `False` otherwise# Upload with overwrite enabled (default is False for safety)

con.copy("./backup", "backups", overwrite=True)

**Examples:**

```python# Download files from OneLake Files

# Upload all filescon.download("uploaded_data", "./downloaded")

con.copy("./data", "processed_data")

# Download only CSV files from a specific folder

# Upload specific file typescon.download("daily_reports", "./reports", ['.csv'])

con.copy("./reports", "monthly", ['.csv', '.xlsx'])```



# With overwrite**Key Features:**

con.copy("./backup", "daily_backup", overwrite=True)- ✅ **Files go to OneLake Files section** (not Delta Tables)

```- ✅ **`remote_folder` parameter is required** for uploads (prevents accidental uploads)  

- ✅ **`overwrite=False` by default** (safer - prevents accidental overwrites)

#### `download(remote_folder="", local_folder="./downloaded_files", file_extensions=None, overwrite=False)`- ✅ **File extension filtering** (e.g., only `.csv` or `.parquet` files)

- ✅ **Preserves folder structure** during upload/download

Download files from OneLake Files section to local folder.- ✅ **Progress reporting** with file sizes and upload status



**Parameters:**### 3. Pipeline Orchestration

- `remote_folder` (str): Source folder in OneLake Files (default: root)

- `local_folder` (str): Local destination folder (default: `"./downloaded_files"`)For production workflows with reusable SQL and Python tasks:

- `file_extensions` (list, optional): Filter by extensions

- `overwrite` (bool): Overwrite existing files (default: `False`)```python

con = duckrun.connect(

**Returns:** `True` if successful, `False` otherwise    "my_workspace/my_lakehouse.lakehouse/dbo",

    sql_folder="./sql"  # folder with .sql and .py files

**Examples:**)

```python

# Download from root# Define pipeline

con.download()pipeline = [

    ('download_data', (url, path)),    # Python task

# Download from specific folder    ('clean_data', 'overwrite'),       # SQL task  

con.download("processed_data", "./local_data")    ('aggregate', 'append')            # SQL task

]

# Download specific file types

con.download("reports", "./exports", ['.csv'])# Run it

```con.run(pipeline)

```

---

## Pipeline Tasks

### Pipeline Orchestration

### Python Tasks

#### `run(pipeline)`

**Format:** `('function_name', (arg1, arg2, ...))`

Execute a pipeline of SQL and Python tasks.

Create `sql_folder/function_name.py`:

**Parameters:**

- `pipeline` (list): List of task tuples```python

# sql_folder/download_data.py

**Returns:** `True` if all tasks succeeded, `False` if any faileddef download_data(url, path):

    # your code here

**Task Formats:**    return 1  # 1 = success, 0 = failure

```python```

# Python task: ('function_name', (arg1, arg2, ...))

('download_data', ('https://api.example.com/data', './raw'))### SQL Tasks



# SQL task: ('table_name', 'mode')**Formats:**

('clean_data', 'overwrite')- `('table_name', 'mode')` - Simple SQL with no parameters

- `('table_name', 'mode', {params})` - SQL with template parameters  

# SQL with params: ('table_name', 'mode', {params})- `('table_name', 'mode', {params}, {delta_options})` - SQL with Delta Lake options

('filter_data', 'append', {'min_value': 100})

Create `sql_folder/table_name.sql`:

# SQL with Delta options: ('table_name', 'mode', {params}, {options})

('evolving_table', 'append', {}, {'mergeSchema': 'true', 'partitionBy': ['region']})```sql

```-- sql_folder/clean_data.sql

SELECT 

**Write Modes:**    id,

- `"overwrite"` - Replace table completely    TRIM(name) as name,

- `"append"` - Add to existing table    date

- `"ignore"` - Create only if doesn't existFROM raw_data

WHERE date >= '2024-01-01'

**Examples:**```

```python

con = duckrun.connect("workspace/lakehouse.lakehouse/dbo", sql_folder="./sql")**Write Modes:**

- `overwrite` - Replace table completely

# Simple pipeline- `append` - Add to existing table  

pipeline = [- `ignore` - Create only if doesn't exist

    ('extract_data', 'overwrite'),

    ('transform', 'append'),### Parameterized SQL

    ('load_final', 'overwrite')

]Built-in parameters (always available):

con.run(pipeline)- `$ws` - workspace name

- `$lh` - lakehouse name

# With parameters- `$schema` - schema name

pipeline = [

    ('fetch_api', ('https://api.com/data', './raw')),Custom parameters:

    ('clean', 'overwrite', {'date': '2024-01-01'}),

    ('aggregate', 'append', {}, {'partitionBy': ['region']})```python

]pipeline = [

con.run(pipeline)    ('sales', 'append', {'start_date': '2024-01-01', 'end_date': '2024-12-31'})

```]

```

**Pipeline Behavior:**

- SQL tasks automatically fail on errors (syntax, runtime)```sql

- Python tasks control success/failure by returning `1` (success) or `0` (failure)-- sql_folder/sales.sql

- Pipeline stops immediately when any task failsSELECT * FROM transactions

- Remaining tasks are skippedWHERE date BETWEEN '$start_date' AND '$end_date'

```

---

### Delta Lake Options (Schema Evolution & Partitioning)

### Workspace Management

Use the 4-tuple format for advanced Delta Lake features:

#### `list_lakehouses()`

```python

List all lakehouses in the workspace.pipeline = [

    # SQL with empty params but Delta options

**Returns:** List of lakehouse names (strings)    ('evolving_table', 'append', {}, {'mergeSchema': 'true'}),

    

**Example:**    # SQL with both params AND Delta options

```python    ('sales_data', 'append', 

ws = duckrun.connect("My Workspace")     {'region': 'North America'}, 

lakehouses = ws.list_lakehouses()     {'mergeSchema': 'true', 'partitionBy': ['region', 'year']}),

print(lakehouses)  # ['lakehouse1', 'lakehouse2', ...]     

```    # Partitioning without schema merging

    ('time_series', 'overwrite', 

#### `create_lakehouse_if_not_exists(lakehouse_name)`     {'start_date': '2024-01-01'}, 

     {'partitionBy': ['year', 'month']})

Create a lakehouse if it doesn't already exist.]

```

**Parameters:**

- `lakehouse_name` (str): Name of the lakehouse to create**Available Delta Options:**

- `mergeSchema: 'true'` - Automatically handle schema evolution (new columns)

**Returns:** `True` if exists or was created, `False` on error- `partitionBy: ['col1', 'col2']` - Partition data by specified columns



**Example:**## Advanced Features

```python

ws = duckrun.connect("My Workspace")### SQL Lookup Functions

success = ws.create_lakehouse_if_not_exists("new_lakehouse")

```Duckrun automatically registers helper functions that allow you to resolve workspace and lakehouse names from GUIDs directly in SQL queries. These are especially useful when working with storage logs or audit data that contains workspace/lakehouse IDs.



---**Available Functions:**



### SQL Lookup Functions```python

con = duckrun.connect("workspace/lakehouse.lakehouse/dbo")

Built-in SQL functions for resolving workspace/lakehouse names from GUIDs.

# ID → Name lookups (most common use case)

**Functions:**con.sql("""

- `get_workspace_name(workspace_id)` - GUID → workspace name    SELECT 

- `get_lakehouse_name(workspace_id, lakehouse_id)` - GUIDs → lakehouse name        workspace_id,

- `get_workspace_id_from_name(workspace_name)` - workspace name → GUID        get_workspace_name(workspace_id) as workspace_name,

- `get_lakehouse_id_from_name(workspace_id, lakehouse_name)` - lakehouse name → GUID        lakehouse_id,

        get_lakehouse_name(workspace_id, lakehouse_id) as lakehouse_name

**Features:**    FROM storage_logs

- Automatically cached to avoid repeated API calls""").show()

- Return `NULL` for missing or inaccessible items

- Always available after connection# Name → ID lookups (reverse)

con.sql("""

**Example:**    SELECT 

```python        workspace_name,

con = duckrun.connect("workspace/lakehouse.lakehouse/dbo")        get_workspace_id_from_name(workspace_name) as workspace_id,

        lakehouse_name,

# Enrich storage logs with friendly names        get_lakehouse_id_from_name(workspace_id, lakehouse_name) as lakehouse_id

result = con.sql("""    FROM configuration_table

    SELECT """).show()

        workspace_id,```

        get_workspace_name(workspace_id) as workspace_name,

        lakehouse_id,**Function Reference:**

        get_lakehouse_name(workspace_id, lakehouse_id) as lakehouse_name,

        operation_count- `get_workspace_name(workspace_id)` - Convert workspace GUID to display name

    FROM storage_logs- `get_lakehouse_name(workspace_id, lakehouse_id)` - Convert lakehouse GUID to display name

    ORDER BY workspace_name, lakehouse_name- `get_workspace_id_from_name(workspace_name)` - Convert workspace name to GUID

""").show()- `get_lakehouse_id_from_name(workspace_id, lakehouse_name)` - Convert lakehouse name to GUID

```

**Features:**

---- ✅ **Automatic Caching**: Results are cached to avoid repeated API calls

- ✅ **NULL on Error**: Returns `NULL` instead of errors for missing or inaccessible items

### Semantic Model Deployment- ✅ **Fabric API Integration**: Resolves names using Microsoft Fabric REST API

- ✅ **Always Available**: Functions are automatically registered on connection

#### `deploy(bim_url, dataset_name=None, wait_seconds=5)`

**Example Use Case:**

Deploy a Power BI semantic model from a BIM file using DirectLake mode.

```python

**Parameters:**# Enrich OneLake storage logs with friendly names

- `bim_url` (str): URL to BIM file, local path, or `"workspace/model"` formatcon = duckrun.connect("Analytics/Monitoring.lakehouse/dbo")

- `dataset_name` (str, optional): Name for semantic model (auto-generated if not provided)

- `wait_seconds` (int): Wait time for permission propagation (default: 5)result = con.sql("""

    SELECT 

**Returns:** `1` for success, `0` for failure        workspace_id,

        get_workspace_name(workspace_id) as workspace_name,

**Examples:**        lakehouse_id,

```python        get_lakehouse_name(workspace_id, lakehouse_id) as lakehouse_name,

con = duckrun.connect("Analytics/Sales.lakehouse/dbo")        operation_name,

        COUNT(*) as operation_count,

# From URL        SUM(bytes_transferred) as total_bytes

con.deploy("https://raw.githubusercontent.com/user/repo/main/model.bim")    FROM onelake_storage_logs

    WHERE log_date = CURRENT_DATE

# With custom name    GROUP BY ALL

con.deploy(    ORDER BY workspace_name, lakehouse_name

    "https://github.com/user/repo/raw/main/sales.bim",""").show()

    dataset_name="Sales Analytics"```

)

This makes it easy to create human-readable reports from GUID-based log data!

# From workspace/model (copies from another workspace)

con.deploy("Source Workspace/Source Model", dataset_name="Sales Copy")### Schema Evolution & Partitioning

```

Handle evolving schemas and optimize query performance with partitioning:

---

```python

### Utility Methods# Using Spark-style API

con.sql("""

#### `get_workspace_id()`    SELECT 

        customer_id,

Get the workspace ID (GUID or name without spaces).        region,

        product_category,

**Returns:** Workspace ID string        sales_amount,

        -- New column that might not exist in target table

#### `get_lakehouse_id()`        discount_percentage

    FROM raw_sales

Get the lakehouse ID (GUID or name).""").write \

    .mode("append") \

**Returns:** Lakehouse ID string    .option("mergeSchema", "true") \

    .partitionBy("region", "product_category") \

#### `get_connection()`    .saveAsTable("sales_partitioned")



Get the underlying DuckDB connection object.# Using pipeline format

pipeline = [

**Returns:** DuckDB connection    ('sales_summary', 'append', 

     {'batch_date': '2024-10-07'}, 

#### `close()`     {'mergeSchema': 'true', 'partitionBy': ['region', 'year']})

]

Close the DuckDB connection.```



**Example:****Benefits:**

```python- 🔄 **Schema Evolution**: Automatically handles new columns without breaking existing queries

con.close()- ⚡ **Query Performance**: Partitioning improves performance for filtered queries

```

### Table Name Variants

---

Use `__` to create multiple versions of the same table:

## Advanced Features

```python

### Schema Evolutionpipeline = [

    ('sales__initial', 'overwrite'),     # writes to 'sales'

Automatically handle schema changes (new columns) using `mergeSchema`:    ('sales__incremental', 'append'),    # appends to 'sales'

]

```python```

# Using write API

con.sql("SELECT * FROM source").write \Both tasks write to the `sales` table but use different SQL files (`sales__initial.sql` and `sales__incremental.sql`).

   .mode("append") \

   .option("mergeSchema", "true") \### Remote SQL Files

   .saveAsTable("evolving_table")

Load tasks from GitHub or any URL:

# Using pipeline

pipeline = [```python

    ('table', 'append', {}, {'mergeSchema': 'true'})con = duckrun.connect(

]    "Analytics/Sales.lakehouse/dbo",

```    sql_folder="https://raw.githubusercontent.com/user/repo/main/sql"

)

### Partitioning```



Optimize query performance by partitioning data:### Early Exit on Failure



```python**Pipelines automatically stop when any task fails** - subsequent tasks won't run.

# Partition by single column

con.sql("SELECT * FROM sales").write \For **SQL tasks**, failure is automatic:

   .mode("overwrite") \- If the query has a syntax error or runtime error, the task fails

   .partitionBy("region") \- The pipeline stops immediately

   .saveAsTable("partitioned_sales")

For **Python tasks**, you control success/failure by returning:

# Partition by multiple columns- `1` = Success → pipeline continues to next task

con.sql("SELECT * FROM orders").write \- `0` = Failure → pipeline stops, remaining tasks are skipped

   .mode("overwrite") \

   .partitionBy("year", "month", "region") \Example:

   .saveAsTable("time_partitioned")

``````python

# sql_folder/download_data.py

**Best Practices:**def download_data(url, path):

- ✅ Partition by columns frequently used in WHERE clauses    try:

- ✅ Use low to medium cardinality columns (dates, regions, categories)        response = requests.get(url)

- ❌ Avoid high cardinality columns (customer_id, transaction_id)        response.raise_for_status()

        # save data...

### SQL Template Parameters        return 1  # Success - pipeline continues

    except Exception as e:

Use template parameters in SQL files:        print(f"Download failed: {e}")

        return 0  # Failure - pipeline stops here

**Built-in parameters:**```

- `$ws` - workspace name

- `$lh` - lakehouse name```python

- `$schema` - schema namepipeline = [

- `$storage_account` - storage account name    ('download_data', (url, path)),     # If returns 0, stops here

- `$tables_url` - base URL for Tables folder    ('clean_data', 'overwrite'),        # Won't run if download failed

- `$files_url` - base URL for Files folder    ('aggregate', 'append')             # Won't run if download failed

]

**Custom parameters:**

```pythonsuccess = con.run(pipeline)  # Returns True only if ALL tasks succeed

# sql/sales.sql```

SELECT * FROM transactions

WHERE date >= '$start_date' AND region = '$region'This prevents downstream tasks from processing incomplete or corrupted data.

```

### Semantic Model Deployment

```python

pipeline = [Deploy Power BI semantic models directly from BIM files using DirectLake mode:

    ('sales', 'append', {'start_date': '2024-01-01', 'region': 'US'})

]```python

```# Connect to lakehouse

con = duckrun.connect("Analytics/Sales.lakehouse/dbo")

### Table Name Variants

# Deploy with auto-generated name (lakehouse_schema)

Create multiple SQL files for the same table:con.deploy("https://raw.githubusercontent.com/user/repo/main/model.bim")



```python# Deploy with custom name

pipeline = [con.deploy(

    ('sales__initial', 'overwrite'),     # writes to 'sales'    "https://raw.githubusercontent.com/user/repo/main/sales_model.bim",

    ('sales__incremental', 'append'),    # appends to 'sales'    dataset_name="Sales Analytics Model",

]    wait_seconds=10  # Wait for permission propagation

```)

```

Both use different SQL files but write to the same `sales` table.

**Features:**

### Remote SQL Files- 🚀 **DirectLake Mode**: Deploys semantic models with DirectLake connection

- 🔄 **Automatic Configuration**: Auto-configures workspace, lakehouse, and schema connections

Load SQL/Python files from GitHub or any URL:- 📦 **BIM from URL**: Load model definitions from GitHub or any accessible URL

- ⏱️ **Permission Handling**: Configurable wait time for permission propagation

```python

con = duckrun.connect(**Use Cases:**

    "workspace/lakehouse.lakehouse/dbo",- Deploy semantic models as part of CI/CD pipelines

    sql_folder="https://raw.githubusercontent.com/user/repo/main/sql"- Version control your semantic models in Git

)- Automated model deployment across environments

```- Streamline DirectLake model creation



### Auto-Compaction### Delta Lake Optimization



Delta tables are automatically compacted when file count exceeds threshold:Duckrun automatically:

- Compacts small files when file count exceeds threshold (default: 100)

```python- Vacuums old versions on overwrite

# Customize threshold- Cleans up metadata

con = duckrun.connect(

    "workspace/lakehouse.lakehouse/dbo",Customize compaction threshold:

    compaction_threshold=50  # compact after 50 files

)```python

```con = duckrun.connect(

    "workspace/lakehouse.lakehouse/dbo",

---    compaction_threshold=50  # compact after 50 files

)

## Complete Example```



```python## File Management API Reference

import duckrun

### `copy(local_folder, remote_folder, file_extensions=None, overwrite=False)`

# 1. Connect with SQL folder for pipelines

con = duckrun.connect("Analytics/Sales.lakehouse/dbo", sql_folder="./sql")Upload files from a local folder to OneLake Files section.



# 2. Upload raw data files**Parameters:**

con.copy("./raw_data", "staging", ['.csv', '.json'])- `local_folder` (str): Path to local folder containing files to upload

- `remote_folder` (str): **Required** target folder path in OneLake Files  

# 3. Run data pipeline- `file_extensions` (list, optional): Filter by file extensions (e.g., `['.csv', '.parquet']`)

pipeline = [- `overwrite` (bool, optional): Whether to overwrite existing files (default: False)

    # Python: Download from API

    ('fetch_api_data', ('https://api.example.com/sales', 'raw')),**Returns:** `True` if all files uploaded successfully, `False` otherwise

    

    # SQL: Clean and transform**Examples:**

    ('clean_sales', 'overwrite'),```python

    # Upload all files to a target folder

    # SQL: Aggregate with parameterscon.copy("./data", "processed_data")

    ('regional_summary', 'overwrite', {'min_amount': 1000}),

    # Upload only CSV and Parquet files

    # SQL: Append to history with schema evolution and partitioningcon.copy("./reports", "monthly_reports", ['.csv', '.parquet'])

    ('sales_history', 'append', {}, {

        'mergeSchema': 'true', # Upload with overwrite enabled

        'partitionBy': ['year', 'region']con.copy("./backup", "daily_backup", overwrite=True)

    })```

]

### `download(remote_folder="", local_folder="./downloaded_files", file_extensions=None, overwrite=False)`

success = con.run(pipeline)

Download files from OneLake Files section to a local folder.

# 4. Query and explore results

con.sql("""**Parameters:**

    SELECT region, SUM(total) as grand_total- `remote_folder` (str, optional): Source folder path in OneLake Files (default: root)

    FROM regional_summary- `local_folder` (str, optional): Local destination folder (default: "./downloaded_files")  

    GROUP BY region- `file_extensions` (list, optional): Filter by file extensions (e.g., `['.csv', '.json']`)

""").show()- `overwrite` (bool, optional): Whether to overwrite existing local files (default: False)



# 5. Create derived table**Returns:** `True` if all files downloaded successfully, `False` otherwise

con.sql("SELECT * FROM sales WHERE year = 2024").write \

   .mode("overwrite") \**Examples:**

   .partitionBy("month") \```python

   .saveAsTable("sales_2024")# Download all files from OneLake Files root

con.download()

# 6. Download processed reports

con.download("processed_reports", "./exports", ['.csv'])# Download from specific folder

con.download("processed_data", "./local_data")

# 7. Deploy semantic model

con.deploy(# Download only JSON files

    "https://raw.githubusercontent.com/user/repo/main/sales_model.bim",con.download("config", "./configs", ['.json'])

    dataset_name="Sales Analytics"```

)

**Important Notes:**

# 8. Enrich logs with lookup functions- Files are uploaded/downloaded to/from the **OneLake Files section**, not Delta Tables

logs = con.sql("""- The `remote_folder` parameter is **required** for uploads to prevent accidental uploads

    SELECT - Both methods default to `overwrite=False` for safety

        workspace_id,- Folder structure is preserved during upload/download operations

        get_workspace_name(workspace_id) as workspace,- Progress is reported with file names, sizes, and upload/download status

        lakehouse_id,

        get_lakehouse_name(workspace_id, lakehouse_id) as lakehouse,## Complete Example

        COUNT(*) as operations

    FROM audit_logs```python

    GROUP BY ALLimport duckrun

""").df()

# Connect (specify schema for best performance)

print(logs)con = duckrun.connect("Analytics/Sales.lakehouse/dbo", sql_folder="./sql")

```

# 1. Upload raw data files to OneLake Files

---con.copy("./raw_data", "raw_uploads", ['.csv', '.json'])



## Python Task Reference# 2. Pipeline with mixed tasks

pipeline = [

Create Python tasks in your `sql_folder`:    # Download raw data (Python)

    ('fetch_api_data', ('https://api.example.com/sales', 'raw')),

```python    

# sql_folder/fetch_api_data.py    # Clean and transform (SQL)

def fetch_api_data(url, output_path):    ('clean_sales', 'overwrite'),

    """    

    Download data from API.    # Aggregate by region (SQL with params)

        ('regional_summary', 'overwrite', {'min_amount': 1000}),

    Returns:    

        1 for success (pipeline continues)    # Append to history with schema evolution (SQL with Delta options)

        0 for failure (pipeline stops)    ('sales_history', 'append', {}, {'mergeSchema': 'true', 'partitionBy': ['year', 'region']})

    """]

    try:

        import requests# Run pipeline

        response = requests.get(url)success = con.run(pipeline)

        response.raise_for_status()

        # 3. Explore results using DuckDB

        # Save datacon.sql("SELECT * FROM regional_summary").show()

        with open(output_path, 'w') as f:

            f.write(response.text)# 4. Export to new Delta table

        con.sql("""

        return 1  # Success    SELECT region, SUM(total) as grand_total

    except Exception as e:    FROM regional_summary

        print(f"Error: {e}")    GROUP BY region

        return 0  # Failure - pipeline will stop""").write.mode("overwrite").saveAsTable("region_totals")

```

# 5. Download processed files for external systems

**Important:**con.download("processed_reports", "./exports", ['.csv'])

- Function name must match filename

- Return `1` for success, `0` for failure# 6. Deploy semantic model for Power BI

- Python tasks can use workspace/lakehouse IDs as parameterscon.deploy(

    "https://raw.githubusercontent.com/user/repo/main/sales_model.bim",

---    dataset_name="Sales Analytics"

)

## Requirements & Notes```



**Requirements:****This example demonstrates:**

- Lakehouse must have a schema (e.g., `dbo`, `sales`, `analytics`)- 📁 **File uploads** to OneLake Files section

- Azure authentication (Azure CLI, browser, or Fabric notebook environment)- 🔄 **Pipeline orchestration** with SQL and Python tasks  

- ⚡ **Fast data exploration** with DuckDB

**Important Notes:**- 💾 **Delta table creation** with Spark-style API

- ✅ Workspace names with spaces are fully supported- 🔀 **Schema evolution** and partitioning

- ✅ Files uploaded/downloaded to OneLake **Files** section (not Delta Tables)- 📤 **File downloads** from OneLake Files

- ✅ Pipeline stops on first failure (SQL errors or Python returning 0)- 📊 **Semantic model deployment** with DirectLake

- ⚠️ Uses older deltalake version for row size control (Power BI optimization)

- ⚠️ Scanning all schemas can be slow for large lakehouses## Schema Evolution & Partitioning Guide



**Authentication:**### When to Use Schema Evolution

- Fabric notebooks: Automatic using notebook credentials

- Local/VS Code: Azure CLI or interactive browser authenticationUse `mergeSchema: 'true'` when:

- Custom: Use Azure Identity credential chain- Adding new columns to existing tables

- Source data schema changes over time  

---- Working with evolving data pipelines

- Need backward compatibility

## Real-World Example

### When to Use Partitioning

For a complete production example, see [fabric_demo](https://github.com/djouallah/fabric_demo).

Use `partitionBy` when:

## License- Queries frequently filter by specific columns (dates, regions, categories)

- Tables are large and need performance optimization

MIT- Want to organize data logically for maintenance


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