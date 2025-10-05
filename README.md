<img src="https://raw.githubusercontent.com/djouallah/duckrun/main/duckrun.png" width="400" alt="Duckrun">

Simple task runner for Microsoft Fabric Python notebooks, powered by DuckDB and Delta Lake.

## Important Notes

**Requirements:**
- Lakehouse must have a schema (e.g., `dbo`, `sales`, `analytics`)
- Workspace and lakehouse names cannot contain spaces

**Why no spaces?** Duckrun uses simple name-based paths instead of GUIDs. This keeps the code clean and readable, which is perfect for data engineering workspaces where naming conventions are already well-established. Just use underscores or hyphens instead: `my_workspace` or `my-lakehouse`.

## Installation

```bash
pip install duckrun
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

## Two Ways to Use Duckrun

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

### 2. Pipeline Orchestration

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

## Complete Example

```python
import duckrun

# Connect (specify schema for best performance)
con = duckrun.connect("Analytics/Sales.lakehouse/dbo", sql_folder="./sql")

# Pipeline with mixed tasks
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

# Run
success = con.run(pipeline)

# Explore results
con.sql("SELECT * FROM regional_summary").show()

# Export to new table
con.sql("""
    SELECT region, SUM(total) as grand_total
    FROM regional_summary
    GROUP BY region
""").write.mode("overwrite").saveAsTable("region_totals")
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