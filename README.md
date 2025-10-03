# 🦆 Duckrun

Simple  task runner for Microsoft Fabric Python notebook, powered by DuckDB and Delta_rs.

## Installation

```bash
pip install duckrun
```



## Quick Start

```python
import duckrun as dr

# Connect to your Fabric lakehouse
lakehouse = dr.connect(
    workspace="my_workspace",
    lakehouse_name="my_lakehouse", 
    schema="dbo",
    sql_folder="./sql"  # folder containing your .sql and .py files
)

# Define your pipeline
pipeline = [
    ('load_data', (url, path)),           # Python task
    ('clean_data', 'overwrite'),          # SQL task  
    ('aggregate', 'append')               # SQL task
]

# Run it
lakehouse.run(pipeline)
```

## How It Works

Duckrun runs two types of tasks:

### 1. Python Tasks
Format: `('function_name', (arg1, arg2, ...))`

Create a file `sql_folder/function_name.py` with a function matching the name:

```python
# sql_folder/load_data.py
def load_data(url, path):
    # your code here
    # IMPORTANT: Must return 1 for success, 0 for failure
    return 1
```

### 2. SQL Tasks  
Format: `('table_name', 'mode')` or `('table_name', 'mode', {params})`

Create a file `sql_folder/table_name.sql`:

```sql
-- sql_folder/clean_data.sql
SELECT 
    id,
    TRIM(name) as name,
    date
FROM raw_data
WHERE date >= '2024-01-01'
```

**Modes:**
- `overwrite` - Replace table completely
- `append` - Add to existing table
- `ignore` - Create only if doesn't exist

## Task Files

The `sql_folder` can contain a mixture of both `.sql` and `.py` files. This allows you to combine SQL transformations and Python logic in your pipelines.

### SQL Files
Your SQL files automatically have access to:
- `$ws` - workspace name
- `$lh` - lakehouse name
- `$schema` - schema name

Pass custom parameters:

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

## Table Name Convention

Use `__` to create variants of the same table:

```python
pipeline = [
    ('sales__initial', 'overwrite'),    # writes to 'sales' table
    ('sales__incremental', 'append'),   # appends to 'sales' table
]
```

Both write to the same `sales` table, but use different SQL files.

## Query Data

```python
# Run queries
lakehouse.sql("SELECT * FROM my_table LIMIT 10").show()

# Get as DataFrame
df = lakehouse.sql("SELECT COUNT(*) FROM sales").df()
```

## Real-World Example

```python
import duckrun as dr

lakehouse = dr.connect(
    workspace="Analytics",
    lakehouse_name="Sales", 
    schema="dbo",
    sql_folder="./etl"
)

# Daily pipeline
daily = [
    ('download_files', (api_url, local_path)),
    ('staging_orders', 'overwrite', {'run_date': '2024-06-01'}),
    ('staging_customers', 'overwrite', {'run_date': '2024-06-01'}),
    ('fact_sales', 'append'),
    ('dim_customer', 'overwrite')
]

lakehouse.run(daily)

# Check results
lakehouse.sql("SELECT COUNT(*) FROM fact_sales").show()
```

## Remote SQL Files

You can load SQL/Python files from a URL:

```python
lakehouse = dr.connect(
    workspace="Analytics",
    lakehouse_name="Sales", 
    schema="dbo",
    sql_folder="https://raw.githubusercontent.com/user/repo/main/sql"
)
```

## Real-Life Usage

For a complete, production-style example, see [fabric_demo](https://github.com/djouallah/fabric_demo).

## License

MIT