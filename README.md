
<img src="duckrun.png" width="400" alt="Duckrun">

Simple  task runner for Microsoft Fabric Python notebook, powered by DuckDB and Delta_rs.


## Known Limitation

Support only Lakehouse with schema, Workspace and lakehouse names should not contains space

## Installation

```bash
pip install duckrun
```



## Quick Start

```python
import duckrun

# Connect to your Fabric lakehouse (using `con` pattern)
con = duckrun.connect(
    workspace="my_workspace",
    lakehouse_name="my_lakehouse", 
    schema="dbo",
    sql_folder="./sql"  # optional: folder containing your .sql and .py files (only needed for pipeline tasks)
)

# Define your pipeline
pipeline = [
    ('load_data', (url, path)),           # Python task
    ('clean_data', 'overwrite'),          # SQL task  
    ('aggregate', 'append')               # SQL task
]

# Run it
con.run(pipeline)
```

Note: the `sql/` folder is optional — if all you want to do is explore data with SQL (for example by calling `con.sql(...)`), you don't need to provide a `sql_folder`.

## Early Exit

In a pipeline run, if a task fails, the pipeline will stop without running the subsequent tasks.

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
con.sql("SELECT * FROM my_table LIMIT 10").show()

# Get as DataFrame
df = con.sql("SELECT COUNT(*) FROM sales").df()
```

Explanation: DuckDB is connected to the lakehouse through `con`, so it is aware of the tables in that lakehouse (including tables created by your pipelines). That means you can query those tables directly with `con.sql(...)` just like any other DuckDB query. If you don't provide a `sql_folder`, you can still use `con.sql(...)` to explore existing tables.



## Remote SQL Files

You can load SQL/Python files from a URL:

```python
con = duckrun.connect(
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