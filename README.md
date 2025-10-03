# 🦆 Duckrun

Lakehouse task runner powered by DuckDB for Microsoft Fabric.

## Features

- 🦆 **DuckDB-powered**: Fast in-memory processing
- 📦 **Delta Lake**: Native Delta table support
- 🔄 **Simple API**: Clean tuple-based pipeline definition
- 🎯 **Fabric-native**: Built for Microsoft Fabric lakehouses
- 🐍 **Python + SQL**: Mix Python and SQL tasks seamlessly

## Installation
```bash
pip install duckrun

from duckrun import Duckrun

# Connect to your lakehouse
dr = Duckrun.connect(
    workspace="your_workspace",
    lakehouse_name="your_lakehouse",
    schema="dbo",
    sql_folder="./sql"
)

# Define pipeline
pipeline = [
    ('download', (urls, paths, depth)),
    ('staging', 'overwrite', {'run_date': '2024-06-01'}),
    ('transform', 'append'),
    ('fact_sales', 'append')
]

# Run it
dr.run(pipeline)

# Query directly
dr.sql("SELECT * FROM staging").show()