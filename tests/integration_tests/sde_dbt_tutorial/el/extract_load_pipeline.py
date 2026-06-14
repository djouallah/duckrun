"""Phase-1 EL: land the raw application CSVs as Delta tables.

Upstream (josephmachado/simple_dbt_project) wrote raw tables into a DuckDB file. On duckrun the
warehouse is Delta, and sources are resolved as delta_scan views, so raw lives as Delta tables at
``<DUCKRUN_WAREHOUSE>/raw/<name>``. Columns are read as VARCHAR so the schema is stable across
re-runs and later source updates; the bronze models do the typing (``::timestamp`` etc.).

This is just the EL half of the same dbt+Delta stack the models run on, so it uses duckrun's own
connection API (read CSV -> Spark-style ``.write.saveAsTable``) rather than poking duckdb/deltalake
directly — same pyarrow-free write path the adapter uses.
"""
import os

import duckrun

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ROOT = os.environ.get("DUCKRUN_WAREHOUSE", os.path.join(HERE, "warehouse"))

FILES = {
    "raw_customer": "customer.csv",
    "raw_orders": "orders.csv",
    "raw_state": "state.csv",
    "raw_clickstream": "clickstream.csv",
}


def _storage_options():
    # OneLake (abfss://) authenticates with a storage bearer token; a local root needs none.
    token = os.environ.get("ONELAKE_TOKEN") or os.environ.get("AZURE_STORAGE_TOKEN")
    return {"bearer_token": token} if token else None


def main():
    con = duckrun.connect(ROOT, storage_options=_storage_options())
    for table, csv_name in FILES.items():
        path = os.path.join(HERE, "raw_data", csv_name).replace("\\", "/")
        df = con.read.option("all_varchar", True).option("header", True).csv(path)
        rows = df.count()
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"raw.{table}")
        print(f"{table}: wrote {rows} rows -> raw.{table}")


if __name__ == "__main__":
    main()
