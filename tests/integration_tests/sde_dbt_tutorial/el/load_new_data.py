"""Phase-2 EL: append the incremental CSVs onto the raw Delta tables.

This reproduces the upstream change semantics that the SCD2 snapshot and the incremental model react
to on the second ``dbt build``:

  * ``customer_new.csv`` is *appended* (not replaced), so the raw table then holds duplicate
    ``customer_id``s for the updated customers (82, 5, 12, 60, 33) plus brand-new ones (101, 102).
    The timestamp snapshot closes the old version and opens a new one -> ``customer_id=82`` gets a
    second row.
  * ``clickstream_incremental.csv`` is 10 brand-new ``event_id``s (101-110) -> the merge-incremental
    ``fct_clickstream`` grows 100 -> 110.

Uses duckrun's connection API (read CSV -> Spark-style ``.write.mode("append").saveAsTable``) rather
than poking duckdb/deltalake directly, mirroring extract_load_pipeline.py.
"""
import os

import duckrun

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ROOT = os.environ.get("DUCKRUN_WAREHOUSE", os.path.join(HERE, "warehouse"))

FILES = {
    "raw_clickstream": "clickstream_incremental.csv",
    "raw_customer": "customer_new.csv",
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
        df.write.mode("append").saveAsTable(f"raw.{table}")
        print(f"{table}: appended {rows} rows -> raw.{table}")


if __name__ == "__main__":
    main()
