"""Phase-1 EL: land the raw application CSVs as Delta tables.

Upstream (josephmachado/simple_dbt_project) wrote raw tables into a DuckDB file. On duckrun the
warehouse is Delta, and sources are resolved as delta_scan views, so raw lives as Delta tables at
``<DUCKRUN_WAREHOUSE>/raw/<name>``. Columns are read as VARCHAR so the schema is stable across the
phase-2 append (el/load_new_data.py); the bronze models do the typing (``::timestamp`` etc.).
"""
import os

import duckdb
from deltalake import write_deltalake

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ROOT = os.environ.get("DUCKRUN_WAREHOUSE", os.path.join(HERE, "warehouse"))

FILES = {
    "raw_customer": "customer.csv",
    "raw_orders": "orders.csv",
    "raw_state": "state.csv",
    "raw_clickstream": "clickstream.csv",
}


def _read_csv(con, csv_name):
    path = os.path.join(HERE, "raw_data", csv_name).replace("\\", "/")
    return con.execute(
        f"select * from read_csv_auto('{path}', all_varchar=true, header=true)"
    ).to_arrow_table()


def main():
    con = duckdb.connect()
    for table, csv_name in FILES.items():
        tbl = _read_csv(con, csv_name)
        dest = os.path.join(ROOT, "raw", table)
        write_deltalake(dest, tbl, mode="overwrite", schema_mode="overwrite")
        print(f"{table}: wrote {tbl.num_rows} rows -> {dest}")
    con.close()


if __name__ == "__main__":
    main()
