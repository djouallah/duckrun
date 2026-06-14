"""Phase-2 EL: append the incremental CSVs onto the raw Delta tables.

This reproduces the upstream change semantics that the SCD2 snapshot and the incremental model react
to on the second ``dbt build``:

  * ``customer_new.csv`` is *appended* (not replaced), so the raw table then holds duplicate
    ``customer_id``s for the updated customers (82, 5, 12, 60, 33) plus brand-new ones (101, 102).
    The timestamp snapshot closes the old version and opens a new one -> ``customer_id=82`` gets a
    second row.
  * ``clickstream_incremental.csv`` is 10 brand-new ``event_id``s (101-110) -> the merge-incremental
    ``fct_clickstream`` grows 100 -> 110.
"""
import os

import duckdb
from deltalake import write_deltalake

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ROOT = os.environ.get("DUCKRUN_WAREHOUSE", os.path.join(HERE, "warehouse"))

FILES = {
    "raw_clickstream": "clickstream_incremental.csv",
    "raw_customer": "customer_new.csv",
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
        write_deltalake(dest, tbl, mode="append")
        print(f"{table}: appended {tbl.num_rows} rows -> {dest}")
    con.close()


if __name__ == "__main__":
    main()
