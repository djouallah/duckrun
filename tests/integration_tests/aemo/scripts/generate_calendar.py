"""Generate the calendar dimension as an external Delta table.

dim_calendar is no longer a dbt model; it's an external Delta table that the dbt project
reads through ``source('aemo', 'dim_calendar')``. Run this before ``dbt build`` / ``dbt test``
so the table exists when the source (and its tests) resolve.

Output:  $WAREHOUSE_PATH/sources/dim_calendar   (default WAREHOUSE_PATH=/tmp)
Auth:    on abfss:///az:// paths, duckrun self-acquires the OneLake bearer token
         (auth.get_onelake_token — GitHub OIDC / azure-identity), matching how the adapter writes.

DuckDB builds the rows (the same generate_series the old dim_calendar.sql model used) and the
relation is handed straight to write_deltalake over Arrow's C-stream interface — no pyarrow
import, mirroring how the adapter writes every Delta table.
"""

import os

import duckdb
from deltalake import write_deltalake

CALENDAR_SQL = """
SELECT
  CAST(date AS DATE)                       AS date,
  CAST(EXTRACT(year FROM date) AS INT)     AS year,
  CAST(EXTRACT(month FROM date) AS INT)    AS month
FROM (
  SELECT unnest(generate_series(
    CAST('2018-04-01' AS DATE),
    CAST('2026-12-31' AS DATE),
    INTERVAL 1 DAY
  )) AS date
)
"""


def main() -> None:
    # WAREHOUSE_PATH may be the `<ws>/<lakehouse>` OneLake shorthand; expand before appending the
    # table sub-path (delta-rs itself only understands the full URL).
    from dbt.adapters.duckrun.remote import expand_onelake_shorthand

    root = expand_onelake_shorthand(os.environ.get("WAREHOUSE_PATH", "/tmp")).rstrip("/")
    path = f"{root}/sources/dim_calendar"

    storage_options = None
    if path.startswith(("abfss://", "az://")):
        from duckrun import auth
        storage_options = {"bearer_token": auth.get_onelake_token()}

    con = duckdb.connect()
    n = con.sql(f"SELECT count(*) FROM ({CALENDAR_SQL})").fetchone()[0]
    rel = con.sql(CALENDAR_SQL)
    write_deltalake(path, rel, mode="overwrite", storage_options=storage_options)
    print(f"Wrote dim_calendar ({n} rows) to {path}")


if __name__ == "__main__":
    main()
