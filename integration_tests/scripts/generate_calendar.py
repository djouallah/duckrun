"""Generate the calendar dimension as an external Delta table.

dim_calendar is no longer a dbt model; it's an external Delta table that the dbt project
reads through ``source('aemo', 'dim_calendar')``. Run this before ``dbt build`` / ``dbt test``
so the table exists when the source (and its tests) resolve.

Output:  $WAREHOUSE_PATH/sources/dim_calendar   (default WAREHOUSE_PATH=/tmp)
Auth:    on abfss:///az:// paths, $ONELAKE_TOKEN is forwarded as a deltalake bearer_token
         (matches profiles.yml storage_options and engine.write_delta).
"""

import os
from datetime import date, timedelta

import pyarrow as pa
from deltalake import write_deltalake

START = date(2018, 4, 1)
END = date(2026, 12, 31)


def build_table() -> pa.Table:
    dates, years, months = [], [], []
    d = START
    while d <= END:
        dates.append(d)
        years.append(d.year)
        months.append(d.month)
        d += timedelta(days=1)
    return pa.table(
        {
            "date": pa.array(dates, type=pa.date32()),
            "year": pa.array(years, type=pa.int32()),
            "month": pa.array(months, type=pa.int32()),
        }
    )


def main() -> None:
    root = os.environ.get("WAREHOUSE_PATH", "/tmp").rstrip("/")
    path = f"{root}/sources/dim_calendar"

    storage_options = None
    token = os.environ.get("ONELAKE_TOKEN")
    if path.startswith(("abfss://", "az://")) and token:
        storage_options = {"bearer_token": token}

    table = build_table()
    write_deltalake(path, table, mode="overwrite", storage_options=storage_options)
    print(f"Wrote dim_calendar ({table.num_rows} rows) to {path}")


if __name__ == "__main__":
    main()
