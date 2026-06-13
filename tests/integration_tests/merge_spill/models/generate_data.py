def model(dbt, session):
    # Side-effect-only DAG node: generate the big TPCH lineitem as PARQUET with tpchgen-cli into
    # MERGE_SPILL_GEN, which the `tpch.lineitem` source (sources.yml) then scans as a streaming view.
    #
    # We deliberately do NOT return the data. A dbt python model is staged as
    # `CREATE TABLE <model> AS SELECT * FROM <return value>` (dbt-duckdb's py_write_table), which
    # fully evaluates the returned relation — so returning the ~120M-row read_parquet relation would
    # collect every row into DuckDB's buffer before a single Delta byte is written, and OOM-kill the
    # runner. (That was the old lineitem.py bug.) Generation and the Delta write are different
    # concerns: this node only generates parquet and returns a tiny marker; the downstream SQL seed
    # streams straight from the parquet source (a lazy view), which never materializes in RAM.
    dbt.config(materialized="table")

    import os
    import shutil
    import subprocess

    sf = os.environ.get("MERGE_SPILL_SF", "1")
    gen = os.environ["MERGE_SPILL_GEN"]  # scratch dir the runner owns (cleaned with --dir)
    shutil.rmtree(gen, ignore_errors=True)
    os.makedirs(gen, exist_ok=True)
    parts = max(1, round(float(sf) / 2))
    subprocess.run(
        ["tpchgen-cli", "-s", str(sf), "--tables", "lineitem",
         "--parts", str(parts), "--format", "parquet", "--output-dir", gen],
        check=True,
    )
    # Tiny marker relation only — the real data lives in the parquet the source reads.
    return session.sql("select 1 as generated")
