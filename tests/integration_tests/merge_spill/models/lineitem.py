def model(dbt, session):
    # Generate the big TPCH lineitem with tpchgen-cli and hand it back as a STREAMED relation, so
    # duckrun writes it to Delta over the Arrow C-stream (no full in-memory load). Data generation
    # lives in the DAG — a dbt python model — not the test runner. Everything downstream ref()s this.
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
    pattern = os.path.join(gen, "lineitem", "*.parquet").replace(os.sep, "/")
    return session.sql(f"select * from read_parquet('{pattern}')")
