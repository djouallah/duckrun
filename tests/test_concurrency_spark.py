"""
Delta Lake OCC demonstration (Spark engine) — mirror of test_concurrency.py.

Same point, proven on Spark: a MERGE's conflict check is bound to HEAD-at-merge-start, not to
the version an earlier read observed. Spark cannot pin the merge to your read snapshot, so the
read -> concurrent-DELETE -> MERGE sequence commits with no error and batch_001 is lost. This
is correct OCC behaviour, commonly misunderstood.

Reference notebook: occ_spark.ipynb (Fabric_Notebooks_Demo).

Heavy deps (pyspark + delta-spark + a JVM) — the whole module skips when they are absent.
"""
import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

from delta import configure_spark_with_delta_pip  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    warehouse = tmp_path_factory.mktemp("spark-warehouse")
    builder = (
        SparkSession.builder
        .appName("duckrun-occ-test")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("spark.sql.shuffle.partitions", "2")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()


def test_spark_merge_conflict_check_is_bound_to_merge_start_not_read(spark):
    """The merge commits and batch_001 is silently lost — correct OCC, commonly misread."""
    spark.sql("DROP TABLE IF EXISTS occ_spark_target")

    # All 5 batches as a source view: 20 ids each, filename = provenance (no files needed).
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW all_batches AS
        SELECT id,
               id * 10 AS value,
               concat('batch_', lpad(cast(ceil(id / 20.0) AS int), 3, '0'), '.csv') AS filename
        FROM range(1, 101)
    """)

    # Bootstrap target with batch_001 already ingested.
    spark.sql("""
        CREATE TABLE occ_spark_target USING DELTA AS
        SELECT * FROM all_batches WHERE filename = 'batch_001.csv'
    """)

    # Ordinary read of the target. The work-list (NOT IN seen) IS the state we observed:
    # batches 002..005; batch_001 is treated as already ingested and skipped.
    seen = [r.filename for r in
            spark.sql("SELECT DISTINCT filename FROM occ_spark_target").collect()]
    assert seen == ["batch_001.csv"]
    seen_sql = ", ".join(f"'{f}'" for f in seen)
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW to_ingest AS
        SELECT * FROM all_batches WHERE filename NOT IN ({seen_sql})
    """)

    # A concurrent transaction deletes batch_001 between our read and our merge.
    spark.sql("DELETE FROM occ_spark_target WHERE filename = 'batch_001.csv'")

    # The merge commits with NO concurrency error: Spark fixes one snapshot at transaction
    # start (HEAD = post-delete) for both its scan and its conflict check.
    spark.sql("""
        MERGE INTO occ_spark_target AS t
        USING to_ingest AS s
        ON t.filename = s.filename
        WHEN NOT MATCHED THEN INSERT *
    """)

    final = {r.filename for r in
             spark.sql("SELECT DISTINCT filename FROM occ_spark_target").collect()}
    assert "batch_001.csv" not in final                       # silently lost — by design
    assert final == {f"batch_{b:03d}.csv" for b in range(2, 6)}
