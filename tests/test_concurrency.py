"""
Delta Lake OCC demonstration (duckrun / delta-rs engine).

A common misconception: reading a table and then writing gives you read-to-write isolation.
It does not. A MERGE's conflict check is bound to the snapshot at the *merge transaction's
start* (HEAD-at-merge-start), not to the version an earlier application read observed.

So the sequence below commits with no error — and it is correct, working-as-designed OCC:

  1. Read the target -> batch_001 is present, so the work-list SKIPS it.
  2. A concurrent transaction DELETEs batch_001 between the read and the merge.
  3. MERGE ... WHEN NOT MATCHED commits successfully. batch_001 is simply gone: the merge
     never re-adds it (the work-list skipped it), and OCC raises nothing because the only
     window it checks — merge-start -> commit — had no conflicting change.

Reference notebook: occ.ipynb (Fabric_Notebooks_Demo). The Spark equivalent lives in
test_concurrency_spark.py and reaches the same conclusion.
"""
import pyarrow as pa

from deltalake import DeltaTable

from dbt.adapters.duckrun import engine

# Five batches of 20 ids each; filename is the provenance column we dedupe on.
ALL_BATCHES = {
    f"batch_{b:03d}.csv": range((b - 1) * 20 + 1, b * 20 + 1)
    for b in range(1, 6)
}


def _batch_table(filenames):
    """Build a reusable Arrow table (id, value, filename) for the given batch filenames."""
    ids, values, names = [], [], []
    for fname in filenames:
        for i in ALL_BATCHES[fname]:
            ids.append(i)
            values.append(i * 10)
            names.append(fname)
    return pa.table({
        "id": pa.array(ids, pa.int64()),
        "value": pa.array(values, pa.int64()),
        "filename": pa.array(names),
    })


def _filenames(path):
    return set(DeltaTable(path).to_pyarrow_table().column("filename").to_pylist())


def test_merge_conflict_check_is_bound_to_merge_start_not_read(tmp_path):
    """The merge commits and batch_001 is silently lost — correct OCC, commonly misread."""
    path = str(tmp_path / "target")

    # Bootstrap: batch_001 already ingested.
    engine.write_delta(path, _batch_table(["batch_001.csv"]), "overwrite")

    # Ordinary read of the target (what every pipeline does). The work-list is derived from
    # this observed state: batches NOT already seen -> 002..005. batch_001 is skipped.
    seen = _filenames(path)
    assert seen == {"batch_001.csv"}
    to_ingest = _batch_table([f for f in ALL_BATCHES if f not in seen])

    # A concurrent transaction deletes batch_001 between our read and our merge.
    DeltaTable(path).delete("filename = 'batch_001.csv'")
    assert _filenames(path) == set()  # B is now empty

    # Our merge, built from the earlier read, commits with NO concurrency error: OCC only
    # checks HEAD-at-merge-start (the post-delete, empty table) -> commit, where nothing
    # conflicts. The work-list never carried batch_001, so it is not re-added.
    engine.merge_delta(path, to_ingest, "filename")

    final = _filenames(path)
    assert "batch_001.csv" not in final                       # silently lost — by design
    assert {f for f in ALL_BATCHES if f != "batch_001.csv"} == final
