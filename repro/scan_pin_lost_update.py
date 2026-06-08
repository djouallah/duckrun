"""
Standalone reproduction: duckrun's merge source scan and merge conflict-check are NOT pinned
to a single Delta snapshot, so a concurrent commit landing between them silently loses a write.

This is NOT a CI test (it lives outside `tests/` on purpose). Run it directly:

    python repro/scan_pin_lost_update.py

Background
----------
A duckrun incremental merge reads the same Delta table twice, with no shared snapshot pin:

  1. SOURCE SCAN  - the model's source rows come from DuckDB. For an ordinary incremental model
     the source derives from `{{ this }}`, which duckrun registers as
     `select * from delta_scan('<path>')`. That scan sees snapshot A.
  2. MERGE TARGET - `engine.merge_delta` calls `_delta_table(path)` fresh and `dt.merge(...)`;
     delta-rs binds its target read and conflict check to HEAD-at-merge-start = snapshot B.

Spark pins one snapshot for the whole merge (scan and conflict check see the same version).
duckrun's scan is lazy, so "in practice" A and B line up - but that is a practical consequence,
not a guarantee. Under heavy concurrent writes a commit can land in the A->B window.

The monkeypatch below is a TIMING DEVICE only. It forces the A->B window open deterministically
so the race is reproducible every run; it does not manufacture a condition that cannot occur in
the wild - it just stops relying on losing a real, tiny race. Remove the patch and the window
closes and the committed value survives, confirming the corruption depends purely on A != B.

Scope: this affects the key-based merge/insert path only. `append` and `overwrite` do no OCC
read-snapshot conflict check, so they have no A<->B snapshot to diverge.

Fix once upstream lands: https://github.com/duckdb/duckdb-delta/pull/312 adds version pinning to
`delta_scan`. Then duckrun can read HEAD once (DeltaTable(path).version()), scan that exact
version in DuckDB (delta_scan(..., version=N)) and load the merge target at the same
DeltaTable(path, version=N) - source scan and conflict check share one pinned snapshot.
"""
import sys
import tempfile
from pathlib import Path

import duckdb
import pyarrow as pa
from deltalake import DeltaTable

from dbt.adapters.duckrun import engine


def _read_value(path: str, key: int) -> int:
    tbl = DeltaTable(path).to_pyarrow_table()
    rows = {r["id"]: r["value"] for r in tbl.to_pylist()}
    return rows[key]


def reproduce(path: str) -> bool:
    """Returns True if the silent lost update is reproduced."""
    # --- Snapshot A: bootstrap the target with id=1, value=10 -----------------------------
    engine.write_delta(
        path,
        pa.table({"id": pa.array([1], pa.int64()), "value": pa.array([10], pa.int64())}),
        "overwrite",
    )

    # --- Build the merge source from a real DuckDB delta_scan of the target AT SNAPSHOT A.
    # An ordinary incremental model derives its source from `{{ this }}` exactly this way.
    # Materialising to Arrow now pins the source to A: an unchanged passthrough of id=1
    # (value=10) plus a new id=2 (value=20).
    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta;")
    scan_path = Path(path).as_posix()
    source = con.execute(
        f"""
        SELECT id, value FROM delta_scan('{scan_path}')
        UNION ALL
        SELECT 2 AS id, 20 AS value
        """
    ).fetch_arrow_table()  # <- pinned to snapshot A here

    # --- Force the A->B window open: on the first _delta_table() call inside merge_delta, land a
    # legitimate concurrent commit FIRST (HEAD -> B), THEN construct the table the merge reads.
    # So the merge's target/conflict snapshot is B (newer), while the source is pinned to A
    # (older). This is the dangerous direction: nothing commits AFTER the merge starts, so OCC
    # raises nothing - yet the merge applies stale-A source rows over the committed-B value.
    # (The opposite ordering - merge reads A, HEAD moves to B - is the SAFE case: OCC fires.)
    real_delta_table = engine._delta_table
    injected = {"done": False}

    def patched_delta_table(p, storage_options):
        if not injected["done"]:
            injected["done"] = True
            # Snapshot B: a concurrent writer commits value=999 for id=1.
            real_delta_table(p, storage_options).update(
                predicate="id = 1", updates={"value": "999"}
            )
        return real_delta_table(p, storage_options)  # reads B

    engine._delta_table = patched_delta_table
    try:
        # Default upsert (when_matched_update_all). The merge binds to B (where id=1 == 999),
        # but its source rows were computed at A (where id=1 == 10).
        engine.merge_delta(path, source, "id")
    finally:
        engine._delta_table = real_delta_table

    final = _read_value(path, 1)
    print(f"committed concurrent value (snapshot B): 999")
    print(f"source value (snapshot A):              10")
    print(f"final value after merge:                {final}")
    if final == 10:
        print("\nBUG REPRODUCED: committed value 999 was silently lost, stale 10 won "
              "(no conflict error raised).")
        return True
    print("\nNot reproduced: the committed value survived.")
    return False


def main() -> int:
    with tempfile.TemporaryDirectory() as tmp:
        path = str(Path(tmp) / "target")
        reproduced = reproduce(path)
    return 1 if reproduced else 0


if __name__ == "__main__":
    sys.exit(main())
