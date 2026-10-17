"""Smoke test for duckrun as `pip install --pre "duckrun[dbt]"` resolves it today.

A pre-release install pulls the newest duckdb 2.0 dev build, and DuckDB 2.0 is reshaping its Python
result API (fetchone & co). This proves duckrun still works against whatever duckdb
that resolves to: the connect() DML surface and an incremental dbt run.

Run by the `pre-install` job in .github/workflows/cores.yml (a release gate) from OUTSIDE the
checkout, so `import duckrun` can only be the installed wheel. Plain script: exits non-zero on the first failed check.
"""
import tempfile
from importlib.metadata import version
from pathlib import Path

import duckdb
import duckrun

assert "site-packages" in Path(duckrun.__file__).parts, f"not the installed duckrun: {duckrun.__file__}"
print(f"duckrun {version('duckrun')} / duckdb {duckdb.__version__}")


# --- connect(): every DML statement, read back ---------------------------------------------------
# conn.sql() returns a DuckDB relation, and duckdb 2.0 relations have no fetchone()/fetchmany() (a
# relation keeps no result between calls) — fetchall() / limit(n).fetchall() is the portable read.
conn = duckrun.connect(tempfile.mkdtemp(), read_only=False)
conn.sql("CREATE TABLE t AS SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) v(id, name)")
conn.sql("INSERT INTO t VALUES (4, 'd')")
conn.sql("UPDATE t SET name = 'B' WHERE id = 2")
conn.sql("DELETE FROM t WHERE id = 3")
conn.sql("MERGE INTO t USING (VALUES (1, 'A'), (9, 'z')) s(id, name) ON target.id = source.id "
         "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")

expected = [(1, "A"), (2, "B"), (4, "d"), (9, "z")]
assert conn.sql("SELECT id, name FROM t ORDER BY id").fetchall() == expected
assert conn.sql("SELECT count(*) FROM t").fetchall() == [(4,)]
assert conn.sql("SELECT id, name FROM t ORDER BY id").limit(2).fetchall() == expected[:2]
assert conn.sql("SELECT id FROM t WHERE id = 42").fetchall() == []
print("connect() OK")


# --- dbt: a table model + an incremental merge model, run twice -------------------------------
from dbt.cli.main import dbtRunner  # noqa: E402  (after the connect() checks: those need no dbt)

proj = Path(tempfile.mkdtemp())
root = Path(tempfile.mkdtemp()).as_posix()
(proj / "models").mkdir()
(proj / "dbt_project.yml").write_text(
    "name: smoke\nversion: '1.0'\nconfig-version: 2\nprofile: smoke\nmodel-paths: [models]\n",
    encoding="utf-8")
(proj / "profiles.yml").write_text(
    f"smoke:\n  target: dev\n  outputs:\n    dev:\n      type: duckrun\n      root_path: \"{root}\"\n",
    encoding="utf-8")
(proj / "models" / "src.sql").write_text(
    "{{ config(materialized='table') }}\n"
    "select range as id, 'v' || range as val from range(100)\n", encoding="utf-8")
(proj / "models" / "inc.sql").write_text(
    "{{ config(materialized='incremental', unique_key='id') }}\n"
    "select id, val from {{ ref('src') }}\n"
    "{% if is_incremental() %} where id >= 50 {% endif %}\n", encoding="utf-8")

for run in (1, 2):  # run 2 takes the is_incremental() merge path
    res = dbtRunner().invoke(["run", "--project-dir", str(proj), "--profiles-dir", str(proj)])
    assert res.success, f"dbt run {run} failed: {res.exception}"

wh = duckrun.connect(root, schema="main")
assert wh.sql("SELECT count(*), count(DISTINCT id) FROM src").fetchall() == [(100, 100)]
assert wh.sql("SELECT count(*), count(DISTINCT id) FROM inc").fetchall() == [(100, 100)]
assert wh.sql("SELECT val FROM inc WHERE id = 99").fetchall() == [("v99",)]
print("dbt OK")

print("pre-install smoke OK")
