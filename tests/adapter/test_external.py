"""`materialized='external'` through a REAL `dbt run`.

duckrun ships dbt-duckdb's external materialization (macros/materializations/external.sql) so a
project can export a model as a plain parquet/csv/json file instead of a Delta table. Nothing about
that path is duckrun's own code — it is upstream's macro plus upstream's adapter helpers — so what
needs pinning is the *interaction* with duckrun:

  * the statements it emits (`create table … as (select …)`, `insert … values`, `copy … to`,
    `create view`, `drop table`) must all stay NATIVE DuckDB. duckrun rewrites raw DML into
    delta_rs calls at the cursor (delta_dml.py); if any of these were intercepted, an external
    model would quietly write Delta instead of the file it promised.
  * the model relation is a view over the file, and duckrun's disk discovery only rebuilds Delta
    tables — so a *later* run that doesn't rebuild the external model needs upstream's
    `register_upstream_external_models()` on-run-start hook to see it.
  * external and Delta models have to coexist in one run and `ref()` each other.
"""
import os
import subprocess
import sys
from pathlib import Path

import duckdb
import duckrun
import pytest

from dbt.cli.main import dbtRunner


def _project(tmp_path, models, on_run_start=None):
    """Write a dbt project whose models are `{name: sql}`, on a fresh local lakehouse root.

    Returns (project_dir, root_path, external_root). `root_path` is where Delta tables land,
    `external_root` where external models write their files — kept apart so a stray Delta write
    under an external-only project is obvious.
    """
    proj = tmp_path / "proj"
    (proj / "models").mkdir(parents=True)
    (proj / "dbt_project.yml").write_text(
        "name: ext\nversion: '1.0'\nconfig-version: 2\nprofile: ext\n"
        "model-paths: [models]\n"
        + (f'on-run-start: "{on_run_start}"\n' if on_run_start else ""),
        encoding="utf-8")
    (proj / "profiles.yml").write_text(
        "ext:\n  target: dev\n  outputs:\n    dev:\n"
        "      type: duckrun\n"
        "      root_path: \"{{ env_var('EXT_ROOT_PATH') }}\"\n"
        "      external_root: \"{{ env_var('EXT_EXTERNAL_ROOT') }}\"\n",
        encoding="utf-8")
    for name, sql in models.items():
        suffix = ".py" if name.endswith("_py") else ".sql"
        (proj / "models" / f"{name}{suffix}").write_text(sql, encoding="utf-8")

    root = (tmp_path / "wh").as_posix()
    exports = (tmp_path / "exports").as_posix()
    Path(exports).mkdir()
    os.environ["EXT_ROOT_PATH"] = root
    os.environ["EXT_EXTERNAL_ROOT"] = exports
    return proj, root, exports


def _dbt(proj: Path, *args: str):
    return dbtRunner().invoke([*args, "--project-dir", str(proj), "--profiles-dir", str(proj)])


def _delta_logs(root):
    """Every Delta table under the lakehouse root — a table exists iff it has a _delta_log."""
    return sorted(p.parent.name for p in Path(root).rglob("_delta_log"))


def test_parquet_default_location_and_delta_downstream(tmp_path):
    """The headline path: an external model writes `<external_root>/<identifier>.parquet`, and a
    Delta `table` model in the SAME run reads it through ref() — the two materializations coexist
    and the export is a real parquet file, not a Delta table."""
    proj, root, exports = _project(tmp_path, {
        "ext_orders": "{{ config(materialized='external') }}\n"
                      "select 1 as id, 'a' as name union all select 2, 'b'",
        "mart_orders": "{{ config(materialized='table') }}\n"
                       "select id, upper(name) as name from {{ ref('ext_orders') }}",
    })
    assert _dbt(proj, "run").success

    export = Path(exports) / "ext_orders.parquet"
    assert export.is_file()
    assert duckdb.sql(f"select id, name from read_parquet('{export.as_posix()}') order by id"
                      ).fetchall() == [(1, "a"), (2, "b")]

    # ref() resolved through the view over the file, and only the Delta model wrote Delta.
    con = duckrun.connect(root, schema="main", read_only=True)
    assert con.sql("select id, name from mart_orders order by id").fetchall() == [(1, "A"), (2, "B")]
    assert _delta_logs(root) == ["mart_orders"]

    # Run 2 goes down the other branch — the existing relation is renamed to a backup and dropped
    # after the swap — and must land in the same place with the same contents.
    assert _dbt(proj, "run").success
    assert duckdb.sql(f"select id, name from read_parquet('{export.as_posix()}') order by id"
                      ).fetchall() == [(1, "a"), (2, "b")]
    assert _delta_logs(root) == ["mart_orders"]


def test_external_only_project_writes_no_delta(tmp_path):
    """The regression guard for duckrun's DML interception. `create table … as (select …)`,
    `insert … values`, `copy … to` and `drop table` all have to pass through to DuckDB: if the
    staging CTAS were rewritten into a delta_rs write (delta_dml._create_as), the temp relation
    would become a Delta table under root_path and the file would be written from a delta_scan."""
    proj, root, exports = _project(tmp_path, {
        "ext_only": "{{ config(materialized='external') }}\nselect 1 as id",
    })
    assert _dbt(proj, "run").success

    assert (Path(exports) / "ext_only.parquet").is_file()
    assert _delta_logs(root) == []                       # nothing Delta was written at all
    assert not list(Path(root).rglob("*__dbt_tmp"))      # and no staging leftovers


@pytest.mark.parametrize("name, config, filename", [
    ("ext_csv", "materialized='external', format='csv'", "ext_csv.csv"),
    ("ext_json", "materialized='external', location=\"{{ env_var('EXT_EXTERNAL_ROOT') }}/o.json\"",
     "o.json"),
])
def test_format_explicit_and_inferred_from_extension(tmp_path, name, config, filename):
    """`format:` picks the writer; with no `format:` it is inferred from the location's extension
    (upstream's rule), and the default location carries the format as its extension."""
    proj, root, exports = _project(tmp_path, {
        name: "{{ config(" + config + ") }}\nselect 1 as id, 'a' as name",
    })
    assert _dbt(proj, "run").success

    export = Path(exports) / filename
    assert export.is_file()
    reader = "read_csv" if export.suffix == ".csv" else "read_json"
    assert duckdb.sql(f"select id, name from {reader}('{export.as_posix()}')"
                      ).fetchall() == [(1, "a")]


def test_partitioned_export_globs_back_through_ref(tmp_path):
    """`options: {partition_by: …}` makes the location a hive directory, so the read view has to
    glob it back (adapter.external_read_location) for ref() to see every partition."""
    proj, root, exports = _project(tmp_path, {
        "ext_parts": "{{ config(materialized='external', options={'partition_by': 'g'}) }}\n"
                     "select 1 as id, 'x' as g union all select 2, 'y'",
        "mart_parts": "{{ config(materialized='table') }}\n"
                      "select count(*) as n from {{ ref('ext_parts') }}",
    })
    assert _dbt(proj, "run").success

    assert (Path(exports) / "ext_parts" / "g=x").is_dir()
    assert (Path(exports) / "ext_parts" / "g=y").is_dir()
    con = duckrun.connect(root, schema="main", read_only=True)
    assert con.sql("select n from mart_parts").fetchall() == [(2,)]


def test_empty_model_reads_back_empty(tmp_path):
    """An empty result can't be written as a schema-less file, so upstream writes one all-NULL
    sentinel row and filters it out in the read view. Downstream must see zero rows."""
    proj, root, exports = _project(tmp_path, {
        "ext_empty": "{{ config(materialized='external') }}\n"
                     "select 1 as id, 'a' as name where false",
        "mart_empty": "{{ config(materialized='table') }}\n"
                      "select count(*) as n from {{ ref('ext_empty') }}",
    })
    assert _dbt(proj, "run").success

    assert (Path(exports) / "ext_empty.parquet").is_file()
    con = duckrun.connect(root, schema="main", read_only=True)
    assert con.sql("select n from mart_empty").fetchall() == [(0,)]


def test_python_external_model(tmp_path):
    """External supports python models too (`supported_languages=['sql', 'python']`)."""
    proj, root, exports = _project(tmp_path, {
        "ext_py": 'def model(dbt, session):\n'
                  '    dbt.config(materialized="external")\n'
                  '    return session.sql("select 7 as id")\n',
    })
    assert _dbt(proj, "run").success

    export = Path(exports) / "ext_py.parquet"
    assert export.is_file()
    assert duckdb.sql(f"select id from read_parquet('{export.as_posix()}')").fetchall() == [(7,)]


def test_upstream_external_model_resolves_in_a_fresh_process(tmp_path):
    """duckrun's DuckDB is in-memory and its disk discovery only rebuilds DELTA tables, so an
    external model's view does not survive the process that built it. Upstream's answer is the
    `register_upstream_external_models()` on-run-start hook, which duckrun inherits verbatim
    (it is a plain macro, so no adapter dispatch is involved). Pin it in a genuinely separate
    process — in-process the DuckDB environment is a singleton and the view would still be there.
    """
    proj, root, exports = _project(tmp_path, {
        "ext_src": "{{ config(materialized='external') }}\nselect 1 as id",
        "mart_src": "{{ config(materialized='table') }}\n"
                    "select count(*) as n from {{ ref('ext_src') }}",
    }, on_run_start="{{ register_upstream_external_models() }}")
    assert _dbt(proj, "run").success

    # Fresh process, downstream model only: `ext_src` is never rebuilt, so the hook is the only
    # thing that can make ref('ext_src') resolve.
    out = subprocess.run(
        [sys.executable, "-m", "dbt.cli.main", "run", "--select", "mart_src",
         "--project-dir", str(proj), "--profiles-dir", str(proj)],
        capture_output=True, text=True, env={**os.environ},
    )
    assert out.returncode == 0, out.stdout + out.stderr

    con = duckrun.connect(root, schema="main", read_only=True)
    assert con.sql("select n from mart_src").fetchall() == [(1,)]
