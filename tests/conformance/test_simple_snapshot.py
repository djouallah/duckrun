from dbt.tests.adapter.simple_snapshot.test_snapshot import (
    BaseSnapshotCheck,
    BaseSimpleSnapshot,
)
from dbt.tests.util import relation_from_name, run_dbt, write_file
import pytest


class TestSimpleSnapshotDuckDB(BaseSimpleSnapshot):
    pass


class TestSnapshotCheckDuckDB(BaseSnapshotCheck):
    pass


# ---------------------------------------------------------------------------
# Schema evolution on snapshots (issue #8).
#
# dbt's default snapshot materialization unconditionally appends new source
# columns (get_missing_columns -> create_columns). duckrun's Delta-backed
# snapshot reuses the merge plumbing and used to hardcode on_schema_change
# 'ignore', so a column added upstream was silently dropped: `dbt snapshot`
# succeeded, a new SCD2 version was created, but the new column never landed.
# The macro now passes 'append_new_columns' to match dbt.
# ---------------------------------------------------------------------------

# The snapshot's source, materialized to Delta so it persists across the
# separate `dbt run` and `dbt snapshot` invocations. v1: no `region` column.
_source_v1 = """
{{ config(materialized='table') }}
select 1 as id, 10 as price, 'a' as category
"""

# v2: a tracked column changed (price 10 -> 20, so the check strategy creates a
# real new SCD2 version) AND an additive new column (`region`) — exactly the
# repro in issue #8.
_source_v2 = """
{{ config(materialized='table') }}
select 1 as id, 20 as price, 'a' as category, 'east' as region
"""

# v3: another tracked-column change AFTER the evolution boundary — the previously
# closed versions must stay untouched (region NULL on the pre-evolution one).
_source_v3 = """
{{ config(materialized='table') }}
select 1 as id, 30 as price, 'a' as category, 'west' as region
"""

# v2-no-change: the new column is added but NO tracked column changes — the run
# evolves the schema (region present, NULL everywhere) without closing any version.
_source_v2_nochange = """
{{ config(materialized='table') }}
select 1 as id, 10 as price, 'a' as category, 'east' as region
"""

_snapshot_sql = """
{% snapshot col_snapshot %}
    {{ config(
        unique_key='id', strategy='check', check_cols=['price', 'category'],
        target_database=database, target_schema=schema
    ) }}
    select * from {{ ref('snap_source') }}
{% endsnapshot %}
"""


class TestSnapshotSchemaEvolution:
    """A column added upstream between snapshot runs must be tracked, not dropped,
    and existing SCD2 history must be preserved."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"snap_source.sql": _source_v1}

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"col_snapshot.sql": _snapshot_sql}

    def test_new_column_is_appended_not_dropped(self, project):
        # Build the initial source and snapshot history.
        run_dbt(["run"])
        results = run_dbt(["snapshot"])
        assert all(r.status == "success" for r in results)

        relation = relation_from_name(project.adapter, "col_snapshot")
        assert project.run_sql(f"select count(*) from {relation}", fetch="one")[0] == 1

        # Evolve the source: change a tracked column (triggers a new version) and
        # add a new column. Rebuild the source table, then re-snapshot.
        write_file(_source_v2, project.project_root, "models", "snap_source.sql")
        run_dbt(["run"])
        results = run_dbt(["--no-partial-parse", "snapshot"])
        assert all(r.status == "success" for r in results)

        # The `region` column exists now (not silently dropped) and both SCD2 versions of
        # id=1 survive: the old one closed, the new one open.
        rows = project.run_sql(
            f"select price, region, (dbt_valid_to is null) as is_open "
            f"from {relation} order by dbt_valid_from",
            fetch="all",
        )
        assert len(rows) == 2

        old_version, new_version = rows
        # Old version: history preserved — the pre-change price is intact and the row is closed.
        assert old_version[0] == 10
        assert old_version[2] is False
        # The column added at the evolution boundary reads NULL on the already-closed version:
        # dbt's default snapshot update clause sets only dbt_valid_to, so a column that did not
        # exist when this version was closed must be NULL — not back-filled from the current source.
        # duckrun evolves the schema as a metadata-only commit BEFORE the merge to get this right.
        assert old_version[1] is None
        # New version: the added column is present and carries its value, and it's the open row.
        assert new_version[0] == 20
        assert new_version[1] == "east"
        assert new_version[2] is True

        # Regression: a further tracked-column change AFTER the evolution boundary must not disturb
        # the already-closed versions. The pre-evolution version keeps region NULL; the vNprev
        # version (region 'east') is now closed; the new version is open.
        write_file(_source_v3, project.project_root, "models", "snap_source.sql")
        run_dbt(["run"])
        results = run_dbt(["--no-partial-parse", "snapshot"])
        assert all(r.status == "success" for r in results)

        rows = project.run_sql(
            f"select price, region, (dbt_valid_to is null) as is_open "
            f"from {relation} order by dbt_valid_from",
            fetch="all",
        )
        assert len(rows) == 3
        v10, v20, v30 = rows
        assert (v10[0], v10[1], v10[2]) == (10, None, False)   # pre-evolution version untouched, NULL
        assert (v20[0], v20[1], v20[2]) == (20, "east", False)  # now closed, value preserved
        assert (v30[0], v30[1], v30[2]) == (30, "west", True)   # newest open version


class TestSnapshotSchemaEvolutionNoVersionChange:
    """A column added upstream with NO tracked-column change: the schema evolves (the new column is
    present and NULL on the existing row) but no version is closed and no new version is inserted."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"snap_source.sql": _source_v1}

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"col_snapshot.sql": _snapshot_sql}

    def test_column_added_without_version_change(self, project):
        run_dbt(["run"])
        results = run_dbt(["snapshot"])
        assert all(r.status == "success" for r in results)

        relation = relation_from_name(project.adapter, "col_snapshot")
        assert project.run_sql(f"select count(*) from {relation}", fetch="one")[0] == 1

        # Add `region` but leave the tracked columns (price, category) unchanged.
        write_file(_source_v2_nochange, project.project_root, "models", "snap_source.sql")
        run_dbt(["run"])
        results = run_dbt(["--no-partial-parse", "snapshot"])
        assert all(r.status == "success" for r in results)

        rows = project.run_sql(
            f"select price, region, (dbt_valid_to is null) as is_open "
            f"from {relation} order by dbt_valid_from",
            fetch="all",
        )
        # Still exactly one row: nothing was closed (no tracked change), and the new column is
        # present and NULL on the existing row — the schema evolved, the history did not.
        assert len(rows) == 1
        assert rows[0][0] == 10
        assert rows[0][1] is None
        assert rows[0][2] is True
