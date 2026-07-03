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
        # New version: the added column is present and carries its value, and it's the open row.
        assert new_version[0] == 20
        assert new_version[1] == "east"
        assert new_version[2] is True
        # NOTE: we deliberately do not assert old_version's `region`. dbt's default snapshot would
        # leave it NULL (its update clause only sets dbt_valid_to); delta-rs, when it evolves the
        # schema mid-merge, back-fills the newly added column on the matched (closed) row from the
        # source. That's an accepted syntax-vs-behavior diff — the column is tracked, which is the
        # point of issue #8.
