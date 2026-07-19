import pytest

from dbt.tests.util import run_dbt_and_capture
from dbt.tests.adapter.constraints.test_constraints import (
    BaseTableConstraintsColumnsEqual,
    BaseViewConstraintsColumnsEqual,
    BaseIncrementalConstraintsColumnsEqual,
    BaseConstraintsRuntimeDdlEnforcement,
    BaseConstraintsRollback,
    BaseIncrementalConstraintsRuntimeDdlEnforcement,
    BaseIncrementalConstraintsRollback,
    BaseModelConstraintsRuntimeEnforcement,
)


class DuckDBColumnEqualSetup:
    @pytest.fixture
    def int_type(self):
        return "INT"

    @pytest.fixture
    def string_type(self):
        return "VARCHAR"

    @pytest.fixture
    def data_types(self, schema_int_type, int_type, string_type):
        # sql_column_value, schema_data_type, error_data_type
        return [
            ["1", schema_int_type, int_type],
            ["'1'", string_type, string_type],
            ["true", "bool", "BOOL"],
            ["'2013-11-03 00:00:00-07'::timestamp", "TIMESTAMP", "TIMESTAMP"],
            ["'2013-11-03 00:00:00-07'::timestamptz", "TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE"],
            ["ARRAY['a','b','c']", "VARCHAR[]", "VARCHAR[]"],
            ["ARRAY[1,2,3]", "INTEGER[]", "INTEGER[]"],
            ["'1'::numeric", "numeric", "DECIMAL"],
            [
                """'{"bar": "baz", "balance": 7.77, "active": false}'::json""",
                "json",
                "JSON",
            ],
        ]


class TestTableConstraintsColumnsEqual(
    DuckDBColumnEqualSetup, BaseTableConstraintsColumnsEqual
):
    pass


class TestViewConstraintsColumnsEqual(
    DuckDBColumnEqualSetup, BaseViewConstraintsColumnsEqual
):
    pass


class TestIncrementalConstraintsColumnsEqual(
    DuckDBColumnEqualSetup, BaseIncrementalConstraintsColumnsEqual
):
    pass


# duckrun materializes every model as a `create or replace view ... select * from delta_scan(...)`
# over a delta_rs-written Delta table — it never emits the standard `create table (... constraints)`
# DDL. The behavioral half of these suites (column equivalence + NOT NULL) is enforced for real in
# the materialization/plugin and passes above; these *_ddl tests only assert the literal emitted
# statement, so per dbt-tests-adapter's subclass-and-override design we override the expected DDL to
# duckrun's real view output. (After the test's find/replace of the model name, the delta_scan path's
# last segment also becomes <model_identifier>, so the normalized statement is the line below.)
_DUCKRUN_VIEW_DDL = "create or replace view <model_identifier> as select * from <model_identifier>"


@pytest.mark.skip_profile("md")
class TestTableConstraintsRuntimeDdlEnforcement(
    DuckDBColumnEqualSetup, BaseConstraintsRuntimeDdlEnforcement
):
    @pytest.fixture(scope="class")
    def expected_sql(self):
        return _DUCKRUN_VIEW_DDL


@pytest.mark.skip_profile("md", "buenavista")
class TestTableConstraintsRollback(DuckDBColumnEqualSetup, BaseConstraintsRollback):
    @pytest.fixture(scope="class")
    def expected_error_messages(self):
        return ["NOT NULL constraint failed"]


@pytest.mark.skip_profile("md")
class TestIncrementalConstraintsRuntimeDdlEnforcement(
    DuckDBColumnEqualSetup, BaseIncrementalConstraintsRuntimeDdlEnforcement
):
    @pytest.fixture(scope="class")
    def expected_error_messages(self):
        return ["NOT NULL constraint failed"]

    @pytest.fixture(scope="class")
    def expected_sql(self):
        return _DUCKRUN_VIEW_DDL


@pytest.mark.skip_profile("md", "buenavista")
class TestIncrementalConstraintsRollback(
    DuckDBColumnEqualSetup, BaseIncrementalConstraintsRollback
):
    @pytest.fixture(scope="class")
    def expected_error_messages(self):
        return ["NOT NULL constraint failed"]


@pytest.mark.skip_profile("md")
class TestModelConstraintsRuntimeEnforcement(
    DuckDBColumnEqualSetup, BaseModelConstraintsRuntimeEnforcement
):
    @pytest.fixture(scope="class")
    def expected_error_messages(self):
        return ["NOT NULL constraint failed"]

    @pytest.fixture(scope="class")
    def expected_sql(self):
        return _DUCKRUN_VIEW_DDL


# duckrun enforces a contract's column shape + not_null for real, but check/pk/fk/unique have no
# delta_rs equivalent. Per dbt's NOT_ENFORCED convention the run stays green and WARNS (silent
# acceptance would be the same divergence class as an ignored merge config); warn_unenforced: false
# opts a constraint out of the warning. Regression for the silent-pass gap found in review.
_unenforced_warn_model = """
{{ config(materialized='table') }}
select 1 as id
"""

_unenforced_silent_model = """
{{ config(materialized='table') }}
select 1 as id
"""

_unenforced_schema_yml = """
version: 2
models:
  - name: warn_model
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: int
        constraints:
          - type: not_null
          - type: check
            expression: id > 0
  - name: silent_model
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: int
        constraints:
          - type: primary_key
            warn_unenforced: false
"""


class TestUnenforcedConstraintWarns:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "warn_model.sql": _unenforced_warn_model,
            "silent_model.sql": _unenforced_silent_model,
            "schema.yml": _unenforced_schema_yml,
        }

    def test_check_constraint_warns_but_passes(self, project):
        _, logs = run_dbt_and_capture(["run", "--select", "warn_model"], expect_pass=True)
        assert "cannot enforce: check on id" in logs

    def test_warn_unenforced_false_is_silent(self, project):
        _, logs = run_dbt_and_capture(["run", "--select", "silent_model"], expect_pass=True)
        assert "cannot enforce" not in logs
