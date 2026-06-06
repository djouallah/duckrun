"""Conformance: dbt.tests.adapter.basic.* — core materializations, docs, ephemeral, etc."""
from dbt.tests.adapter.basic import (
    test_adapter_methods,
    test_base,
    test_docs_generate,
    test_empty,
    test_ephemeral,
    test_generic_tests,
    test_get_catalog_for_single_relation,
    test_incremental,
    test_singular_tests,
    test_singular_tests_ephemeral,
    test_snapshot_check_cols,
    test_snapshot_timestamp,
    test_table_materialization,
    test_validate_connection,
)

from _subclass import export

export(
    globals(),
    test_adapter_methods,
    test_base,
    test_docs_generate,
    test_empty,
    test_ephemeral,
    test_generic_tests,
    test_get_catalog_for_single_relation,
    test_incremental,
    test_singular_tests,
    test_singular_tests_ephemeral,
    test_snapshot_check_cols,
    test_snapshot_timestamp,
    test_table_materialization,
    test_validate_connection,
)
