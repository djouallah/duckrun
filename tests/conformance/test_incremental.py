"""Conformance: dbt.tests.adapter.incremental.* — incremental strategies, schema change, predicates."""
from dbt.tests.adapter.incremental import (
    test_incremental_merge_exclude_columns,
    test_incremental_microbatch,
    test_incremental_on_schema_change,
    test_incremental_predicates,
    test_incremental_unique_id,
)

from _subclass import export

export(
    globals(),
    test_incremental_merge_exclude_columns,
    test_incremental_microbatch,
    test_incremental_on_schema_change,
    test_incremental_predicates,
    test_incremental_unique_id,
)
