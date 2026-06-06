"""Conformance: dbt.tests.adapter.simple_snapshot.* — snapshot strategies and configs."""
from dbt.tests.adapter.simple_snapshot import (
    test_ephemeral_snapshot_hard_deletes,
    test_snapshot,
    test_various_configs,
)

from _subclass import export

export(
    globals(),
    test_ephemeral_snapshot_hard_deletes,
    test_snapshot,
    test_various_configs,
)
