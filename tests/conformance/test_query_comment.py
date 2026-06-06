"""Conformance: dbt.tests.adapter.query_comment.* — query-comment injection."""
from dbt.tests.adapter.query_comment import test_query_comment

from _subclass import export

export(globals(), test_query_comment)
