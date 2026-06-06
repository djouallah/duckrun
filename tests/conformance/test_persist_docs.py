"""Conformance: dbt.tests.adapter.persist_docs.* — persisting model/column descriptions."""
from dbt.tests.adapter.persist_docs import test_persist_docs

from _subclass import export

export(globals(), test_persist_docs)
