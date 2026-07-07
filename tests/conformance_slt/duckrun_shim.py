"""Validation shim: exposes connect(path).sql() over plain DuckDB (file db),
so the suite's expected results can be verified against DuckDB semantics."""
import os
import duckdb

class _Con:
    def __init__(self, path):
        self._c = duckdb.connect(os.path.join(path, "shim.duckdb"))
    def sql(self, q):
        return self._c.sql(q)
    def close(self):
        self._c.close()

def connect(path):
    os.makedirs(path, exist_ok=True)
    return _Con(path)
