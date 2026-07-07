"""Write-enabled duckrun adapter for the conformance-SLT runner.

The suite drives any engine that exposes ``connect(path).sql(q)``. duckrun's real
``connect()`` defaults to ``read_only=True`` (so an accidental write can't mutate a shared
lakehouse), which would make every DDL/DML in the suite raise ``PermissionError``. This thin
shim opens the same session with writes enabled — the ONLY duckrun-specific glue the suite needs,
mirroring the suite's own ``duckrun_shim.py`` (which adapts plain DuckDB as the semantics oracle).
"""
import duckrun


def connect(path):
    return duckrun.connect(path, read_only=False)
