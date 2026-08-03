"""The in-process dbt builds here (``_build_everything(threads=4)``) make adapter init call
``engine.set_run_threads``, mutating the process-global ``engine.RUN_THREADS`` with no teardown —
which would leak a stale thread count into any suite that runs after this one in the same pytest
process (the plugin's multi-thread cursor guard and ``pool_workers`` read it). Restore it around
every test."""
import pytest

from dbt.adapters.duckrun import engine


@pytest.fixture(autouse=True)
def _restore_run_threads():
    saved = engine.RUN_THREADS
    yield
    engine.RUN_THREADS = saved
