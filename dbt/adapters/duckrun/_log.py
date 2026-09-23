"""The adapter's logger, shared by engine / delta_dml / policy.

dbt's ``AdapterLogger`` when dbt is installed (so messages land in dbt's event log). A lean
``pip install duckrun`` (no ``[dbt]`` extra) only runs ``duckrun.connect()``, which uses these
modules without dbt — there it falls back to plain stdlib logging.
"""
try:
    from dbt.adapters.events.logging import AdapterLogger

    logger = AdapterLogger("Duckrun")
except ImportError:
    import logging

    logger = logging.getLogger("duckrun")
