"""
Helper that turns every official ``Base*`` test class in a dbt-tests-adapter module into a
concrete ``Test*`` subclass, injected into the caller's module namespace so pytest collects it.

This is the standard adapter-conformance pattern (`class TestX(BaseX): pass`), done in bulk so
the suite stays in lockstep with whatever dbt-tests-adapter version is installed — no class is
silently dropped when dbt adds one.
"""
import inspect

# `*Base` / `*Setup` classes are shared scaffolding, not runnable tests; `*Postgres*` classes
# are Postgres-specific rather than generic conformance. Skip both.
_SKIP_SUFFIXES = ("Base", "Setup")


def export(globals_dict, *modules):
    target_module = globals_dict.get("__name__")
    for module in modules:
        for name, obj in inspect.getmembers(module, inspect.isclass):
            if obj.__module__ != module.__name__:
                continue
            if not name.startswith("Base"):
                continue
            if "Postgres" in name:
                continue
            if name.endswith(_SKIP_SUFFIXES):
                continue
            test_name = "Test" + name[len("Base"):]
            globals_dict[test_name] = type(test_name, (obj,), {"__module__": target_module})
