"""
Single source of truth for duckrun's **public connection-API surface**, plus the removal gate.

The surface is introspected from the shipped classes — never hand-listed — so it can't drift from
the code. Two consumers share this one definition:

  * tests/tools/connection_summary.py  — renders the API-reference card (docs/api-reference.md)
  * tests/connection_api/test_public_api.py — the gate: fails if the live surface no longer matches
    the committed baseline (tests/connection_api/public_api_baseline.txt)

So a public method can't silently disappear: removing one (or adding one) makes the gate fail until
the baseline is **intentionally** regenerated. Removing an API is a breaking change and must be a
deliberate act, not an accident.

    python tests/tools/public_api.py            # print the current surface
    python tests/tools/public_api.py --check    # exit 1 if it differs from the baseline (CI gate)
    python tests/tools/public_api.py --write     # regenerate the baseline (the explicit opt-in)
"""
import inspect
import os
import sys

import duckrun
from duckrun import session as _S, delta_table as _D

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

# The reachable public API surfaces, keyed by how a user gets to each: the top-level module, then the
# objects hung off a session (conn.catalog / conn.read / df.write / dt.merge(...)). (label, class,
# extra members introspection can't see because they're instance attributes set in __init__).
SURFACES = [
    ("DuckSession", _S.DuckSession, ["catalog"]),   # self.catalog = Catalog(self)
    ("Catalog", _S.Catalog, []),
    ("DataFrame", _S.DataFrame, []),
    ("DataFrameReader", _S.DataFrameReader, []),
    ("DataFrameWriter", _S.DataFrameWriter, []),
    ("DeltaTable", _D.DeltaTable, []),
    ("DeltaMergeBuilder", _D.DeltaMergeBuilder, []),
]

# Public members that exist but aren't part of the advertised contract (internal plumbing accessors).
EXCLUDE = {("DuckSession", "root_path"), ("DuckSession", "storage_options")}

BASELINE = os.path.join(os.path.dirname(__file__), os.pardir,
                        "connection_api", "public_api_baseline.txt")


def _members(cls, extra):
    names = {n for n, _ in inspect.getmembers(cls) if not n.startswith("_")}
    names.update(extra)
    return sorted(names)


def public_api():
    """The canonical, sorted contract: top-level exports (``duckrun.<name>``) + every surface's
    public members (``Surface.member``)."""
    entries = [f"duckrun.{n}" for n in duckrun.__all__ if not n.startswith("_")]
    for surface, cls, extra in SURFACES:
        for m in _members(cls, extra):
            if (surface, m) not in EXCLUDE:
                entries.append(f"{surface}.{m}")
    return sorted(entries)


def read_baseline():
    with open(BASELINE, encoding="utf-8") as f:
        return sorted(l.strip() for l in f if l.strip() and not l.startswith("#"))


def write_baseline():
    api = public_api()
    with open(BASELINE, "w", encoding="utf-8", newline="\n") as f:
        f.write("# duckrun public connection-API surface — the removal-gate baseline.\n")
        f.write("# Introspected from the code; DO NOT hand-edit. Regenerate intentionally with:\n")
        f.write("#     python tests/tools/public_api.py --write\n")
        f.write("# A diff here means a public API was added or removed — a removal is a breaking change.\n")
        f.write("\n".join(api) + "\n")
    return api


def diff():
    """(removed, added) — baseline entries gone from the live surface, and live entries not yet in
    the baseline. `removed` is the breaking one."""
    current = set(public_api())
    base = set(read_baseline())
    return sorted(base - current), sorted(current - base)


def main(argv):
    if "--write" in argv:
        api = write_baseline()
        print(f"wrote {len(api)} entries to {os.path.relpath(BASELINE)}")
        return 0
    if "--check" in argv:
        removed, added = diff()
        if not removed and not added:
            print(f"public API matches baseline ({len(read_baseline())} entries)")
            return 0
        if removed:
            print("BREAKING — public API removed (regenerate the baseline only if intentional):")
            print("\n".join(f"  - {e}" for e in removed))
        if added:
            print("public API added (regenerate the baseline to record it):")
            print("\n".join(f"  + {e}" for e in added))
        print("\n  python tests/tools/public_api.py --write   # to accept these changes")
        return 1
    print("\n".join(public_api()))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
