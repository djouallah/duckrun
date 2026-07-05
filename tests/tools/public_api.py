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
TEST_FILE = os.path.join(os.path.dirname(__file__), os.pardir,
                         "connection_api", "test_connection_api.py")

# Optional parameters that can't be honestly unit-tested on a local filesystem — covered elsewhere.
# Keep this list tiny and reasoned; everything else must be exercised by the suite.
COVERAGE_EXEMPT = {
    ("DuckSession", "attach", "storage_options"),   # needs real S3/OneLake creds — integration_tests
}


def signature(func):
    """A method's parameter contract as a normalized string — param names, order, defaults, and
    ``*args`` / ``**kwargs``, with ``self`` / ``cls`` and type annotations stripped (so a pure
    type-hint edit doesn't trip the gate, but adding / removing / renaming a parameter, or flipping
    it required↔optional, does)."""
    s = inspect.signature(func)
    ps = [p.replace(annotation=inspect.Parameter.empty)
          for n, p in s.parameters.items() if n not in ("self", "cls")]
    return str(s.replace(parameters=ps, return_annotation=inspect.Signature.empty))


def member_entry(cls, name):
    """`name(sig)` for a method, `name (property)` / `name (accessor)` for a non-callable accessor."""
    if isinstance(inspect.getattr_static(cls, name, None), property):
        return f"{name} (property)"
    try:
        attr = getattr(cls, name)
    except AttributeError:                       # instance attribute (e.g. self.catalog) — an accessor
        return f"{name} (accessor)"
    return f"{name}{signature(attr)}" if callable(attr) else f"{name} (attr)"


def _members(cls, extra):
    names = {n for n, _ in inspect.getmembers(cls) if not n.startswith("_")}
    names.update(extra)
    return sorted(names)


def public_api():
    """The canonical, sorted contract: top-level exports (``duckrun.<name>``) + every surface's
    public members, each **with its parameter signature** (``Surface.member(params)``)."""
    entries = []
    for n in duckrun.__all__:
        if n.startswith("_"):
            continue
        obj = getattr(duckrun, n)
        entries.append(f"duckrun.{n}{signature(obj)}" if inspect.isfunction(obj)
                       else f"duckrun.{n} (class)")
    for surface, cls, extra in SURFACES:
        for m in _members(cls, extra):
            if (surface, m) not in EXCLUDE:
                entries.append(f"{surface}.{member_entry(cls, m)}")
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


def _key(entry):
    """The method identity — the ``Surface.member`` part, dropping the ``(signature)`` / marker."""
    return entry.split("(", 1)[0].rstrip()


def diff():
    """(removed, added, changed): methods gone from the surface, brand-new methods, and methods whose
    parameter signature changed. `removed` is a method deletion; `changed` is a param add/remove/
    rename or a required↔optional flip — both are breaking; `added` is additive."""
    cur = {_key(e): e for e in public_api()}
    base = {_key(e): e for e in read_baseline()}
    removed = sorted(k for k in base if k not in cur)
    added = sorted(k for k in cur if k not in base)
    changed = sorted(f"{base[k]}  ->  {cur[k]}" for k in cur if k in base and cur[k] != base[k])
    return removed, added, changed


import re as _re


def _call_arg_strings(method, src):
    """Every ``method(...)`` / ``.method(...)`` call in the test source, as its raw arg string
    (balanced-paren aware). Lookbehind excludes a word char so ``submethod(`` doesn't match, but
    allows a leading ``.`` so ``obj.method(`` does."""
    for m in _re.finditer(r"(?<!\w)" + _re.escape(method) + r"\s*\(", src):
        i, depth, buf = m.end(), 1, []
        while i < len(src) and depth:
            c = src[i]
            if c == "(":
                depth += 1
            elif c == ")":
                depth -= 1
                if depth == 0:
                    break
            buf.append(c)
            i += 1
        yield "".join(buf)


def _split_top(s):
    parts, depth, cur = [], 0, ""
    for c in s:
        if c in "([{":
            depth += 1
        elif c in ")]}":
            depth -= 1
        if c == "," and depth == 0:
            parts.append(cur)
            cur = ""
        else:
            cur += c
    if cur.strip():
        parts.append(cur)
    return [p.strip() for p in parts if p.strip()]


def uncovered_params():
    """Optional parameters the test suite never exercises — reached neither positionally nor by
    keyword in test_connection_api.py — minus the documented exemptions. Empty == full coverage.

    An optional param is 'covered' if some call to its method either passes it by keyword (``p=``) or
    supplies enough positional args to reach its position. Required params are covered by any call;
    ``*args`` / ``**kwargs`` are skipped."""
    src = open(TEST_FILE, encoding="utf-8").read()
    out = []
    for surface, cls, extra in SURFACES:
        for name in _members(cls, extra):
            if (surface, name) in EXCLUDE:
                continue
            if isinstance(inspect.getattr_static(cls, name, None), property):
                continue
            try:
                attr = getattr(cls, name)
            except AttributeError:
                continue
            if not callable(attr):
                continue
            try:
                params = [(n, p) for n, p in inspect.signature(attr).parameters.items()
                          if n not in ("self", "cls")]
            except (TypeError, ValueError):
                continue
            max_pos, kw = 0, set()
            for a in _call_arg_strings(name, src):
                pos = 0
                for part in _split_top(a):
                    mm = _re.match(r"([a-zA-Z_]\w*)\s*=(?!=)", part)
                    if mm:
                        kw.add(mm.group(1))
                    else:
                        pos += 1
                max_pos = max(max_pos, pos)
            for idx, (n, p) in enumerate(params):
                if p.default is inspect.Parameter.empty or p.kind in (
                        p.VAR_POSITIONAL, p.VAR_KEYWORD):
                    continue
                if (n in kw) or (idx < max_pos) or ((surface, name, n) in COVERAGE_EXEMPT):
                    continue
                out.append(f"{surface}.{name}({n}=)")
    return sorted(out)


def main(argv):
    if "--coverage" in argv:
        miss = uncovered_params()
        if not miss:
            print("every method + optional parameter is exercised by the test suite")
            return 0
        print(f"{len(miss)} optional parameter(s) never exercised in test_connection_api.py:")
        print("\n".join(f"  - {e}" for e in miss))
        return 1
    if "--write" in argv:
        api = write_baseline()
        print(f"wrote {len(api)} entries to {os.path.relpath(BASELINE)}")
        return 0
    if "--check" in argv:
        removed, added, changed = diff()
        if not removed and not added and not changed:
            print(f"public API matches baseline ({len(read_baseline())} entries)")
            return 0
        if removed:
            print("BREAKING — public API removed (regenerate the baseline only if intentional):")
            print("\n".join(f"  - {e}" for e in removed))
        if changed:
            print("BREAKING — parameter signature changed (regenerate the baseline if intentional):")
            print("\n".join(f"  ~ {e}" for e in changed))
        if added:
            print("public API added (regenerate the baseline to record it):")
            print("\n".join(f"  + {e}" for e in added))
        print("\n  python tests/tools/public_api.py --write   # to accept these changes")
        return 1
    print("\n".join(public_api()))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
