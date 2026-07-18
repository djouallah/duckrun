"""
Render the duckrun.workspace() **Fabric-workspace API** as a Markdown card for the docs
(docs/api-reference.md, the WORKSPACE_API block).

    python tests/tools/workspace_summary.py            # print the card

The method list is **introspected from the shipped classes** (`duckrun.workspace` + the `Workspace`
handle it returns) — NOT hand-listed — so it lists the exact public surface and can't drift from the
code. This is the deploy/run/schedule analogue of tests/tools/connection_summary.py; the two share
the same signature/entry formatting from public_api.py so they can never disagree about what a
method's parameters are.

Unlike the connection card there's no pass/fail badge: the Workspace surface talks to live Microsoft
Fabric, so it's exercised by the manual deploy demo (tests/deploy_testing), not a unit XML.
"""
import inspect
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import duckrun   # noqa: E402
from duckrun.workspace import Workspace   # noqa: E402
from public_api import member_entry, signature   # noqa: E402

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

# Public instance attributes set in Workspace.__init__ that class introspection can't see (they're
# not class attributes), so name them explicitly the way the connection card names its accessors.
_ACCESSORS = ["id", "name"]

_MARKERS = {"(property)": "property", "(accessor)": "accessor", "(attr)": "attribute"}


def _members():
    names = {n for n, _ in inspect.getmembers(Workspace) if not n.startswith("_")}
    names.update(_ACCESSORS)
    return sorted(names)


def _rows():
    """Code-derived (surface, member, params) rows — the module entry point plus every public
    member of the returned handle, params taken from the real signature."""
    rows = [("duckrun", "workspace", signature(duckrun.workspace)[1:-1])]
    for m in _members():
        entry = member_entry(Workspace, m)                 # "deploy(source, ...)" | "display_name (property)"
        params = entry[len(m) + 1:-1] if entry.startswith(m + "(") else entry[len(m):].strip()
        rows.append(("Workspace", m, params))
    return rows


def render():
    rows = _rows()
    out = ["## duckrun workspace API — Fabric artifact deploy", ""]
    out += [f"🗂️ **{len(rows)} public methods** · deploy · run · schedule", ""]
    out += ["> Introspected from the shipped classes — the exact public surface of "
            "`duckrun.workspace()` and the `Workspace` handle it returns, signatures and all, not a "
            "hand-maintained list. It drives Microsoft Fabric (create lakehouses, deploy notebooks / "
            "semantic models / pipelines / variable libraries, run and schedule them) — see the "
            "[Workspace (Fabric)](workspace.md) page. Exercised by the manual deploy demo "
            "([`tests/deploy_testing`](../tests/deploy_testing)).", ""]
    out += ["| Surface | Method | Parameters |", "| --- | --- | --- |"]
    for surface, method, params in rows:
        if params in _MARKERS:
            cell = f"*{_MARKERS[params]}*"
        elif params == "":
            cell = "*(none)*"
        else:
            cell = f"`{params}`"
        out.append(f"| `{surface}` | `{method}` | {cell} |")
    out.append("")
    return "\n".join(out)


if __name__ == "__main__":
    print(render())
