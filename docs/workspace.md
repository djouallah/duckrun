---
hide:
  - navigation
---

# Fabric workspace handle

`connect()` points *into* an existing lakehouse, and works the same on local, `s3://`, `gs://`, or
OneLake. Creating a lakehouse is the opposite job — Fabric-only, control-plane, and there's no
lakehouse to point at yet — so it lives on a separate **workspace handle**:

```python
import duckrun

ws = duckrun.workspace("My Workspace")          # workspace name or GUID
lh_id = ws.create_lakehouse("bronze")           # returns the lakehouse item id
nb_id = ws.deploy("etl.ipynb")                  # deploy a notebook
sm_id = ws.deploy("model.bim")                  # deploy + refresh a semantic model
pl_id = ws.deploy("pipeline.json")              # deploy a data pipeline
ws.list_lakehouses()                            # [{"displayName": ..., "id": ...}, ...]
```

**`create_lakehouse(name, schemas=True)`** provisions an empty container by name. It is
**idempotent**: if a lakehouse of that name already exists it returns its id unchanged (nothing is
re-created), so it's safe to call before every run. Pass `schemas=False` for a non-schema-enabled
lakehouse. It raises on a real API failure rather than returning a sentinel.

**`deploy(source, name=None, overwrite=False)`** pushes a file artifact to the workspace. `source` is
a local file path or an `http(s)` URL (e.g. a GitHub raw URL). The item type comes from the
extension — `.ipynb` → notebook, `.bim` → semantic model, `.json` → data pipeline — and the name
defaults to the filename stem (override with `name=`). A `.bim` is also **refreshed** after deploy (a
*reframe* onto the latest Delta data for Direct Lake), so `deploy` returns only once the model is
live. A `.json` is deployed **verbatim** as a data pipeline — its contents are validated as a Fabric
pipeline definition (`properties.activities`) but no notebook/workspace ids are rewritten. It is
**not** idempotent: if an item of that name already exists it is replaced only when `overwrite=True`,
otherwise the call raises rather than hide a stale deploy.

`deploy` does create + refresh; it does **not** set up a data-source *connection binding*. A freshly
deployed Direct Lake model whose connection isn't already resolvable will fail the refresh — that
Power BI error is surfaced, not swallowed. Binding is a separate step (needs a connection id).

Authentication reuses the same Fabric control-plane token as [remote execution](remote.md): inside a
Fabric notebook it's automatic (`notebookutils`), and locally it comes from `FABRIC_TOKEN`, GitHub
OIDC, or `az login`. The semantic-model refresh additionally needs a Power BI token (`POWERBI_TOKEN`
or `az login --scope https://analysis.windows.net/powerbi/api/.default`); inside Fabric it's
automatic. Pass `token=` to inject the Fabric one.

Pipelines, TMDL-folder semantic models, and git-folder workspace CD are intentionally out of scope —
use [`fabric-cicd`](https://microsoft.github.io/fabric-cicd/) or
[`semantic-link-labs`](https://github.com/microsoft/semantic-link-labs) for those.
