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
sm_id = ws.deploy("model.bim", lakehouse="bronze")  # Direct Lake model, pointed at a lakehouse
pl_id = ws.deploy("pipeline.json")              # deploy a data pipeline
vl_id = ws.deploy("variables.json", variables={"lakehouse_name": "bronze", "workspace_id": ws.id})
ws.run("etl.ipynb")                             # run a deployed notebook/pipeline on Fabric, wait
ws.schedule("pipeline", daily="06:00")          # or every="1h" / weekly=["Mon"], at="06:00"
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
live. A `.json` is a **data pipeline** or a **variable library**, told apart by content: a pipeline
(`properties.activities`) is deployed verbatim (no id rewriting); a variable library (`variables`)
takes an optional `variables=` mapping to set values at deploy time —
`ws.deploy("variables.json", variables={"lakehouse_name": "bronze", "workspace_id": ws.id})` — the
environment-specific injection, without editing the file (an unknown variable name raises). A `.json`
that is neither raises. `deploy` is **not** idempotent: if an item of that name already exists it is
replaced only when `overwrite=True`, otherwise the call raises rather than hide a stale deploy.

**Pointing a Direct Lake model at a lakehouse.** A Direct Lake `model.bim` bakes in the OneLake
workspace + lakehouse GUIDs it reads from; deploying it elsewhere would leave it pointed at the wrong
place. `lakehouse=` fixes that — you name a lakehouse in this workspace and duckrun rewrites those
GUIDs for you (no connection strings or GUIDs to edit):

```python
ws.deploy("model.bim", lakehouse="silver")   # point the model at the silver lakehouse
ws.deploy("model.bim")                        # workspace has one lakehouse → inferred
```

The lakehouse is **inferred** when the model already targets one that exists in this workspace, or
the workspace has exactly one; with several you must name it (otherwise `deploy` raises, listing
them). A wrong name raises with the available names. `lakehouse=` is ignored for `.ipynb` / `.json`.

`deploy` does create + refresh, and for Direct Lake that's the whole story: a Direct Lake model reads
the Delta files straight from OneLake with the caller's own identity, so there's no gateway or stored
data-source credential to bind — the reframe just works once the model is deployed.

**`run(name)`** executes a deployed notebook or data pipeline on Fabric compute and waits for it,
returning the terminal job status (raising on failure). `name` is the item's display name with or
without the source extension — `run("etl.ipynb")` / `run("etl")` run the notebook,
`run("pipeline.json")` the pipeline; a bare name is looked up as a notebook then a pipeline. It's
inherently remote (the job runs on Fabric, not your machine). Parameterizing a run is a **pipeline's**
job — a pipeline passes parameters to the notebooks it orchestrates — so `run` itself takes none.

**`schedule(name, every=/daily=/weekly=, at=, tz="UTC")`** schedules a deployed notebook or pipeline
to run on Fabric, returning the schedule id. Fabric's scheduler is interval / daily / weekly, not
free-form cron, so pass one of: `every="30m"` / `"2h"` (interval), `daily="06:00"` or
`daily=["06:00","18:00"]`, or `weekly=["Mon","Fri"], at="06:00"`. No cadence → **daily at midnight**.
It's idempotent — re-scheduling the same item updates its schedule instead of stacking a duplicate.

Authentication reuses the same Fabric control-plane token as [remote execution](remote.md): inside a
Fabric notebook it's automatic (`notebookutils`), and locally it comes from `FABRIC_TOKEN`, GitHub
OIDC, or `az login`. The semantic-model refresh additionally needs a Power BI token (`POWERBI_TOKEN`
or `az login --scope https://analysis.windows.net/powerbi/api/.default`); inside Fabric it's
automatic. Pass `token=` to inject the Fabric one.

Pipelines, TMDL-folder semantic models, and git-folder workspace CD are intentionally out of scope —
use [`fabric-cicd`](https://microsoft.github.io/fabric-cicd/) or
[`semantic-link-labs`](https://github.com/microsoft/semantic-link-labs) for those.
