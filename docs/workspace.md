---
hide:
  - navigation
---

# Fabric workspace handle

`connect()` points into an existing lakehouse. Creating one, and deploying the items around it, is Fabric-only control-plane work, so it lives on a separate **workspace handle**:

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
ws.list_items()                                 # [{"displayName":…, "id":…, "type":…}, ...]
```

## `list_items`

`list_items(kind=None)` lists every item with its `type`; pass a collection name (`"notebooks"`, `"semanticModels"`, `"lakehouses"`, `"dataPipelines"`, `"variableLibraries"`) to narrow. `list_lakehouses()` wraps the lakehouse collection; `lakehouse_id(name)` / `warehouse_id(name)` resolve one item's id.

## `create_lakehouse` / `create_warehouse`

`create_lakehouse(name, schemas=True, folder=None)` provisions an empty lakehouse and is **idempotent**: an existing lakehouse of that name returns its id unchanged. `schemas=False` makes a non-schema-enabled lakehouse. `create_warehouse(name, folder=None)` is the warehouse sibling, idempotent too. `folder=` places an item it creates in a workspace folder. `sql_endpoint(warehouse=None)` returns the workspace's SQL endpoint hostname (shared by every warehouse and lakehouse SQL analytics endpoint in the workspace), what a connection string's `Server=` takes.

## `deploy`

`deploy(source, name=None, overwrite=False, …)` pushes a file, an `http(s)` URL, or a **folder** of items. The item type comes from the extension: `.ipynb` → notebook, `.bim` → semantic model, `.json` → data pipeline (`properties.activities`, deployed verbatim) or variable library (`variables`, with an optional `variables=` mapping to set values at deploy time; an unknown name raises). The name defaults to the filename stem. `deploy` is **not** idempotent: an existing item is replaced only with `overwrite=True`, otherwise the call raises. Each item logs `created` or `updated`. A `.ipynb`'s cell sources are normalized to Fabric's list-of-lines form. A `.bim` is refreshed (reframed) after deploy, so `deploy` returns once the model is live; a Direct Lake model reads OneLake with the caller's identity, so there is no gateway or stored credential to bind.

**Pointing a Direct Lake model at a lakehouse.** A `.bim` bakes in the OneLake workspace and lakehouse GUIDs it reads; `lakehouse=` rewrites them. It is inferred when the model already targets a lakehouse in this workspace or the workspace has exactly one; with several you must name it, and a wrong name raises with the available names. Ignored for `.ipynb` / `.json`.

```python
ws.deploy("model.bim", lakehouse="silver")
ws.deploy("model.bim")                        # workspace has one lakehouse → inferred
```

**Storage mode.** `mode=` forces every data table in a `.bim` (or every model in a folder) into one storage mode; omit it and the model deploys as authored. `lakehouse=` / `warehouse=` name the item holding the tables — either serves either mode — and are inferred the same way as above; with both named in a mixed folder, each model takes the one matching what it reads today. `direct_lake` gives each table an entity partition on the item's OneLake root with `directLakeOnly` behavior (no SQL endpoint, no DirectQuery fallback) and reframes after deploy; `direct_query` gives each table an M partition over `Sql.Database(<endpoint>, <item>)`. Calculated tables and calculation groups are left alone; a table that reads through a real M query raises, naming the table, rather than deploying with its transformation dropped.

```python
ws.deploy("model.bim", lakehouse="silver",   mode="direct_lake")
ws.deploy("model.bim", warehouse="gold_dwh", mode="direct_query")
ws.deploy("fabric_items", overwrite=True, mode="direct_lake")       # every model in the folder
```

## Deploying a folder of items

Point `deploy` at a folder in the Fabric git-integration layout — one `name.ItemType/` subfolder per item, each with its `.platform` file:

```
fabric_items/
├── deploy_config.VariableLibrary/    .platform, settings.json, variables.json
├── run.Notebook/                     .platform, notebook-content.ipynb
├── model.SemanticModel/              .platform, definition.pbism, model.bim
└── run_pipeline.DataPipeline/        .platform, pipeline-content.json

ws.deploy("fabric_items", overwrite=True)   # → {"deploy_config": id, "run": id, ...}
```

Items deploy in dependency order (variable libraries, notebooks, semantic models, pipelines), each exactly like its single-file deploy, names taken from `.platform` `displayName`. With exactly one notebook in the folder, a pipeline's notebook activities are pointed at it automatically (several: pick one with `notebook=`). Supported types are VariableLibrary, Notebook (ipynb), SemanticModel and DataPipeline; anything else raises rather than half-deploying. Returns `{displayName: item id}`.

## `download`

`download(folder=".", name=None, overwrite=False)` exports the workspace's items to disk in the same layout — notebooks as ipynb, semantic models as TMSL `model.bim`, each with its `.platform` — so a downloaded folder redeploys unchanged. An existing local item folder is skipped unless `overwrite=True`. Returns `{displayName: folder path}`.

```python
ws.download("fabric_items")                  # every item
ws.download("fabric_items", name="run")      # one item
duckrun.workspace("Prod").deploy("fabric_items", overwrite=True)   # round-trip
```

## `run` / `run_python`

`run(name)` executes a deployed notebook or pipeline on Fabric and waits, returning the terminal job status (raising on failure). `name` is the display name with or without extension; a bare name is looked up as a notebook, then a pipeline. Parameters are a pipeline's job, so `run` takes none. `run_python(script, *, lakehouse=None, cores=None, pip=None, env=None, …)` runs a local `.py` file (or a folder with `entry=`) in a throwaway Fabric notebook, streams its output back, and returns a `ScriptResult` (`success`, `returncode`, `log`, `item_id`); it is what [`RemoteRunner`](remote.md) is built on.

## `schedule`

`schedule(name, every=/daily=/weekly=, at=, tz="UTC")` schedules a deployed notebook or pipeline and returns the schedule id. Fabric's scheduler is interval / daily / weekly: `every="30m"` / `"2h"`, `daily="06:00"` or `daily=["06:00","18:00"]`, or `weekly=["Mon","Fri"], at="06:00"`. No cadence → daily at midnight. Re-scheduling the same item updates its schedule.

## Authentication

The same Fabric control-plane token as [remote execution](remote.md): automatic inside a Fabric notebook (`notebookutils`); locally from `FABRIC_TOKEN`, GitHub OIDC, or `az login`. The semantic-model refresh additionally needs a Power BI token (`POWERBI_TOKEN` or `az login --scope https://analysis.windows.net/powerbi/api/.default`). Pass `token=` to inject the Fabric one.

TMDL-folder semantic models and git-folder workspace CD are out of scope — use [`fabric-cicd`](https://microsoft.github.io/fabric-cicd/) or [`semantic-link-labs`](https://github.com/microsoft/semantic-link-labs).
