# coffee — a self-contained deploy example

A minimal illustration of the `duckrun.workspace(...)` deploy surface with hand-authored assets and no
external clone. Kept as a reference; **not run by the workflow** (that runs
[../dbt_fabric_python_delta/](../dbt_fabric_python_delta/)).

## The whole thing ([deploy.py](deploy.py))

```python
import os, duckrun

ws = duckrun.workspace(os.environ["FABRIC_WORKSPACE"])
lh = ws.create_lakehouse("deploy_demo")

files = duckrun.connect(f"abfss://{ws.id}@onelake.dfs.fabric.microsoft.com/{lh}/Tables")
files.copy("../../integration_tests/coffee", "coffee")   # stage the coffee dbt project → Files/coffee

ws.deploy("fabric_items", lakehouse="deploy_demo",       # one call deploys every item in the folder
          variables={"workspace_id": ws.id, "lakehouse_name": "deploy_demo"}, overwrite=True)
ws.run("build_coffee")                        # runs the dbt project ON Fabric → dbo.mart_revenue
ws.schedule("pipeline", daily="06:00")
```

## The flow

1. **create** a fresh `deploy_demo` lakehouse.
2. **copy** the existing coffee dbt project (`tests/integration_tests/coffee/`) into `Files/coffee`
   with `conn.copy` (a `/Tables` connection maps to `/Files` automatically).
3. **deploy the folder** — [fabric_items/](fabric_items/) is the Fabric git-integration layout, one
   `name.ItemType/` subfolder per item, deployed in dependency order by a single `ws.deploy`:

| Item folder | Deploys to | Note |
| --- | --- | --- |
| `variables.VariableLibrary/` | a variable library | `workspace_id` / `lakehouse_name` injected at deploy time via `variables=` |
| `build_coffee.Notebook/` | a notebook | `pip install`s duckrun, pulls `Files/coffee` down, runs `dbt build` on Fabric |
| `coffee_revenue.SemanticModel/` | a Direct Lake semantic model | over `dbo.mart_revenue`; `lakehouse=` rewrites its OneLake GUIDs, then it refreshes |
| `pipeline.DataPipeline/` | a data pipeline | a single `Wait` activity — self-contained, `schedule`d daily |

4. **run** the notebook, materializing `dbo.mart_revenue` etc.
5. **schedule** the pipeline daily.

The model deploy refreshes (reframes) before the notebook run, so it rides on the tables a prior
run left in the shared workspace — on a brand-new workspace, run the notebook once before the
first folder deploy (or let the refresh retry window cover it).
