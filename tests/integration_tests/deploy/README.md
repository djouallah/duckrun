# deploy — build a dbt project on Fabric, then deploy everything over it

A manually-triggered (`workflow_dispatch`) end-to-end demo of the `duckrun.workspace(...)` deploy
surface. It stages a **real dbt project** into a lakehouse, deploys a **notebook that runs it on
Fabric compute**, then deploys a **Direct Lake semantic model + a pipeline + a variable library** over
the built tables, runs the notebook, and schedules the pipeline — all by name, no `fab` CLI.

## The whole thing ([deploy.py](deploy.py))

```python
import os, duckrun

ws = duckrun.workspace(os.environ["FABRIC_WORKSPACE"])
lh = ws.create_lakehouse("deploy_demo")

files = duckrun.connect(f"abfss://{ws.id}@onelake.dfs.fabric.microsoft.com/{lh}/Tables")
files.copy("../coffee", "coffee")            # stage the coffee dbt project → Files/coffee
ws.deploy("build_coffee.ipynb")
ws.run("build_coffee")                        # runs the dbt project ON Fabric → dbo.mart_revenue

ws.deploy("coffee_revenue.bim", lakehouse="deploy_demo")   # Direct Lake model over the built tables
ws.deploy("pipeline.json")
ws.deploy("variables.json", variables={"workspace_id": ws.id, "lakehouse_name": "deploy_demo"})
ws.schedule("pipeline", daily="06:00")
```

## The flow

1. **create** a fresh `deploy_demo` lakehouse.
2. **copy** the existing coffee dbt project (`tests/integration_tests/coffee/`) into `Files/coffee`
   with `conn.copy` (a `/Tables` connection maps to `/Files` automatically).
3. **deploy + run** [build_coffee.ipynb](build_coffee.ipynb) — a notebook that `pip install`s duckrun,
   pulls `Files/coffee` down, and runs `dbt build` on Fabric, materializing `dbo.mart_revenue` etc.
4. **deploy** the rest over those tables:

| File | Deploys to | Note |
| --- | --- | --- |
| [coffee_revenue.bim](coffee_revenue.bim) | a Direct Lake semantic model | over `dbo.mart_revenue`; `lakehouse=` rewrites its OneLake GUIDs, then it refreshes |
| [pipeline.json](pipeline.json) | a data pipeline | a single `Wait` activity — self-contained, `schedule`d daily |
| [variables.json](variables.json) | a variable library | `workspace_id` / `lakehouse_name` injected at deploy time via `variables=` |

5. **schedule** the pipeline daily.

## Running it

CI-only, on demand: **Actions → deploy → Run workflow**. It authenticates to Fabric via OIDC (service
principal), mints the storage / Fabric / Power BI tokens, and runs `deploy.py`. The dbt build itself
runs on **Fabric** (inside `build_coffee`), not on the runner.

Idea for a richer next step: have `pipeline.json` orchestrate `build_coffee` (a notebook activity) and
read its target from the variable library — then the whole thing is environment-agnostic and self-wiring.
