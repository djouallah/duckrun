# deploy — the whole Fabric workspace from one tiny script

A manually-triggered (`workflow_dispatch`) end-to-end demo of the `duckrun.workspace(...)` deploy
surface: build a **real dbt project** into a fresh lakehouse, then deploy a **Direct Lake semantic
model + a notebook + a pipeline + a variable library** over it, run the notebook, and schedule the
pipeline — all by name, no `fab` CLI.

## The whole deploy ([deploy.py](deploy.py))

```python
import os, duckrun

ws = duckrun.workspace(os.environ["FABRIC_WORKSPACE"])
ws.deploy("hello.ipynb")
ws.deploy("pipeline.json")
ws.deploy("variables.json", variables={"workspace_id": ws.id, "lakehouse_name": "deploy_demo"})
ws.deploy("coffee_revenue.bim", lakehouse="deploy_demo")   # Direct Lake — repointed + refreshed
ws.run("hello")
ws.schedule("pipeline", daily="06:00")
```

`deploy()` keys the item type off the extension (`.ipynb` → notebook, `.json` → pipeline **or**
variable library by content, `.bim` → semantic model). Names are inferred from the filenames.

## The pieces

| File | Deploys to | Note |
| --- | --- | --- |
| [hello.ipynb](hello.ipynb) | a notebook | one Python cell; `ws.run("hello")` executes it on Fabric |
| [pipeline.json](pipeline.json) | a data pipeline | a single `Wait` activity, so it runs green on its own |
| [variables.json](variables.json) | a variable library | `workspace_id` / `lakehouse_name` are injected at deploy time via `variables=` |
| [coffee_revenue.bim](coffee_revenue.bim) | a Direct Lake semantic model | over the coffee `mart_revenue` table; `lakehouse=` rewrites its OneLake GUIDs, then it refreshes |

## How the model gets data

The workflow builds the existing **coffee** dbt project (`tests/integration_tests/coffee/`) into a
fresh `deploy_demo` lakehouse first, so `dbo.mart_revenue` really exists when the Direct Lake model
deploys and reframes over it.

## Running it

CI-only, on demand: **Actions → deploy → Run workflow**. It authenticates to Fabric via OIDC (service
principal), mints the OneLake / Fabric / Power BI tokens, builds coffee, then runs `deploy.py`.

Idea for a richer next step: have `pipeline.json` orchestrate `hello` (a notebook activity) and read
its target from the variable library — then the whole thing is environment-agnostic and self-wiring.
