"""The simplest Fabric deploy script — build nothing here, just push artifacts by name.

Run from this folder after the lakehouse is populated (the workflow builds the coffee dbt project
into `deploy_demo` first). Tokens come from the environment: `FABRIC_TOKEN` (control plane) and
`POWERBI_TOKEN` (the semantic-model refresh) are auto-resolved by duckrun — nothing is passed here.
"""
import os

import duckrun

ws = duckrun.workspace(os.environ["FABRIC_WORKSPACE"])

ws.deploy("hello.ipynb")
ws.deploy("pipeline.json")
ws.deploy("variables.json", variables={"workspace_id": ws.id, "lakehouse_name": "deploy_demo"})
ws.deploy("coffee_revenue.bim", lakehouse="deploy_demo")   # Direct Lake — repointed + refreshed

ws.run("hello")
ws.schedule("pipeline", daily="06:00")
