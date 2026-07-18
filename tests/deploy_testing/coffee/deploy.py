"""Deploy the whole thing to Fabric from one small script.

Stages the coffee dbt project into the lakehouse Files, deploys a notebook that runs it on Fabric
(producing `dbo.mart_revenue` etc.), then deploys a Direct Lake semantic model over those tables plus
a pipeline and a variable library, and schedules the pipeline. Nothing about auth is passed here or
set up by CI: given only the OIDC federated identity (`AZURE_CLIENT_ID`/`AZURE_TENANT_ID`), duckrun
mints every token it needs — storage (the Files copy), the Fabric control plane, and Power BI (the
model refresh) — by exchanging a fresh GitHub OIDC assertion, and picks the OneLake HTTP transport
itself. That self-sufficiency is the point of this test.
"""
import os

import duckrun

ws = duckrun.workspace(os.environ["FABRIC_WORKSPACE"])
lh = ws.create_lakehouse("deploy_demo")

# stage the coffee dbt project into the lakehouse Files, then let a notebook run it ON Fabric
files = duckrun.connect(f"abfss://{ws.id}@onelake.dfs.fabric.microsoft.com/{lh}/Tables")
files.copy("../../integration_tests/coffee", "coffee")
ws.deploy("build_coffee.ipynb", overwrite=True)
ws.run("build_coffee")                       # runs the dbt project on Fabric → dbo.mart_revenue

# the tables exist now — deploy the semantic model over them, plus orchestration. overwrite=True keeps
# the demo re-runnable against the shared workspace (items from a prior run are replaced, not errored).
ws.deploy("coffee_revenue.bim", lakehouse="deploy_demo", overwrite=True)
ws.deploy("pipeline.json", overwrite=True)
ws.deploy("variables.json", variables={"workspace_id": ws.id, "lakehouse_name": "deploy_demo"}, overwrite=True)
ws.schedule("pipeline", daily="06:00")
