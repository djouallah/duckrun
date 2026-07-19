"""Deploy the whole thing to Fabric from one small script.

Stages the coffee dbt project into the lakehouse Files, then deploys the entire ``fabric_items/``
folder in one call — the notebook that runs dbt on Fabric, a Direct Lake semantic model over the
built tables, a pipeline, and a variable library — runs the notebook and schedules the pipeline.
Nothing about auth is passed here or set up by CI: given only the OIDC federated identity
(`AZURE_CLIENT_ID`/`AZURE_TENANT_ID`), duckrun mints every token it needs — storage (the Files
copy), the Fabric control plane, and Power BI (the model refresh) — by exchanging a fresh GitHub
OIDC assertion, and picks the OneLake HTTP transport itself. That self-sufficiency is the point of
this test.
"""
import os

import duckrun

ws = duckrun.workspace(os.environ["FABRIC_WORKSPACE"])
lh = ws.create_lakehouse("deploy_demo")

# stage the coffee dbt project into the lakehouse Files, then let a notebook run it ON Fabric
files = duckrun.connect(f"abfss://{ws.id}@onelake.dfs.fabric.microsoft.com/{lh}/Tables")
files.copy("../../integration_tests/coffee", "coffee")

# one call deploys everything in fabric_items/ in dependency order: the variable library (values
# injected), the notebook, the Direct Lake model (repointed at deploy_demo + refreshed), and the
# pipeline. overwrite=True keeps the demo re-runnable against the shared workspace (items from a
# prior run are replaced, not errored) — and its tables let the model refresh succeed pre-run.
ws.deploy("fabric_items", lakehouse="deploy_demo",
          variables={"workspace_id": ws.id, "lakehouse_name": "deploy_demo"}, overwrite=True)
ws.run("build_coffee")                       # runs the dbt project on Fabric → dbo.mart_revenue
ws.schedule("pipeline", daily="06:00")
