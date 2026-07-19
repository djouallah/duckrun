import os
import duckrun
ws = duckrun.workspace(os.environ["FABRIC_WORKSPACE"])
lh = ws.create_lakehouse("deploy_demo")
files = duckrun.connect(f"abfss://{ws.id}@onelake.dfs.fabric.microsoft.com/{lh}/Tables")
files.copy("../../integration_tests/coffee", "coffee")
ws.deploy("fabric_items", lakehouse="deploy_demo", variables={"workspace_id": ws.id, "lakehouse_name": "deploy_demo"}, overwrite=True)
ws.run("build_coffee")                       # runs the dbt project on Fabric → dbo.mart_revenue
ws.schedule("pipeline", daily="06:00")