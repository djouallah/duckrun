# dbt_fabric_python_delta — deploy a real-world Fabric project via duckrun

Parity-style (like [tests/parity_tests/](../../parity_tests/)): the project under test —
[dbt_fabric_python_delta](https://github.com/djouallah/dbt_fabric_python_delta) (the AEMO electricity
pipeline: a full dbt project plus a real SemanticModel / Notebook / VariableLibrary in Fabric
source-control format) — is `git clone`d fresh at runtime and **nothing from it is vendored here**.
That repo ships its own `fab`-CLI `deploy.py`; [run_deploy.py](run_deploy.py) expresses the same
deploy entirely through `duckrun.workspace(...)`.

```python
ws = duckrun.workspace(os.environ["FABRIC_WORKSPACE"])
lh_id = ws.create_lakehouse("data")   # created if it doesn't already exist (idempotent)
duckrun.connect(tables).copy(dbt_dir, "dbt")   # stage the project → Files/dbt for the in-Fabric notebook
# ... build the cloned repo's dbt project on the runner via duckrun (→ OneLake Delta) ...
ws.deploy(".../run.Notebook/notebook-content.ipynb", name="run", overwrite=True)
ws.deploy(".../deploy_config.VariableLibrary/variables.json", name="deploy_config",
          variables={"workspace_id": ws.id, "lakehouse_name": "data"}, overwrite=True)
ws.deploy(".../aemo_electricity.SemanticModel/model.bim", name="aemo_electricity",
          lakehouse="data", overwrite=True)   # repoint OneLake GUIDs + refresh
ws.deploy(".../run_pipeline.DataPipeline/pipeline-content.json", name="run_pipeline",
          notebook="run", overwrite=True)     # point its notebook activities at 'run' by name
ws.schedule("run_pipeline", daily="06:00")
```

## Notes

- **The dbt build runs on the runner** via duckrun (not on Fabric), so the semantic model has tables
  to reframe onto before it refreshes. The injected [profiles.yml](profiles.yml) (name
  `aemo_electricity`, no token → OIDC self-acquire) is passed via `--profiles-dir`; the cloned repo is
  never modified.
- **The dbt project is also staged to `Files/dbt`** (with the repo's own `profiles.yml`). The deployed
  `run` notebook chdir's to its mounted `Files/dbt` and runs dbt there, so the scheduled pipeline can
  rebuild ON Fabric compute — the runner build is just the first load. Copied before the runner build
  so build artifacts aren't staged.
- **The DataPipeline references its notebook by name.** Its JSON carries the source workspace/notebook
  GUIDs; `ws.deploy(..., notebook="run")` repoints every notebook activity at the deployed `run`
  notebook (name → id), the same way `lakehouse=` repoints the Direct Lake model. Deploy the notebook
  first. The pipeline (not the notebook) is then scheduled.
- **CI-only** (needs Fabric + OIDC): **Actions → deploy_to_fabric → Run workflow**.
