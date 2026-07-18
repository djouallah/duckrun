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
# ... build the cloned repo's dbt project on the runner via duckrun (→ OneLake Delta) ...
ws.deploy(".../run.Notebook/notebook-content.ipynb", name="run", overwrite=True)
ws.deploy(".../deploy_config.VariableLibrary/variables.json", name="deploy_config",
          variables={"workspace_id": ws.id, "lakehouse_name": "data"}, overwrite=True)
ws.deploy(".../aemo_electricity.SemanticModel/model.bim", name="aemo_electricity",
          lakehouse="data", overwrite=True)   # repoint OneLake GUIDs + refresh
ws.schedule("run", daily="06:00")
```

## Notes

- **The dbt build runs on the runner** via duckrun (not on Fabric), so the semantic model has tables
  to reframe onto before it refreshes. The injected [profiles.yml](profiles.yml) (name
  `aemo_electricity`, no token → OIDC self-acquire) is passed via `--profiles-dir`; the cloned repo is
  never modified.
- **The DataPipeline is skipped.** It references the notebook by GUID and `ws.deploy` writes pipeline
  JSON verbatim (no notebook-ref repoint), so the notebook is scheduled directly instead.
- **CI-only** (needs Fabric + OIDC): **Actions → deploy_to_fabric → Run workflow**.
