# deploy — deploy a dbt project's Fabric items via `duckrun.workspace(...)`

Manually-triggered (`workflow_dispatch`) end-to-end tests of the `duckrun.workspace(...)` deploy
surface — deploy notebooks / semantic models / variable libraries / pipelines and schedule them, all
by name, no `fab` CLI. Two projects, each in its own folder:

- **[dbt_fabric_python_delta/](dbt_fabric_python_delta/) — the real-world one the workflow runs.**
  Parity-style (like [tests/parity_tests/](../parity_tests/)): clone
  [dbt_fabric_python_delta](https://github.com/djouallah/dbt_fabric_python_delta) fresh at runtime
  (nothing vendored), build its AEMO dbt project on the runner via duckrun, then deploy its Fabric
  items. That repo ships its own `fab`-CLI `deploy.py`; this expresses the same deploy through duckrun.
- **[coffee/](coffee/) — a self-contained example.** Hand-authored assets, no external clone; a
  minimal illustration of the same API. Not run by the workflow.

Both write a shared Fabric workspace, so [the workflow](../../.github/workflows/deploy_to_fabric.yml) is manual
and serialized, and authenticates via OIDC only (`AZURE_CLIENT_ID`/`AZURE_TENANT_ID`) — duckrun mints
every token it needs (storage, Fabric control plane, Power BI) itself.
