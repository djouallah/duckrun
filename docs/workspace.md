---
hide:
  - navigation
---

# Fabric workspace handle

`connect()` points *into* an existing lakehouse, and works the same on local, `s3://`, `gs://`, or
OneLake. Creating a lakehouse is the opposite job — Fabric-only, control-plane, and there's no
lakehouse to point at yet — so it lives on a separate **workspace handle**:

```python
import duckrun

ws = duckrun.workspace("My Workspace")          # workspace name or GUID
lh_id = ws.create_lakehouse("bronze")           # returns the lakehouse item id
nb_id = ws.create_notebook("test")              # returns the notebook item id
ws.list_lakehouses()                            # [{"displayName": ..., "id": ...}, ...]
```

`create_lakehouse` is **idempotent**: if a lakehouse of that name already exists it returns its id
unchanged (nothing is re-created), so it's safe to call before every run. Pass `schemas=False` for a
non-schema-enabled lakehouse. It raises on a real API failure rather than returning a sentinel.

`create_notebook(name, source=None, overwrite=False)` creates a Python notebook. `source` is a path
to a local `.ipynb`, an already-parsed nbformat `dict`, or `None` for an empty notebook. Unlike a
lakehouse a notebook has content, so it is **not** idempotent: if one of that name already exists it
is replaced only when `overwrite=True`, otherwise the call raises rather than hide a stale deploy.

Authentication reuses the same Fabric control-plane token as
[remote execution](remote.md): inside a Fabric notebook it's automatic (`notebookutils`), and
locally it comes from `FABRIC_TOKEN`, GitHub OIDC, or `az login`. Pass `token=` to inject one.

Deploying notebooks, pipelines, or semantic models is intentionally out of scope — use
[`fabric-cicd`](https://microsoft.github.io/fabric-cicd/) or
[`semantic-link-labs`](https://github.com/microsoft/semantic-link-labs) for those.
