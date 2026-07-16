---
hide:
  - navigation
---

# Remote execution on Fabric

`RemoteRunner` runs a duckrun **dbt project on Microsoft Fabric compute** instead of on your laptop
or CI runner. It's a drop-in for dbt's own `dbtRunner`: it ships only the dbt *logic* (models,
macros, a token-scrubbed `profiles.yml`) into a **temporary Fabric Python notebook**, runs your dbt
command there, streams the log back, and deletes the notebook when it's done.

Why bother? Running dbt against OneLake from a laptop means every read and write crosses the network,
tokens have to be juggled, and big builds can exhaust local memory. The same project run **on Fabric
compute, co-located with the lakehouse**, is far faster and scales past what your machine can hold —
Fabric has no Livy-for-Python endpoint, so the temp-notebook trick is how you get there.

## Usage

Take the dbt code you already run in-process:

```python
from dbt.cli.main import dbtRunner
os.chdir(dbt_path)
dbt = dbtRunner()
args = ["--target", "fabric", "--profiles-dir", ".", "--target-path", "/tmp/dbt_target"]
run_res  = dbt.invoke(["run",  *args])
test_res = dbt.invoke(["test", *args])
```

and change **one line** to run it on Fabric:

```python
from duckrun import RemoteRunner
os.chdir(dbt_path)
dbt = RemoteRunner(cores=8)              # was: dbt = dbtRunner()
args = ["--target", "fabric", "--profiles-dir", ".", "--target-path", "/tmp/dbt_target"]
run_res  = dbt.invoke(["run",  *args])   # one temp notebook: create -> run -> delete
test_res = dbt.invoke(["test", *args])   # another temp notebook
```

Local vs remote is decided purely by **which runner you construct** — there's no profile flag and no
second target. The project comes from the current directory plus the `--project-dir`/`--profiles-dir`
in the args, exactly like `dbtRunner`.

### One notebook for several commands

Each `.invoke()` above spins its own notebook. To run several commands in a **single** notebook (one
session start, one install), use the `with` form:

```python
with RemoteRunner(cores=8) as dbt:
    build = dbt.invoke(["build", "--target", "fabric"])
    test  = dbt.invoke(["test",  "--target", "fabric"])
# on block exit: ONE notebook runs [build, test] in sequence, then is deleted
print(build.success, test.success)       # results are populated after the block
```

`.invoke()` returns a `RemoteResult` with `.success` (bool) and `.result` (a list of
`{"node", "status"}`). It is a lightweight stand-in for dbt's `dbtRunnerResult`, enough for the
common `res.success` check — the full dbt log tail is printed to your console.

## The `cores` knob

`cores` is the only constructor argument that matters: the **vCores of the Fabric Python notebook**
the job runs on (recommended values `2, 4, 8, 16, 32, 64`; memory scales with it). It's optional —
`RemoteRunner()` uses the workspace default (small), so it stays a true drop-in for `dbtRunner`.
Turn it up for heavier builds:

```python
RemoteRunner(cores=16)   # ~16 vCores / ~128 GB for a large merge
```

## What travels, and what doesn't

- **Embedded in the notebook:** your dbt project — models, macros, seeds, snapshots, and a copy of
  `profiles.yml` with any bearer token **removed**.
- **Never embedded:** your OneLake token. Inside Fabric the notebook authenticates through its own
  runtime (`notebookutils`), so no secret is written into the notebook definition.
- **Never embedded:** external data assets. Source tables, seeds already in the lakehouse, and any
  Delta tables your models read via `source()` stay in OneLake and are read in place.
- **Config env vars are forwarded automatically.** RemoteRunner scans the project for
  `env_var('NAME')` references and forwards those from your current environment into the notebook —
  **except** anything that looks like a secret (names matching `TOKEN`/`SECRET`/`KEY`/`PASSWORD`).

## Authentication

Two different tokens are involved, both acquired on the machine that constructs `RemoteRunner`:

| Token | Scope | Used for |
| --- | --- | --- |
| Fabric control-plane | `https://api.fabric.microsoft.com/.default` | create / run / delete the temp notebook |
| OneLake storage | `https://storage.azure.com/.default` | read the run's log back from OneLake Files |

Inside a Fabric notebook both are automatic. Elsewhere (a laptop or CI):

```bash
az login --scope https://api.fabric.microsoft.com/.default   # or set FABRIC_TOKEN
az login --scope https://storage.azure.com/.default          # or set AZURE_STORAGE_TOKEN
```

You can also pass them explicitly — handy in CI where you mint them with `az account get-access-token`:

```python
RemoteRunner(cores=16,
             fabric_token=os.environ["FABRIC_TOKEN"],
             storage_token=os.environ["ONELAKE_TOKEN"])
```

!!! note "Workspace permission"
    The identity creating the notebook needs **item CRUD** on the workspace (an Admin / Member /
    Contributor role), not just data-write on the lakehouse. A service principal used unattended must
    be added to the workspace with one of those roles.

## How it works

For each run (or once per `with` block):

1. Zip the project and write a token-scrubbed `profiles.yml` into it; base64 it into a generated
   Python notebook (`%%configure` sets the vCores; `pip install duckrun` runs the project).
2. Create the notebook item in the workspace via the Fabric REST API.
3. Start it as an on-demand job and poll to completion.
4. The notebook writes its dbt log + per-node status to a small JSON file in OneLake Files; the runner
   downloads and prints it.
5. Delete the notebook — always, even if the run failed.

The return is a **log and status, never table data**.

## Requirements

- The temp notebook installs `duckrun` from PyPI, so your models run on the released adapter.
- `requests` (a duckrun dependency) for the REST calls; `azure-identity` (the `duckrun[local]` extra)
  only if you rely on `az login` to mint tokens.
