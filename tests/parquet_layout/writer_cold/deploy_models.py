"""Deploy one fresh Direct Lake model per writer variant, each pointed at its own temp lakehouse.

Freshness is the point: the model must not have existed before this run, because a new dataset is
the only reliable way to get an empty column store — that is what makes the first query genuinely
cold. `provision.py drop` deletes them at the end of every run, so a deploy here always creates.

One `.bim` template serves both variants; the only substitution is the partition's `entityName`,
which is the physical Delta table each model reads. Everything else — logical table name, columns,
the DAX suite that runs against them — is identical, so any difference in cold time is the parquet.

deploy() itself rewrites the template's placeholder OneLake GUIDs to (this workspace, the named
lakehouse) and then reframes the model, retrying for several minutes because a brand-new model's
OneLake read permission takes ~5 min to propagate.

Env: WS_ID, LH_NAME_DELTARS / LH_NAME_DUCKDB, MODEL_DELTARS / MODEL_DUCKDB, TABLE_DELTARS /
TABLE_DUCKDB (the physical Delta table names).
"""
import json
import os
import tempfile

import duckrun

HERE = os.path.dirname(os.path.abspath(__file__))
TEMPLATE = os.path.join(HERE, "fct_writer.SemanticModel", "model.bim")

VARIANTS = [
    {"key": "deltars",
     "model": os.environ.get("MODEL_DELTARS") or "writer_cold_deltars",
     "lakehouse": os.environ.get("LH_NAME_DELTARS") or "wab_deltars",
     "table": os.environ.get("TABLE_DELTARS") or "fct_summary_deltars"},
    {"key": "duckdb",
     "model": os.environ.get("MODEL_DUCKDB") or "writer_cold_duckdb",
     "lakehouse": os.environ.get("LH_NAME_DUCKDB") or "wab_duckdb",
     "table": os.environ.get("TABLE_DUCKDB") or "fct_summary_duckdb"},
]


ARMS = [a.strip() for a in (os.environ.get("ARMS") or "deltars,duckdb").split(",") if a.strip()]
# The .bim declares every fact column. When OPT_COLUMNS narrowed the physical table, the model must
# be narrowed to match or the Direct Lake partition fails to bind to columns that are not there.
MODEL_COLUMNS = [c.strip() for c in (os.environ.get("MODEL_COLUMNS") or "").split(",") if c.strip()]


def _narrow(bim):
    if not MODEL_COLUMNS:
        return bim
    doc = json.loads(bim)
    for t in doc["model"]["tables"]:
        kept = [c for c in t["columns"] if c["name"] in MODEL_COLUMNS]
        if not kept:
            raise SystemExit(f"deploy_models: MODEL_COLUMNS {MODEL_COLUMNS} matched no column in "
                             f"{[c['name'] for c in t['columns']]}")
        t["columns"] = kept
    return json.dumps(doc, indent=2)


def main():
    raw = open(TEMPLATE, encoding="utf-8").read()
    if "__FACT_TABLE__" not in raw:
        raise SystemExit("model.bim template lost its __FACT_TABLE__ placeholder")
    # Guard the setting the whole measurement depends on: without directLakeOnly, Fabric may
    # silently serve a query via DirectQuery, which measures the SQL endpoint instead of a transcode
    # and would look like a suspiciously fast cold number.
    if json.loads(raw.replace("__FACT_TABLE__", "x"))["model"].get(
            "directLakeBehavior") != "directLakeOnly":
        raise SystemExit("model.bim must set directLakeBehavior=directLakeOnly — otherwise a cold "
                         "query can fall back to DirectQuery and measure nothing.")

    ws = duckrun.workspace(os.environ["WS_ID"])
    out = {}
    for v in (v for v in VARIANTS if v["key"] in ARMS):
        bim = _narrow(raw.replace("__FACT_TABLE__", v["table"]))
        path = os.path.join(tempfile.mkdtemp(prefix=f"bim_{v['key']}_"), "model.bim")
        with open(path, "w", encoding="utf-8") as f:
            f.write(bim)
        print(f"deploying {v['model']} -> {v['lakehouse']}.tests.{v['table']}", flush=True)
        ws.deploy(path, lakehouse=v["lakehouse"], name=v["model"], overwrite=True)
        item = next((i for i in ws.list_items("semanticModels")
                     if i.get("displayName") == v["model"]), None)
        if item is None:
            raise SystemExit(f"deployed {v['model']} but it is not in the workspace listing")
        out[f"MODEL_ID_{v['key'].upper()}"] = item["id"]
        print(f"  {v['model']} -> {item['id']}", flush=True)

    gh = os.environ.get("GITHUB_ENV")
    if gh:
        with open(gh, "a", encoding="utf-8") as f:
            f.write("".join(f"{k}={v}\n" for k, v in out.items()))
    for k, v in out.items():
        print(f"{k}={v}")


if __name__ == "__main__":
    main()
