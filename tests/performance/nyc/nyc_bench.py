"""The nyc full-scale benchmark driver — runs on the GitHub runner, builds on Fabric.

Why this exists: the layout benchmark caught duckrun 0.4.54 spending ~49 of a 72-minute
fct_trips build inside the sort-key profiler and landing 21.7M-row row groups — a week after the
change shipped. This job asks the same question of duckrun@HEAD from THIS repo, before a release:
ship tests/performance/nyc/project to a throwaway 8-vcore Fabric Python notebook
(duckrun.run_python — the runner can't hold the build: spill peaks >100 GiB), run
`dbt build --full-refresh` there against the persistent nyc raw archive, then read the landed
geometry back and leave one row in docs/nyc-benchmark-history.md.

What the row records: fct_trips build seconds, the profiler's own cost (the `sort profile of …:
N scans over R rows in Ss` INFO line), landed files / rows-per-row-group / size, and the remote
duckrun/duckdb/deltalake versions. Exit 0 iff the dbt build succeeded and the stats were read
back — so a dispatch doubles as a full-scale write-path guard.

Config: tests/performance/nyc/deploy_config.yml, overridable via env (FILES_PATH, NYC_PIP_SPEC,
DBT_SCHEMA, the DUCKDB_* layout knobs). Auth is duckrun's own OIDC/azure-identity self-acquire —
AZURE_CLIENT_ID / AZURE_TENANT_ID only, no tokens here and none shipped to the notebook.
"""
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import yaml

import duckrun

HERE = Path(__file__).resolve().parent

# Env the shipped project reads via env_var() — forwarded into the notebook when set here.
# DATASET-style silent-inert vars don't exist in this single-tree port; the layout knobs default
# to the benchmark shape (sorted auto, adaptive geometry) inside the model itself.
_FORWARD = ("FILES_PATH", "WAREHOUSE_PATH", "DBT_SCHEMA", "DUCKDB_SORTED", "DUCKDB_SORT_BY",
            "DUCKDB_ROW_GROUP_SIZE", "DUCKDB_FILE_SIZE_MB", "DUCKDB_TEMP_DIR")


def _run_ref() -> str:
    run_id = os.environ.get("GITHUB_RUN_ID")
    if not run_id:
        return "local"
    num = os.environ.get("GITHUB_RUN_NUMBER", run_id)
    url = (f"{os.environ.get('GITHUB_SERVER_URL', 'https://github.com')}/"
           f"{os.environ.get('GITHUB_REPOSITORY', '')}/actions/runs/{run_id}")
    return f"[#{num}]({url})"


def _pip_spec() -> str:
    """What the notebook installs: the exact commit under test, or NYC_PIP_SPEC verbatim."""
    spec = os.environ.get("NYC_PIP_SPEC")
    if spec:
        return spec
    sha = os.environ.get("GITHUB_SHA")
    return f"duckrun @ git+https://github.com/djouallah/duckrun@{sha}" if sha else "duckrun"


def _parse_log(log: str) -> dict:
    """Pull the attribution lines out of the streamed remote log."""
    out = {}
    m = re.search(r"NYC_BENCH_RESULT (\{.*\})", log)
    if m:
        try:
            out["result"] = json.loads(m.group(1))
        except Exception:
            pass
    m = re.search(r"sort profile of [^:]+: (\d+) scans over ([\d,]+)(?: of [\d,]+)? rows "
                  r"in ([\d.]+)s", log)
    if m:
        out["profile"] = {"scans": int(m.group(1)), "rows": int(m.group(2).replace(",", "")),
                          "seconds": float(m.group(3))}
    m = re.search(r"auto geometry landed (\d+) file\(s\), ([\d,]+) rows per row group "
                  r"vs a ([\d,]+) target \(([\d.]+)x\)", log)
    if m:
        out["geometry_log"] = {"files": int(m.group(1)),
                               "rows_per_rg": int(m.group(2).replace(",", "")),
                               "target": int(m.group(3).replace(",", "")),
                               "ratio": float(m.group(4))}
    return out


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    cfg = yaml.safe_load((HERE / "deploy_config.yml").read_text(encoding="utf-8"))
    ws = duckrun.workspace(cfg["workspace"])
    lh_id = ws.lakehouse_id(cfg["lakehouse"])
    warehouse = f"abfss://{ws.id}@onelake.dfs.fabric.microsoft.com/{lh_id}/Tables"
    schema = os.environ.get("DBT_SCHEMA", cfg.get("schema", "nyc"))
    mart = "mart" if schema == "mart" else f"{schema}_mart"

    env = {"WAREHOUSE_PATH": warehouse,
           "FILES_PATH": os.environ.get("FILES_PATH") or cfg["files_path"],
           "DBT_SCHEMA": schema}
    for key in _FORWARD:
        if key in os.environ and key not in env:
            env[key] = os.environ[key]

    sha = (os.environ.get("GITHUB_SHA", "") or "local")[:7]
    print(f"[nyc_bench] building {mart}.fct_trips in {cfg['lakehouse']} "
          f"({cfg['cores']} vcores) from {env['FILES_PATH']} — pip: {_pip_spec()}", flush=True)
    result = ws.run_python(str(HERE / "project"), entry="remote_build.py",
                           lakehouse=lh_id, cores=int(cfg.get("cores", 8)),
                           pip=[_pip_spec()], env=env, name=f"nyc-bench-{sha}")
    parsed = _parse_log(result.log or "")
    remote = parsed.get("result", {})
    versions = remote.get("versions", {})
    profile = parsed.get("profile")
    geom_log = parsed.get("geometry_log")

    stats = None
    if result.success:
        try:
            con = duckrun.connect(warehouse)
            row = con.get_stats(f"{mart}.fct_trips").fetchall()[0]
            cols = ("catalog", "schema", "table", "total_rows", "num_files", "num_row_groups",
                    "avg_row_group", "size_mb", "vorder", "compression")
            stats = dict(zip(cols, row))
        except Exception as exc:
            print(f"[nyc_bench] stats read-back failed: {exc}", flush=True)

    ok = bool(result.success and remote.get("ok") and stats)
    fct_seconds = remote.get("models", {}).get("fct_trips")

    date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    commit = os.environ.get("GITHUB_SHA", "")[:7] or "local"

    def _n(x, fmt="{:,}"):
        return fmt.format(x) if x is not None else "?"

    prof_cell = (f"{profile['scans']} scans / {profile['seconds']:.0f}s" if profile else "?")
    row = (f"| {date} | {_run_ref()} | {commit} | {versions.get('duckrun', '?')} "
           f"| {versions.get('duckdb', '?')} | {versions.get('deltalake', '?')} "
           f"| {_n(stats['total_rows']) if stats else '?'} "
           f"| {_n(fct_seconds, '{:.0f}') if fct_seconds is not None else '?'}s | {prof_cell} "
           f"| {_n(stats['num_files']) if stats else '?'} "
           f"| {_n(int(stats['avg_row_group'])) if stats and stats['avg_row_group'] else '?'} "
           f"| {_n(stats['size_mb'], '{:.0f}') if stats else '?'} "
           f"| {'✅' if ok else '❌'} |\n")
    with open("docs/nyc_history_row.md", "w", encoding="utf-8", newline="\n") as fh:
        fh.write(row)

    lines = [f"## NYC benchmark — {mart}.fct_trips, duckrun {versions.get('duckrun', '?')} "
             f"({'✅ ok' if ok else '❌ failed'})", ""]
    if fct_seconds is not None:
        lines.append(f"- **fct_trips build:** {fct_seconds:.0f}s "
                     f"(whole `dbt build`: {remote.get('build_seconds', '?')}s)")
    if profile:
        lines.append(f"- **sort profile:** {profile['scans']} scans over "
                     f"{profile['rows']:,} rows in {profile['seconds']:.0f}s")
    if geom_log:
        lines.append(f"- **geometry (write log):** {geom_log['files']} files, "
                     f"{geom_log['rows_per_rg']:,} rows/row group vs a "
                     f"{geom_log['target']:,} target ({geom_log['ratio']:.2f}x)")
    if stats:
        lines.append(f"- **landed:** {stats['total_rows']:,} rows, {stats['num_files']} files, "
                     f"{stats['num_row_groups']} row groups, avg "
                     f"{int(stats['avg_row_group'] or 0):,} rows/group, {stats['size_mb']:.0f} MB "
                     f"({stats['compression']})")
    lines.append(f"- notebook item: `{result.item_id}` · versions: duckdb "
                 f"{versions.get('duckdb', '?')}, deltalake {versions.get('deltalake', '?')}")
    card = "\n".join(lines) + "\n"
    print(card)
    with open("docs/nyc_card.md", "w", encoding="utf-8", newline="\n") as fh:
        fh.write(card)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as fh:
            fh.write(card)
    return 0 if ok else 1


if __name__ == "__main__":
    _code = main()
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(_code)   # delta-rs/duckdb native runtimes can abort interpreter shutdown on Linux
