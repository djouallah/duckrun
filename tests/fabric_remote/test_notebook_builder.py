"""Pure (no-network) unit tests for the RemoteRunner packaging + notebook-building seam.

These pin the parts that run on the laptop before any Fabric call: project zipping, token
scrubbing, profile resolution, OneLake path parsing, and the generated notebook body.
"""
import base64
import io
import json
import zipfile

import pytest

from duckrun import fabric_remote as fr


ROOT_PATH = "abfss://Analytics@onelake.dfs.fabric.microsoft.com/Sales.Lakehouse/Tables"


@pytest.fixture
def project(tmp_path):
    """A minimal dbt project with a token-bearing profiles.yml, in tmp_path."""
    (tmp_path / "dbt_project.yml").write_text(
        "name: demo\nprofile: demo\nversion: '1.0'\n", encoding="utf-8"
    )
    (tmp_path / "profiles.yml").write_text(
        "demo:\n"
        "  target: fabric\n"
        "  outputs:\n"
        "    fabric:\n"
        "      type: duckrun\n"
        f"      root_path: \"{ROOT_PATH}\"\n"
        "      storage_options:\n"
        "        bearer_token: SECRET-LAPTOP-TOKEN\n"
        "        account_name: keepme\n",
        encoding="utf-8",
    )
    models = tmp_path / "models"
    models.mkdir()
    (models / "my_model.sql").write_text("select 1 as id\n", encoding="utf-8")
    # build cruft that must NOT travel
    (tmp_path / "target").mkdir()
    (tmp_path / "target" / "graph.gpickle").write_text("junk", encoding="utf-8")
    return tmp_path


def test_onelake_parts():
    ws, lh, host, files = fr.onelake_parts(ROOT_PATH)
    assert ws == "Analytics"
    assert lh == "Sales.Lakehouse"
    assert host == "onelake.dfs.fabric.microsoft.com"
    assert files == "abfss://Analytics@onelake.dfs.fabric.microsoft.com/Sales.Lakehouse/Files"


def test_onelake_parts_rejects_non_abfss():
    with pytest.raises(fr.RemoteRunError):
        fr.onelake_parts("s3://bucket/tables")


def test_resolve_target_picks_output_and_renders_env_var(project, monkeypatch):
    out = fr.resolve_target(str(project), str(project), target=None)
    assert out["type"] == "duckrun"
    assert out["root_path"] == ROOT_PATH

    monkeypatch.setenv("RP", ROOT_PATH)
    (project / "profiles.yml").write_text(
        "demo:\n  target: fabric\n  outputs:\n    fabric:\n"
        "      type: duckrun\n      root_path: \"{{ env_var('RP') }}\"\n",
        encoding="utf-8",
    )
    out2 = fr.resolve_target(str(project), str(project), target=None)
    assert out2["root_path"] == ROOT_PATH


def test_zip_strips_token_and_cruft_keeps_project(project):
    data = fr.zip_project(str(project), str(project))
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        names = set(zf.namelist())
        profiles = zf.read("profiles.yml").decode("utf-8")

    assert "models/my_model.sql" in names
    assert "dbt_project.yml" in names
    # build cruft excluded
    assert not any(n.startswith("target/") for n in names)
    # laptop token scrubbed, non-token storage option preserved, root_path preserved
    assert "SECRET-LAPTOP-TOKEN" not in profiles
    assert "bearer_token" not in profiles
    assert "keepme" in profiles
    assert ROOT_PATH in profiles


def test_normalize_command_drops_dir_flags():
    got = fr._normalize_command(
        ["run", "--select", "foo", "--project-dir", "/x", "--profiles-dir", ".", "--target", "fabric"],
    )
    assert got == ["run", "--select", "foo", "--target", "fabric"]


def test_build_notebook_embeds_project_commands_and_cores():
    b64 = base64.b64encode(b"zip-bytes").decode("ascii")
    commands = [["run", "--target", "fabric"], ["test", "--target", "fabric"]]
    nb = fr.build_notebook("run123", b64, commands, "abfss://x/Files/r.json", "duckrun==0.4.17", None, 8)

    assert nb["nbformat"] == 4
    assert nb["metadata"]["duckrun"]["cores"] == 8
    assert nb["metadata"]["duckrun"]["runid"] == "run123"

    src = "".join(cell_src for cell in nb["cells"] for cell_src in cell["source"])
    assert b64 in src                       # project payload embedded
    assert "abfss://x/Files/r.json" in src  # result path
    assert "duckrun==0.4.17" in src         # pinned install
    assert json.dumps(commands) in src      # commands embedded verbatim
    assert "notebookutils.notebook.exit" in src


def test_build_notebook_is_python_notebook_not_pyspark():
    # The metadata Fabric keys off to treat the item as a pure Python notebook (not PySpark) —
    # kernel_info.name / kernelspec.name == "jupyter" (Spark uses "synapse_pyspark").
    nb = fr.build_notebook("r", "eA==", [["run"]], "abfss://x/Files/r.json", "duckrun", None, 8)
    md = nb["metadata"]
    assert md["kernel_info"]["name"] == "jupyter"
    assert md["kernelspec"]["name"] == "jupyter"
    assert md["microsoft"]["language_group"] == "jupyter_python"
    assert md["language_info"]["name"] == "python"
    # every cell carries the language marker too
    assert all(c["metadata"]["microsoft"]["language_group"] == "jupyter_python" for c in nb["cells"])


def test_build_notebook_cores_configure_cell_and_restart():
    nb = fr.build_notebook("r", "eA==", [["run"]], "abfss://x/Files/r.json", "duckrun", None, 8)
    first = "".join(nb["cells"][0]["source"])
    assert first.startswith("%%configure")
    assert '"vCores": 8' in first
    src = "".join(s for cell in nb["cells"] for s in cell["source"])
    assert "notebookutils.session.restartPython()" in src  # supported restart in a Python notebook


def test_build_notebook_no_cores_omits_configure_cell():
    nb = fr.build_notebook("r", "eA==", [["run"]], "abfss://x/Files/r.json", "duckrun", None, None)
    assert not "".join(nb["cells"][0]["source"]).startswith("%%configure")


def test_build_notebook_pip_spec_branch_install():
    spec = "git+https://github.com/djouallah/duckrun@abc123"
    nb = fr.build_notebook("r", "eA==", [["run"]], "abfss://x/Files/r.json", spec, None, None)
    src = "".join(s for cell in nb["cells"] for s in cell["source"])
    assert spec in src           # branch install used verbatim
    assert "duckrun==" not in src


def test_build_notebook_exports_forwarded_env():
    env = {"WAREHOUSE_PATH": "abfss://w@h/L/Tables", "download_limit": "5"}
    nb = fr.build_notebook("r", "eA==", [["run"]], "abfss://x/Files/r.json", "duckrun", env, None)
    src = "".join(s for cell in nb["cells"] for s in cell["source"])
    # the env is emitted so os.environ is set before dbt runs
    assert json.dumps(env) in src
    assert "os.environ[_k] = _v" in src


def test_resolve_env_forwards_config_excludes_secrets(project, monkeypatch):
    # aemo-style: profiles.yml references WAREHOUSE_PATH + ONELAKE_TOKEN via env_var
    (project / "profiles.yml").write_text(
        "demo:\n  target: fabric\n  outputs:\n    fabric:\n      type: duckrun\n"
        f"      root_path: \"{{{{ env_var('WAREHOUSE_PATH') }}}}\"\n"
        "      storage_options:\n"
        "        bearer_token: \"{{ env_var('ONELAKE_TOKEN', '') }}\"\n",
        encoding="utf-8",
    )
    (project / "models" / "m.sql").write_text(
        "-- {{ env_var('download_limit', '2') }}\nselect 1 as id\n", encoding="utf-8")
    monkeypatch.setenv("WAREHOUSE_PATH", "abfss://w@h/L/Tables")
    monkeypatch.setenv("ONELAKE_TOKEN", "SECRET")
    monkeypatch.setenv("download_limit", "9")

    runner = fr.RemoteRunner(cores=8, project_dir=str(project))
    env = runner._resolve_env(str(project))
    assert env == {"WAREHOUSE_PATH": "abfss://w@h/L/Tables", "download_limit": "9"}
    assert "ONELAKE_TOKEN" not in env  # secret excluded, even though referenced


def test_onelake_shorthand_resolved_and_forwarded_expanded(project, monkeypatch):
    """A profile/env spelled with the OneLake `<ws>/<item>` shorthand must reach RemoteRunner as a
    full abfss:// URL: onelake_parts() only understands that form, and the notebook installs duckrun
    from PyPI, which may predate the shorthand — so the value has to travel expanded."""
    (project / "profiles.yml").write_text(
        "demo:\n  target: fabric\n  outputs:\n    fabric:\n      type: duckrun\n"
        f"      root_path: \"{{{{ env_var('WAREHOUSE_PATH') }}}}\"\n",
        encoding="utf-8",
    )
    ws, lh = "11111111-1111-1111-1111-111111111111", "22222222-2222-2222-2222-222222222222"
    monkeypatch.setenv("WAREHOUSE_PATH", f"{ws}/{lh}")

    cfg = fr.resolve_target(str(project), str(project), None)
    expected = f"abfss://{ws}@onelake.dfs.fabric.microsoft.com/{lh}/Tables"
    assert cfg["root_path"] == expected
    workspace, lakehouse, _host, files_base = fr.onelake_parts(cfg["root_path"])
    assert (workspace, lakehouse) == (ws, lh)
    assert files_base.endswith(f"/{lh}/Files")
    # …and the value forwarded into the notebook env is expanded too, not the shorthand.
    runner = fr.RemoteRunner(cores=8, project_dir=str(project))
    assert runner._resolve_env(str(project))["WAREHOUSE_PATH"] == expected


def test_scan_env_var_names(project):
    (project / "models" / "m.sql").write_text(
        "select '{{ env_var(\"FOO\") }}' as a, '{{ env_var('BAR', 'x') }}' as b\n", encoding="utf-8")
    names = fr._scan_env_var_names(str(project))
    assert {"FOO", "BAR"} <= names
