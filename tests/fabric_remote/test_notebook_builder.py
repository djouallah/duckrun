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
        "PROJ",
    )
    assert got == ["run", "--select", "foo", "--target", "fabric"]


def test_build_notebook_embeds_project_commands_and_cores():
    b64 = base64.b64encode(b"zip-bytes").decode("ascii")
    commands = [["run", "--target", "fabric"], ["test", "--target", "fabric"]]
    nb = fr.build_notebook("run123", b64, commands, "abfss://x/Files/r.json", "0.4.17", 8)

    assert nb["nbformat"] == 4
    assert nb["metadata"]["duckrun"]["cores"] == 8
    assert nb["metadata"]["duckrun"]["runid"] == "run123"

    src = "".join(cell_src for cell in nb["cells"] for cell_src in cell["source"])
    assert b64 in src                       # project payload embedded
    assert "abfss://x/Files/r.json" in src  # result path
    assert "duckrun==0.4.17" in src         # pinned install
    assert json.dumps(commands) in src      # commands embedded verbatim
    assert "notebookutils.notebook.exit" in src


def test_build_notebook_no_version_uses_bare_install():
    nb = fr.build_notebook("r", "eA==", [["run"]], "abfss://x/Files/r.json", None, None)
    src = "".join(s for cell in nb["cells"] for s in cell["source"])
    assert "'duckrun'" in src  # no ==version pin
    assert nb["metadata"]["duckrun"]["cores"] is None
