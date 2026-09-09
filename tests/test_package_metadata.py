import tomllib
from pathlib import Path


def test_distribution_identity_matches_release() -> None:
    project_root = Path(__file__).resolve().parents[1]
    with (project_root / "pyproject.toml").open("rb") as file:
        project = tomllib.load(file)["project"]

    assert project["name"] == "flowsense-engine"
    assert project["version"] == "0.2.1"
