import subprocess
import sys
from pathlib import Path


def test_production_batch_example_exposes_help_without_airflow() -> None:
    project_root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [
            sys.executable,
            str(project_root / "examples/python/production_batch.py"),
            "--help",
        ],
        cwd=project_root,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert "--dag-limit" in result.stdout
    assert "--max-concurrency" in result.stdout
