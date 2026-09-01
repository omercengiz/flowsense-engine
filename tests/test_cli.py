from unittest.mock import patch

from typer.testing import CliRunner

from flowsense.cli.main import app
from flowsense.infrastructure.airflow import AirflowApiError


def test_analyze_reports_airflow_api_errors() -> None:
    error = AirflowApiError(
        method="GET",
        endpoint="/api/v2/dags/demo/dagRuns",
        status_code=503,
    )

    with patch("flowsense.cli.main.AirflowClient") as client_class:
        client_class.return_value.__enter__.side_effect = error
        result = CliRunner().invoke(app, ["analyze", "demo"])

    assert result.exit_code == 1
    assert "Airflow request failed" in result.output
    assert "503" in result.output
