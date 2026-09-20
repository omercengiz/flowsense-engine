from unittest.mock import MagicMock, patch

from flowsense import ConfigurationError
from flowsense.cli.doctor import DiagnosticStatus, run_airflow_diagnostics
from flowsense.infrastructure.airflow import AirflowApiError, AirflowConfig


def test_doctor_reports_success_without_exposing_credentials() -> None:
    config = AirflowConfig(
        base_url="https://airflow.test/",
        username="secret-user",
        password="secret-password",
        api_version="v2",
        auth_mode="token",
    )
    client = MagicMock()
    client.__enter__.return_value = client
    client.list_dag_ids.return_value = ["first", "second"]

    with (
        patch("flowsense.cli.doctor.load_airflow_config", return_value=config),
        patch("flowsense.cli.doctor.AirflowClient", return_value=client),
    ):
        report = run_airflow_diagnostics()

    assert report.successful is True
    assert [check.status for check in report.checks] == [
        DiagnosticStatus.PASS,
        DiagnosticStatus.PASS,
        DiagnosticStatus.PASS,
    ]
    rendered = str(report.to_dict())
    assert "secret-user" not in rendered
    assert "secret-password" not in rendered


def test_doctor_stops_after_invalid_configuration() -> None:
    with (
        patch(
            "flowsense.cli.doctor.load_airflow_config",
            side_effect=ConfigurationError("AIRFLOW_USERNAME is required."),
        ),
        patch("flowsense.cli.doctor.AirflowClient") as client,
    ):
        report = run_airflow_diagnostics()

    assert report.successful is False
    assert report.checks[0].status is DiagnosticStatus.FAIL
    assert report.checks[1].status is DiagnosticStatus.SKIP
    client.assert_not_called()


def test_doctor_reports_airflow_api_failure() -> None:
    config = AirflowConfig(base_url="https://airflow.test")
    client = MagicMock()
    client.__enter__.return_value = client
    client.list_dag_ids.side_effect = AirflowApiError("GET", "/api/v2/dags", 401)

    with (
        patch("flowsense.cli.doctor.load_airflow_config", return_value=config),
        patch("flowsense.cli.doctor.AirflowClient", return_value=client),
    ):
        report = run_airflow_diagnostics()

    assert report.successful is False
    assert report.checks[1].status is DiagnosticStatus.FAIL
    assert report.checks[2].status is DiagnosticStatus.SKIP


def test_doctor_fails_when_no_dags_are_visible() -> None:
    config = AirflowConfig(base_url="https://airflow.test")
    client = MagicMock()
    client.__enter__.return_value = client
    client.list_dag_ids.return_value = []

    with (
        patch("flowsense.cli.doctor.load_airflow_config", return_value=config),
        patch("flowsense.cli.doctor.AirflowClient", return_value=client),
    ):
        report = run_airflow_diagnostics(include_paused=True)

    assert report.successful is False
    assert report.checks[-1].status is DiagnosticStatus.FAIL
    client.list_dag_ids.assert_called_once_with(include_paused=True)
