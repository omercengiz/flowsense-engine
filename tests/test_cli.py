import json
from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from flowsense import DAGAnalysis, Severity
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


def test_analyze_builds_structural_analysis_policy_from_options() -> None:
    with (
        patch("flowsense.cli.main.AirflowClient"),
        patch("flowsense.cli.main.analyze_dag", return_value=MagicMock()) as analyze,
        patch("flowsense.cli.main.render_analysis"),
    ):
        result = CliRunner().invoke(
            app,
            [
                "analyze",
                "demo",
                "--no-change-point-detection",
                "--change-point-minimum-segment-size",
                "4",
                "--change-point-score-threshold",
                "4.5",
                "--no-trend-detection",
                "--trend-minimum-observations",
                "8",
                "--trend-score-threshold",
                "4.5",
                "--trend-minimum-directional-consistency",
                "0.75",
            ],
        )

    assert result.exit_code == 0
    policy = analyze.call_args.kwargs["policy"]
    assert policy.change_point_detection_enabled is False
    assert policy.change_point_minimum_segment_size == 4
    assert policy.change_point_score_threshold == 4.5
    assert policy.trend_detection_enabled is False
    assert policy.trend_minimum_observations == 8
    assert policy.trend_score_threshold == 4.5
    assert policy.trend_minimum_directional_consistency == 0.75


def test_analyze_outputs_versioned_json() -> None:
    analysis = DAGAnalysis(
        dag_id="demo",
        runs_analyzed=0,
        overall_severity="NORMAL",
        primary_origin=None,
        drift_results={},
        handoff_drift_results={},
        task_impacts={},
        propagation_results=[],
        dependencies={},
    )

    with (
        patch("flowsense.cli.main.AirflowClient"),
        patch("flowsense.cli.main.analyze_dag", return_value=analysis),
        patch("flowsense.cli.main.render_analysis") as render_analysis,
    ):
        result = CliRunner().invoke(app, ["analyze", "demo", "--output", "json"])

    assert result.exit_code == 0
    document = json.loads(result.output)
    assert document["schema_version"] == "1.0"
    assert document["dag_id"] == "demo"
    assert document["overall_severity"] == "NORMAL"
    render_analysis.assert_not_called()


def test_analyze_overrides_airflow_history_run_limit() -> None:
    analysis = _analysis_with_severity(Severity.NORMAL)

    with (
        patch("flowsense.cli.main.AirflowClient") as client_class,
        patch("flowsense.cli.main.analyze_dag", return_value=analysis),
        patch("flowsense.cli.main.render_analysis"),
    ):
        result = CliRunner().invoke(
            app,
            ["analyze", "demo", "--history-run-limit", "250"],
        )

    assert result.exit_code == 0
    client_class.assert_called_once_with(history_run_limit=250)


def test_analyze_exits_with_threshold_code_after_rendering_report() -> None:
    analysis = _analysis_with_severity(Severity.HIGH)

    with (
        patch("flowsense.cli.main.AirflowClient"),
        patch("flowsense.cli.main.analyze_dag", return_value=analysis),
        patch("flowsense.cli.main.render_analysis") as render_analysis,
    ):
        result = CliRunner().invoke(app, ["analyze", "demo", "--fail-on", "high"])

    assert result.exit_code == 2
    render_analysis.assert_called_once()


def test_analyze_succeeds_when_severity_is_below_threshold() -> None:
    analysis = _analysis_with_severity(Severity.MEDIUM)

    with (
        patch("flowsense.cli.main.AirflowClient"),
        patch("flowsense.cli.main.analyze_dag", return_value=analysis),
        patch("flowsense.cli.main.render_analysis"),
    ):
        result = CliRunner().invoke(app, ["analyze", "demo", "--fail-on", "high"])

    assert result.exit_code == 0


def test_analyze_outputs_valid_json_before_threshold_exit() -> None:
    analysis = _analysis_with_severity(Severity.CRITICAL)

    with (
        patch("flowsense.cli.main.AirflowClient"),
        patch("flowsense.cli.main.analyze_dag", return_value=analysis),
    ):
        result = CliRunner().invoke(
            app,
            ["analyze", "demo", "--output", "json", "--fail-on", "medium"],
        )

    assert result.exit_code == 2
    assert json.loads(result.output)["overall_severity"] == "CRITICAL"


def _analysis_with_severity(severity: Severity) -> DAGAnalysis:
    return DAGAnalysis(
        dag_id="demo",
        runs_analyzed=0,
        overall_severity=severity,
        primary_origin=None,
        drift_results={},
        handoff_drift_results={},
        task_impacts={},
        propagation_results=[],
        dependencies={},
    )
