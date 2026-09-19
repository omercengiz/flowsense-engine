import json
from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from flowsense import ANALYSIS_SCHEMA_VERSION, ConfigurationError, DAGAnalysis, Severity
from flowsense.cli.main import app
from flowsense.infrastructure.airflow import AirflowApiError
from flowsense.version import __version__


def test_version_outputs_installed_package_version() -> None:
    result = CliRunner().invoke(app, ["--version"])

    assert result.exit_code == 0
    assert result.output.strip() == __version__


def test_schema_outputs_versioned_json_schema_without_airflow() -> None:
    with patch("flowsense.cli.main.create_airflow_data_source") as source_factory:
        result = CliRunner().invoke(app, ["schema"])

    assert result.exit_code == 0
    schema = json.loads(result.output)
    assert schema["properties"]["schema_version"]["const"] == (ANALYSIS_SCHEMA_VERSION)
    assert schema["title"] == "AnalysisDocument"
    source_factory.assert_not_called()


def test_schema_output_is_deterministic() -> None:
    runner = CliRunner()

    first = runner.invoke(app, ["schema"])
    second = runner.invoke(app, ["schema"])

    assert first.exit_code == 0
    assert second.exit_code == 0
    assert first.output == second.output


def test_analyze_reports_airflow_api_errors() -> None:
    error = AirflowApiError(
        method="GET",
        endpoint="/api/v2/dags/demo/dagRuns",
        status_code=503,
    )

    with patch(
        "flowsense.cli.main.create_airflow_data_source",
        side_effect=error,
    ):
        result = CliRunner().invoke(app, ["analyze", "demo"])

    assert result.exit_code == 1
    assert "Airflow request failed" in result.output
    assert "503" in result.output


def test_analyze_reports_configuration_errors_without_traceback() -> None:
    with patch(
        "flowsense.cli.main.create_airflow_data_source",
        side_effect=ConfigurationError("AIRFLOW_USERNAME is required."),
    ):
        result = CliRunner().invoke(app, ["analyze", "demo"])

    assert result.exit_code == 1
    assert "Analysis failed" in result.output
    assert "AIRFLOW_USERNAME is required" in result.output
    assert "Traceback" not in result.output


def test_analyze_builds_structural_analysis_policy_from_options() -> None:
    with (
        patch("flowsense.cli.main.create_airflow_data_source"),
        patch(
            "flowsense.cli.main.AnalyzeDAG.execute", return_value=MagicMock()
        ) as execute,
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
    policy = execute.call_args.args[0].policy
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
        patch("flowsense.cli.main.create_airflow_data_source"),
        patch("flowsense.cli.main.AnalyzeDAG.execute", return_value=analysis),
        patch("flowsense.cli.main.render_analysis") as render_analysis,
    ):
        result = CliRunner().invoke(app, ["analyze", "demo", "--output", "json"])

    assert result.exit_code == 0
    document = json.loads(result.output)
    assert document["schema_version"] == ANALYSIS_SCHEMA_VERSION
    assert document["dag_id"] == "demo"
    assert document["overall_severity"] == "NORMAL"
    render_analysis.assert_not_called()


def test_analyze_overrides_airflow_history_run_limit() -> None:
    analysis = _analysis_with_severity(Severity.NORMAL)

    with (
        patch("flowsense.cli.main.create_airflow_data_source"),
        patch(
            "flowsense.cli.main.AnalyzeDAG.execute", return_value=analysis
        ) as execute,
        patch("flowsense.cli.main.render_analysis"),
    ):
        result = CliRunner().invoke(
            app,
            ["analyze", "demo", "--history-run-limit", "250"],
        )

    assert result.exit_code == 0
    request = execute.call_args.args[0]
    assert request.history_run_limit == 250
    assert request.dag_run_id is None


def test_analyze_selects_historical_dag_run() -> None:
    analysis = _analysis_with_severity(Severity.NORMAL)

    with (
        patch("flowsense.cli.main.create_airflow_data_source"),
        patch(
            "flowsense.cli.main.AnalyzeDAG.execute", return_value=analysis
        ) as execute,
        patch("flowsense.cli.main.render_analysis"),
    ):
        result = CliRunner().invoke(
            app,
            ["analyze", "demo", "--dag-run-id", "run_42"],
        )

    assert result.exit_code == 0
    request = execute.call_args.args[0]
    assert request.history_run_limit is None
    assert request.dag_run_id == "run_42"


def test_analyze_rejects_inconsistent_history_settings_before_airflow() -> None:
    with patch("flowsense.cli.main.create_airflow_data_source") as source_factory:
        result = CliRunner().invoke(
            app,
            [
                "analyze",
                "demo",
                "--minimum-history",
                "20",
                "--history-run-limit",
                "10",
            ],
        )

    assert result.exit_code == 1
    assert "history_run_limit must be at least minimum_history" in result.output
    source_factory.assert_not_called()


def test_analyze_exits_with_threshold_code_after_rendering_report() -> None:
    analysis = _analysis_with_severity(Severity.HIGH)

    with (
        patch("flowsense.cli.main.create_airflow_data_source"),
        patch("flowsense.cli.main.AnalyzeDAG.execute", return_value=analysis),
        patch("flowsense.cli.main.render_analysis") as render_analysis,
    ):
        result = CliRunner().invoke(app, ["analyze", "demo", "--fail-on", "high"])

    assert result.exit_code == 2
    render_analysis.assert_called_once()


def test_analyze_succeeds_when_severity_is_below_threshold() -> None:
    analysis = _analysis_with_severity(Severity.MEDIUM)

    with (
        patch("flowsense.cli.main.create_airflow_data_source"),
        patch("flowsense.cli.main.AnalyzeDAG.execute", return_value=analysis),
        patch("flowsense.cli.main.render_analysis"),
    ):
        result = CliRunner().invoke(app, ["analyze", "demo", "--fail-on", "high"])

    assert result.exit_code == 0


def test_analyze_outputs_valid_json_before_threshold_exit() -> None:
    analysis = _analysis_with_severity(Severity.CRITICAL)

    with (
        patch("flowsense.cli.main.create_airflow_data_source"),
        patch("flowsense.cli.main.AnalyzeDAG.execute", return_value=analysis),
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
