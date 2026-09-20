import json
from pathlib import Path
from unittest.mock import ANY, MagicMock, patch

from typer.testing import CliRunner

from flowsense import (
    ANALYSIS_SCHEMA_VERSION,
    BatchAnalysisResult,
    ConfigurationError,
    DAGAnalysis,
    MappedTaskAggregation,
    Severity,
)
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


def test_doctor_outputs_json_and_succeeds() -> None:
    report = MagicMock(successful=True)
    report.to_dict.return_value = {"successful": True, "checks": []}

    with patch("flowsense.cli.main.run_airflow_diagnostics", return_value=report):
        result = CliRunner().invoke(app, ["doctor", "--output", "json"])

    assert result.exit_code == 0
    assert json.loads(result.output) == {"successful": True, "checks": []}


def test_doctor_exits_with_one_when_a_check_fails() -> None:
    report = MagicMock(successful=False)
    report.to_dict.return_value = {"successful": False, "checks": []}

    with patch("flowsense.cli.main.run_airflow_diagnostics", return_value=report):
        result = CliRunner().invoke(app, ["doctor", "--output", "json"])

    assert result.exit_code == 1


def test_dags_outputs_discovered_ids_as_json() -> None:
    client = MagicMock()
    client.__enter__.return_value = client
    client.list_dag_ids.return_value = ["orders", "payments"]

    with (
        patch("flowsense.cli.main.load_airflow_config"),
        patch("flowsense.cli.main.AirflowClient", return_value=client),
    ):
        result = CliRunner().invoke(
            app,
            ["dags", "--include-paused", "--output", "json"],
        )

    assert result.exit_code == 0
    assert json.loads(result.output) == {
        "count": 2,
        "include_paused": True,
        "dag_ids": ["orders", "payments"],
    }
    client.list_dag_ids.assert_called_once_with(include_paused=True)


def test_dags_reports_configuration_failure_without_traceback() -> None:
    with patch(
        "flowsense.cli.main.load_airflow_config",
        side_effect=ConfigurationError("AIRFLOW_USERNAME is required."),
    ):
        result = CliRunner().invoke(app, ["dags"])

    assert result.exit_code == 1
    assert "DAG discovery failed" in result.output
    assert "AIRFLOW_USERNAME is required" in result.output
    assert "Traceback" not in result.output


def test_dags_reports_empty_result_in_table_mode() -> None:
    client = MagicMock()
    client.__enter__.return_value = client
    client.list_dag_ids.return_value = []

    with (
        patch("flowsense.cli.main.load_airflow_config"),
        patch("flowsense.cli.main.AirflowClient", return_value=client),
    ):
        result = CliRunner().invoke(app, ["dags"])

    assert result.exit_code == 0
    assert "No DAGs found" in result.output


def test_analyze_batch_outputs_versioned_json() -> None:
    batch = BatchAnalysisResult(
        requested_dag_ids=("first", "second"),
        analyses={
            "first": _analysis_with_severity(Severity.NORMAL),
            "second": _analysis_with_severity(Severity.HIGH),
        },
        failures={},
    )

    with patch(
        "flowsense.cli.main.FlowSenseClient.analyze_many",
        return_value=batch,
    ) as analyze_many:
        result = CliRunner().invoke(
            app,
            [
                "analyze-batch",
                "first",
                "second",
                "--history-run-limit",
                "50",
                "--max-concurrency",
                "2",
            ],
        )

    assert result.exit_code == 0
    document = json.loads(result.output)
    assert document["schema_version"] == "1.0"
    assert document["requested_dag_ids"] == ["first", "second"]
    analyze_many.assert_called_once_with(
        ["first", "second"],
        policy=ANY,
        history_run_limit=50,
        max_concurrency=2,
    )


def test_analyze_batch_builds_policy_from_options() -> None:
    batch = BatchAnalysisResult(
        requested_dag_ids=("demo",),
        analyses={"demo": _analysis_with_severity(Severity.NORMAL)},
        failures={},
    )

    with patch(
        "flowsense.cli.main.FlowSenseClient.analyze_many",
        return_value=batch,
    ) as analyze_many:
        result = CliRunner().invoke(
            app,
            [
                "analyze-batch",
                "demo",
                "--minimum-history",
                "10",
                "--baseline-window",
                "20",
                "--medium-threshold",
                "2.5",
                "--high-threshold",
                "4",
                "--critical-threshold",
                "6",
                "--no-change-point-detection",
                "--no-trend-detection",
                "--mapped-task-aggregation",
                "MEAN",
            ],
        )

    assert result.exit_code == 0
    policy = analyze_many.call_args.kwargs["policy"]
    assert policy.minimum_history == 10
    assert policy.baseline_window == 20
    assert policy.medium_threshold == 2.5
    assert policy.high_threshold == 4.0
    assert policy.critical_threshold == 6.0
    assert policy.change_point_detection_enabled is False
    assert policy.trend_detection_enabled is False
    assert policy.mapped_task_aggregation is MappedTaskAggregation.MEAN


def test_analyze_batch_writes_partial_result_before_failure_exit(
    tmp_path: Path,
) -> None:
    batch = BatchAnalysisResult(
        requested_dag_ids=("first", "broken"),
        analyses={"first": _analysis_with_severity(Severity.NORMAL)},
        failures={"broken": ConfigurationError("invalid configuration")},
    )
    output_path = tmp_path / "batch.json"

    with patch(
        "flowsense.cli.main.FlowSenseClient.analyze_many",
        return_value=batch,
    ):
        result = CliRunner().invoke(
            app,
            [
                "analyze-batch",
                "first",
                "broken",
                "--output-file",
                str(output_path),
            ],
        )
        document = json.loads(output_path.read_text(encoding="utf-8"))

    assert result.exit_code == 1
    assert document["successful_count"] == 1
    assert document["failed_count"] == 1
    assert document["failures"]["broken"]["error_type"] == "ConfigurationError"


def test_analyze_batch_exits_with_threshold_code_after_writing_json(
    tmp_path: Path,
) -> None:
    batch = BatchAnalysisResult(
        requested_dag_ids=("normal", "critical"),
        analyses={
            "normal": _analysis_with_severity(Severity.NORMAL),
            "critical": _analysis_with_severity(Severity.CRITICAL),
        },
        failures={},
    )
    output_path = tmp_path / "batch.json"

    with patch(
        "flowsense.cli.main.FlowSenseClient.analyze_many",
        return_value=batch,
    ):
        result = CliRunner().invoke(
            app,
            [
                "analyze-batch",
                "normal",
                "critical",
                "--fail-on",
                "high",
                "--output-file",
                str(output_path),
            ],
        )

    assert result.exit_code == 2
    assert json.loads(output_path.read_text(encoding="utf-8"))["failed_count"] == 0


def test_analyze_batch_prioritizes_operational_failure_exit_code() -> None:
    batch = BatchAnalysisResult(
        requested_dag_ids=("critical", "broken"),
        analyses={"critical": _analysis_with_severity(Severity.CRITICAL)},
        failures={"broken": ConfigurationError("invalid configuration")},
    )

    with patch(
        "flowsense.cli.main.FlowSenseClient.analyze_many",
        return_value=batch,
    ):
        result = CliRunner().invoke(
            app,
            ["analyze-batch", "critical", "broken", "--fail-on", "medium"],
        )

    assert result.exit_code == 1
    assert json.loads(result.output)["failed_count"] == 1


def test_serve_metrics_forwards_runtime_configuration() -> None:
    with patch(
        "flowsense.observability.service.run_metrics_service"
    ) as run_metrics_service:
        result = CliRunner().invoke(
            app,
            [
                "serve-metrics",
                "first_dag",
                "second_dag",
                "--host",
                "0.0.0.0",
                "--port",
                "9200",
                "--interval-seconds",
                "30",
                "--max-tasks-per-dag",
                "50",
            ],
        )

    assert result.exit_code == 0
    run_metrics_service.assert_called_once_with(
        ["first_dag", "second_dag"],
        source_factory=ANY,
        host="0.0.0.0",
        port=9200,
        interval_seconds=30.0,
        max_tasks_per_dag=50,
    )


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
