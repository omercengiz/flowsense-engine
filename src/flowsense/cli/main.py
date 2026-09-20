from __future__ import annotations

import json
from enum import StrEnum
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from flowsense.application import (
    AnalysisRequest,
    AnalyzeDAG,
    FlowSenseClient,
    analysis_json_schema,
    analysis_policy_json_schema,
    batch_analysis_json_schema,
    serialize_analysis,
    serialize_batch_analysis,
)
from flowsense.cli.doctor import DiagnosticStatus, run_airflow_diagnostics
from flowsense.cli.policy import resolve_analysis_policy
from flowsense.cli.report import render_analysis
from flowsense.domain import (
    ConfigurationError,
    FlowSenseError,
    MappedTaskAggregation,
    Severity,
    severity_meets_threshold,
)
from flowsense.infrastructure.airflow import (
    AirflowApiError,
    AirflowClient,
    create_airflow_data_source,
    load_airflow_config,
)
from flowsense.version import __version__

app = typer.Typer(
    name="flowsense",
    help="Temporal drift and anomaly detection for Apache Airflow.",
    no_args_is_help=True,
)

console = Console()
ANALYSIS_THRESHOLD_EXIT_CODE = 2


def _version_callback(value: bool) -> None:
    if value:
        typer.echo(__version__)
        raise typer.Exit()


class OutputFormat(StrEnum):
    TABLE = "table"
    JSON = "json"


class FailureThreshold(StrEnum):
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class SchemaDocument(StrEnum):
    ANALYSIS = "analysis"
    BATCH = "batch"
    POLICY = "policy"


@app.callback()
def main(
    version: Annotated[
        bool | None,
        typer.Option(
            "--version",
            callback=_version_callback,
            is_eager=True,
            help="Show the installed FlowSense version and exit.",
        ),
    ] = None,
) -> None:
    """FlowSense CLI."""


@app.command("schema")
def show_schema(
    document: Annotated[
        SchemaDocument,
        typer.Option(
            "--document",
            help="Output contract whose JSON Schema should be printed.",
        ),
    ] = SchemaDocument.ANALYSIS,
) -> None:
    """Print a versioned FlowSense output JSON Schema."""
    schemas = {
        SchemaDocument.ANALYSIS: analysis_json_schema,
        SchemaDocument.BATCH: batch_analysis_json_schema,
        SchemaDocument.POLICY: analysis_policy_json_schema,
    }
    schema = schemas[document]()
    typer.echo(
        json.dumps(
            schema,
            indent=2,
            ensure_ascii=False,
            sort_keys=True,
        )
    )


@app.command()
def doctor(
    output: Annotated[
        OutputFormat,
        typer.Option("--output", "-o"),
    ] = OutputFormat.TABLE,
    include_paused: bool = typer.Option(
        False,
        help="Include paused DAGs when checking DAG visibility.",
    ),
) -> None:
    """Validate FlowSense configuration and read-only Airflow access."""
    report = run_airflow_diagnostics(include_paused=include_paused)
    if output is OutputFormat.JSON:
        typer.echo(json.dumps(report.to_dict(), indent=2, ensure_ascii=False))
    else:
        styles = {
            DiagnosticStatus.PASS: "bold green",
            DiagnosticStatus.FAIL: "bold red",
            DiagnosticStatus.SKIP: "yellow",
        }
        for check in report.checks:
            console.print(
                f"[{styles[check.status]}]{check.status.value.upper():4}[/] "
                f"{check.name}: {check.message}"
            )

    if not report.successful:
        raise typer.Exit(code=1)


@app.command("dags")
def list_dags(
    output: Annotated[
        OutputFormat,
        typer.Option("--output", "-o"),
    ] = OutputFormat.TABLE,
    include_paused: bool = typer.Option(
        False,
        help="Include paused DAGs in the result.",
    ),
) -> None:
    """List DAG ids visible to the configured Airflow identity."""
    try:
        config = load_airflow_config()
        with AirflowClient(config) as airflow:
            dag_ids = airflow.list_dag_ids(include_paused=include_paused)
    except FlowSenseError as exc:
        console.print(f"[bold red]DAG discovery failed:[/bold red] {exc}")
        raise typer.Exit(code=1) from exc

    if output is OutputFormat.JSON:
        typer.echo(
            json.dumps(
                {
                    "count": len(dag_ids),
                    "include_paused": include_paused,
                    "dag_ids": dag_ids,
                },
                indent=2,
                ensure_ascii=False,
            )
        )
        return

    if not dag_ids:
        console.print("No DAGs found.")
        return

    for dag_id in dag_ids:
        console.print(dag_id)


@app.command("analyze-batch")
def analyze_batch(
    context: typer.Context,
    dag_ids: Annotated[
        list[str] | None,
        typer.Argument(help="Explicit Airflow DAG ids to analyze."),
    ] = None,
    all_dags: bool = typer.Option(
        False,
        "--all-dags",
        help="Discover and analyze DAGs visible to the configured identity.",
    ),
    include_paused: bool = typer.Option(
        False,
        help="Include paused DAGs when using --all-dags.",
    ),
    dag_limit: int | None = typer.Option(
        None,
        min=1,
        help="Maximum number of discovered DAGs to analyze with --all-dags.",
    ),
    policy_file: Annotated[
        Path | None,
        typer.Option(
            "--policy-file",
            help="Load a versioned JSON policy; explicit CLI options override it.",
        ),
    ] = None,
    history_run_limit: int | None = typer.Option(
        None,
        min=2,
        help="Limit collection to the most recent successful runs per DAG.",
    ),
    max_concurrency: int = typer.Option(
        1,
        min=1,
        help="Maximum number of concurrent DAG analyses.",
    ),
    minimum_history: int = typer.Option(5, min=2),
    baseline_window: int | None = typer.Option(None, min=1),
    medium_threshold: float = typer.Option(2.0, min=0.0),
    high_threshold: float = typer.Option(3.5, min=0.0),
    critical_threshold: float = typer.Option(5.0, min=0.0),
    change_point_detection: bool = typer.Option(
        True,
        "--change-point-detection/--no-change-point-detection",
    ),
    change_point_minimum_segment_size: int = typer.Option(3, min=2),
    change_point_score_threshold: float = typer.Option(3.5, min=0.0),
    trend_detection: bool = typer.Option(
        True,
        "--trend-detection/--no-trend-detection",
    ),
    trend_minimum_observations: int = typer.Option(5, min=3),
    trend_score_threshold: float = typer.Option(3.5, min=0.0),
    trend_minimum_directional_consistency: float = typer.Option(
        0.6,
        min=0.0,
        max=1.0,
    ),
    mapped_task_aggregation: Annotated[
        MappedTaskAggregation,
        typer.Option(),
    ] = MappedTaskAggregation.MAX,
    fail_on: Annotated[
        FailureThreshold | None,
        typer.Option(
            "--fail-on",
            help="Exit with code 2 when any DAG reaches this severity.",
        ),
    ] = None,
    output_file: Annotated[
        Path | None,
        typer.Option(
            "--output-file",
            help="Write batch JSON to this file instead of standard output.",
        ),
    ] = None,
) -> None:
    """Analyze explicit or discovered DAGs and emit versioned batch JSON."""
    try:
        selected_dag_ids = _resolve_batch_dag_ids(
            dag_ids,
            all_dags=all_dags,
            include_paused=include_paused,
            dag_limit=dag_limit,
        )
        policy = resolve_analysis_policy(
            context,
            policy_file,
            minimum_history=minimum_history,
            baseline_window=baseline_window,
            medium_threshold=medium_threshold,
            high_threshold=high_threshold,
            critical_threshold=critical_threshold,
            mapped_task_aggregation=mapped_task_aggregation,
            change_point_detection=change_point_detection,
            change_point_minimum_segment_size=change_point_minimum_segment_size,
            change_point_score_threshold=change_point_score_threshold,
            trend_detection=trend_detection,
            trend_minimum_observations=trend_minimum_observations,
            trend_score_threshold=trend_score_threshold,
            trend_minimum_directional_consistency=(
                trend_minimum_directional_consistency
            ),
        )
        result = FlowSenseClient(create_airflow_data_source).analyze_many(
            selected_dag_ids,
            policy=policy,
            history_run_limit=history_run_limit,
            max_concurrency=max_concurrency,
        )
        rendered = json.dumps(
            serialize_batch_analysis(result),
            indent=2,
            ensure_ascii=False,
        )
        if output_file is None:
            typer.echo(rendered)
        else:
            output_file.write_text(f"{rendered}\n", encoding="utf-8")
    except (FlowSenseError, OSError) as exc:
        console.print(f"[bold red]Batch analysis failed:[/bold red] {exc}")
        raise typer.Exit(code=1) from exc

    if result.failures:
        raise typer.Exit(code=1)

    if fail_on is not None:
        threshold = Severity(fail_on.value.upper())
        if any(
            severity_meets_threshold(analysis.overall_severity, threshold)
            for analysis in result.analyses.values()
        ):
            raise typer.Exit(code=ANALYSIS_THRESHOLD_EXIT_CODE)


def _resolve_batch_dag_ids(
    dag_ids: list[str] | None,
    *,
    all_dags: bool,
    include_paused: bool,
    dag_limit: int | None,
) -> list[str]:
    explicit_dag_ids = dag_ids or []
    if all_dags:
        if explicit_dag_ids:
            raise ConfigurationError(
                "Explicit DAG ids cannot be combined with --all-dags."
            )
        config = load_airflow_config()
        with AirflowClient(config) as airflow:
            discovered_dag_ids = sorted(
                airflow.list_dag_ids(include_paused=include_paused)
            )
        selected_dag_ids = (
            discovered_dag_ids[:dag_limit]
            if dag_limit is not None
            else discovered_dag_ids
        )
        if not selected_dag_ids:
            raise ConfigurationError("No DAGs were discovered for batch analysis.")
        return selected_dag_ids

    if include_paused:
        raise ConfigurationError("--include-paused requires --all-dags.")
    if dag_limit is not None:
        raise ConfigurationError("--dag-limit requires --all-dags.")
    if not explicit_dag_ids:
        raise ConfigurationError("Provide one or more DAG ids or use --all-dags.")
    return explicit_dag_ids


@app.command("serve-metrics")
def serve_metrics(
    dag_ids: Annotated[
        list[str],
        typer.Argument(help="One or more Airflow DAG ids to monitor."),
    ],
    host: str = typer.Option("127.0.0.1", help="Metrics server bind address."),
    port: int = typer.Option(9108, min=1, max=65535),
    interval_seconds: float = typer.Option(60.0, min=1.0),
    max_tasks_per_dag: int = typer.Option(
        200,
        min=0,
        help="Maximum task IDs included in task-level metrics per DAG.",
    ),
) -> None:
    """Continuously analyze DAGs and expose Prometheus metrics."""
    from flowsense.observability.service import run_metrics_service

    console.print(f"Serving metrics at http://{host}:{port}/metrics")
    try:
        run_metrics_service(
            dag_ids,
            source_factory=create_airflow_data_source,
            host=host,
            port=port,
            interval_seconds=interval_seconds,
            max_tasks_per_dag=max_tasks_per_dag,
        )
    except KeyboardInterrupt:
        console.print("Metrics service stopped.")
    except (FlowSenseError, OSError, ValueError) as exc:
        console.print(f"[bold red]Metrics service failed:[/bold red] {exc}")
        raise typer.Exit(code=1) from exc


@app.command()
def analyze(
    context: typer.Context,
    dag_id: str = typer.Argument(
        ...,
        help="Airflow DAG id to analyze.",
    ),
    policy_file: Annotated[
        Path | None,
        typer.Option(
            "--policy-file",
            help="Load a versioned JSON policy; explicit CLI options override it.",
        ),
    ] = None,
    minimum_history: int = typer.Option(5, min=2),
    baseline_window: int | None = typer.Option(None, min=1),
    medium_threshold: float = typer.Option(2.0, min=0.0),
    high_threshold: float = typer.Option(3.5, min=0.0),
    critical_threshold: float = typer.Option(5.0, min=0.0),
    change_point_detection: bool = typer.Option(
        True,
        "--change-point-detection/--no-change-point-detection",
    ),
    change_point_minimum_segment_size: int = typer.Option(3, min=2),
    change_point_score_threshold: float = typer.Option(3.5, min=0.0),
    trend_detection: bool = typer.Option(
        True,
        "--trend-detection/--no-trend-detection",
    ),
    trend_minimum_observations: int = typer.Option(5, min=3),
    trend_score_threshold: float = typer.Option(3.5, min=0.0),
    trend_minimum_directional_consistency: float = typer.Option(
        0.6,
        min=0.0,
        max=1.0,
    ),
    mapped_task_aggregation: Annotated[
        MappedTaskAggregation,
        typer.Option(),
    ] = MappedTaskAggregation.MAX,
    output: Annotated[
        OutputFormat,
        typer.Option("--output", "-o"),
    ] = OutputFormat.TABLE,
    output_file: Annotated[
        Path | None,
        typer.Option(
            "--output-file",
            help="Write versioned analysis JSON to this file.",
        ),
    ] = None,
    fail_on: Annotated[
        FailureThreshold | None,
        typer.Option(
            "--fail-on",
            help="Exit with code 2 when severity reaches this threshold.",
        ),
    ] = None,
    history_run_limit: int | None = typer.Option(
        None,
        min=2,
        help="Limit collection to the most recent successful DAG runs.",
    ),
    dag_run_id: str | None = typer.Option(
        None,
        "--dag-run-id",
        help="Analyze this successful DAG run using only its preceding history.",
    ),
) -> None:
    try:
        policy = resolve_analysis_policy(
            context,
            policy_file,
            minimum_history=minimum_history,
            baseline_window=baseline_window,
            medium_threshold=medium_threshold,
            high_threshold=high_threshold,
            critical_threshold=critical_threshold,
            mapped_task_aggregation=mapped_task_aggregation,
            change_point_detection=change_point_detection,
            change_point_minimum_segment_size=change_point_minimum_segment_size,
            change_point_score_threshold=change_point_score_threshold,
            trend_detection=trend_detection,
            trend_minimum_observations=trend_minimum_observations,
            trend_score_threshold=trend_score_threshold,
            trend_minimum_directional_consistency=(
                trend_minimum_directional_consistency
            ),
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc

    try:
        request = AnalysisRequest(
            dag_id=dag_id,
            policy=policy,
            history_run_limit=history_run_limit,
            dag_run_id=dag_run_id,
        )
        analysis = AnalyzeDAG(create_airflow_data_source).execute(request)
    except AirflowApiError as exc:
        console.print(f"[bold red]Airflow request failed:[/bold red] {exc}")
        raise typer.Exit(code=1) from exc
    except FlowSenseError as exc:
        console.print(f"[bold red]Analysis failed:[/bold red] {exc}")
        raise typer.Exit(code=1) from exc

    if output_file is not None or output is OutputFormat.JSON:
        rendered = json.dumps(
            serialize_analysis(analysis),
            indent=2,
            ensure_ascii=False,
        )
        if output_file is None:
            typer.echo(rendered)
        else:
            try:
                output_file.write_text(f"{rendered}\n", encoding="utf-8")
            except OSError as exc:
                console.print(f"[bold red]Output write failed:[/bold red] {exc}")
                raise typer.Exit(code=1) from exc
    else:
        render_analysis(console, analysis)

    if fail_on is not None and severity_meets_threshold(
        analysis.overall_severity,
        Severity(fail_on.value.upper()),
    ):
        raise typer.Exit(code=ANALYSIS_THRESHOLD_EXIT_CODE)
