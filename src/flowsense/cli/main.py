from __future__ import annotations

import json
from enum import StrEnum
from typing import Annotated

import typer
from rich.console import Console

from flowsense.application import (
    AnalysisRequest,
    analysis_json_schema,
    analyze_dag,
    serialize_analysis,
)
from flowsense.cli.report import render_analysis
from flowsense.domain import (
    AnalysisPolicy,
    FlowSenseError,
    MappedTaskAggregation,
    Severity,
    severity_meets_threshold,
)
from flowsense.infrastructure.airflow import AirflowApiError, AirflowClient
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
def show_schema() -> None:
    """Print the versioned analysis output JSON Schema."""
    typer.echo(
        json.dumps(
            analysis_json_schema(),
            indent=2,
            ensure_ascii=False,
            sort_keys=True,
        )
    )


@app.command()
def analyze(
    dag_id: str = typer.Argument(
        ...,
        help="Airflow DAG id to analyze.",
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
    output: Annotated[
        OutputFormat,
        typer.Option("--output", "-o"),
    ] = OutputFormat.TABLE,
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
) -> None:
    try:
        policy = AnalysisPolicy(
            minimum_history=minimum_history,
            baseline_window=baseline_window,
            medium_threshold=medium_threshold,
            high_threshold=high_threshold,
            critical_threshold=critical_threshold,
            mapped_task_aggregation=mapped_task_aggregation,
            change_point_detection_enabled=change_point_detection,
            change_point_minimum_segment_size=change_point_minimum_segment_size,
            change_point_score_threshold=change_point_score_threshold,
            trend_detection_enabled=trend_detection,
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
        )
        with AirflowClient(history_run_limit=history_run_limit) as source:
            analysis = analyze_dag(
                dag_id=request.dag_id,
                source=source,
                policy=request.policy,
            )
    except AirflowApiError as exc:
        console.print(f"[bold red]Airflow request failed:[/bold red] {exc}")
        raise typer.Exit(code=1) from exc
    except FlowSenseError as exc:
        console.print(f"[bold red]Analysis failed:[/bold red] {exc}")
        raise typer.Exit(code=1) from exc

    if output is OutputFormat.JSON:
        typer.echo(
            json.dumps(
                serialize_analysis(analysis),
                indent=2,
                ensure_ascii=False,
            )
        )
    else:
        render_analysis(console, analysis)

    if fail_on is not None and severity_meets_threshold(
        analysis.overall_severity,
        Severity(fail_on.value.upper()),
    ):
        raise typer.Exit(code=ANALYSIS_THRESHOLD_EXIT_CODE)
