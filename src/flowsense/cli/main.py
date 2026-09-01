from __future__ import annotations

from typing import Annotated

import typer
from rich.console import Console

from flowsense.application import analyze_dag
from flowsense.cli.report import render_analysis
from flowsense.domain import AnalysisPolicy, MappedTaskAggregation
from flowsense.infrastructure.airflow import AirflowApiError, AirflowClient

app = typer.Typer(
    name="flowsense",
    help="Temporal drift and anomaly detection for Apache Airflow.",
    no_args_is_help=True,
)

console = Console()


@app.callback()
def main() -> None:
    """FlowSense CLI."""


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
    mapped_task_aggregation: Annotated[
        MappedTaskAggregation,
        typer.Option(),
    ] = MappedTaskAggregation.MAX,
) -> None:
    try:
        policy = AnalysisPolicy(
            minimum_history=minimum_history,
            baseline_window=baseline_window,
            medium_threshold=medium_threshold,
            high_threshold=high_threshold,
            critical_threshold=critical_threshold,
            mapped_task_aggregation=mapped_task_aggregation,
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc

    try:
        with AirflowClient() as source:
            analysis = analyze_dag(
                dag_id=dag_id,
                source=source,
                policy=policy,
            )
    except AirflowApiError as exc:
        console.print(f"[bold red]Airflow request failed:[/bold red] {exc}")
        raise typer.Exit(code=1) from exc

    render_analysis(console, analysis)
