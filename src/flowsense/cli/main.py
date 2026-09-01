from __future__ import annotations

from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from flowsense.application import analyze_dag
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

    console.print(f"\n[bold]FlowSense Analysis — {analysis.dag_id}[/bold]\n")

    table = Table()

    table.add_column("Task")
    table.add_column("Baseline")
    table.add_column("Current")
    table.add_column("Deviation")
    table.add_column("Z-Score")
    table.add_column("Severity")
    table.add_column("Impact")

    for task_id, result in analysis.drift_results.items():
        impact = analysis.task_impacts.get(task_id)
        impact_label = impact.classification if impact else "-"

        table.add_row(
            task_id,
            f"{result.baseline:.2f}s",
            f"{result.current:.2f}s",
            f"{result.deviation_percent:+.1f}%",
            f"{result.robust_z_score:.2f}",
            result.severity,
            impact_label,
        )

    console.print(table)

    console.print(f"\nOverall Severity: [bold]{analysis.overall_severity}[/bold]")

    if analysis.primary_origin:
        console.print(f"Primary Origin: [bold]{analysis.primary_origin.task_id}[/bold]")
        console.print(f"Reason: [bold]{analysis.primary_origin.classification}[/bold]")
        console.print(f"Severity: [bold]{analysis.primary_origin.severity}[/bold]")
        console.print(
            f"Propagation Score: {analysis.primary_origin.propagation_score:.2f}"
        )

    if analysis.propagation_results:
        console.print("\n[bold]Propagation Analysis[/bold]\n")

        for result in analysis.propagation_results:
            console.print(f"Origin: {result.origin_task}")
            console.print(f"Path: {' -> '.join(result.path)}")
            console.print(f"Propagation Score: {result.propagation_score:.2f}")

    if analysis.diagnostics:
        console.print("\n[bold yellow]Diagnostics[/bold yellow]\n")

        for diagnostic in analysis.diagnostics:
            console.print(
                f"[{diagnostic.code}] {diagnostic.subject_id}: {diagnostic.message}"
            )
