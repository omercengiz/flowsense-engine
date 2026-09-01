from __future__ import annotations

import typer
from rich.console import Console
from rich.table import Table

from flowsense.application import analyze_dag
from flowsense.collector.airflow_client import AirflowClient

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
) -> None:
    analysis = analyze_dag(
        dag_id=dag_id,
        source=AirflowClient(),
    )

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
