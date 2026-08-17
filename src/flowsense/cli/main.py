from __future__ import annotations

import typer
from rich.console import Console
from rich.table import Table

from flowsense.collector.airflow_client import AirflowClient
from flowsense.engine.drift import calculate_drift
from flowsense.engine.history import build_duration_history
from flowsense.engine.propagation import analyze_propagation

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
        help="Airflow DAG ID to analyze.",
    ),
) -> None:
    client = AirflowClient()

    task_runs = client.collect_task_runs(dag_id)

    if not task_runs:
        console.print(f"[red]No successful task runs found for DAG: {dag_id}[/red]")
        raise typer.Exit(code=1)

    history = build_duration_history(task_runs)

    drift_results = {}

    for task_id, durations in history.items():
        result = calculate_drift(
            task_id,
            durations,
        )

        drift_results[task_id] = result

    dependencies = client.get_dag_dependencies(dag_id)

    propagation_results = analyze_propagation(
        drift_results,
        dependencies,
    )

    console.print()
    console.print(f"[bold]FlowSense Analysis[/bold] — {dag_id}")
    console.print()

    table = Table()

    table.add_column("Task")
    table.add_column("Baseline")
    table.add_column("Current")
    table.add_column("Deviation")
    table.add_column("Z-Score")
    table.add_column("Severity")

    for result in drift_results.values():
        table.add_row(
            result.task_id,
            f"{result.baseline:.2f}s",
            f"{result.current:.2f}s",
            f"{result.deviation_percent:+.1f}%",
            f"{result.robust_z_score:.2f}",
            result.severity,
        )

    console.print(table)

    console.print()
    console.print("[bold]Propagation Analysis[/bold]")

    if not propagation_results:
        console.print("No propagation detected.")
        return

    for propagation in propagation_results:
        console.print()
        console.print(f"Origin: [bold]{propagation.origin_task}[/bold]")

        console.print("Path: " + " -> ".join(propagation.path))

        console.print(f"Propagation Score: {propagation.propagation_score:.2f}")


if __name__ == "__main__":
    app()
