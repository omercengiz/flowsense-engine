from __future__ import annotations

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from flowsense.domain import DAGAnalysis, Severity
from flowsense.domain.enums import SEVERITY_SCORE

_SEVERITY_STYLES = {
    Severity.NORMAL: "green",
    Severity.MEDIUM: "yellow",
    Severity.HIGH: "bright_red",
    Severity.CRITICAL: "bold red",
}


def _severity_text(severity: Severity) -> Text:
    return Text(str(severity), style=_SEVERITY_STYLES[severity])


def _percent(value: float | None) -> str:
    return f"{value:+.1f}%" if value is not None else "n/a"


def _render_summary(console: Console, analysis: DAGAnalysis) -> None:
    dag_summary = analysis.summary
    summary = Table.grid(padding=(0, 2))
    summary.add_column(style="bold")
    summary.add_column()
    summary.add_row("DAG", analysis.dag_id)
    summary.add_row("Runs analyzed", str(analysis.runs_analyzed))
    summary.add_row("Overall severity", _severity_text(analysis.overall_severity))
    summary.add_row(
        "Task coverage",
        f"{dag_summary.analyzed_tasks}/{dag_summary.total_tasks} "
        f"({dag_summary.analysis_coverage_percent:.1f}%)",
    )
    summary.add_row("Anomalous tasks", str(dag_summary.anomalous_tasks))
    summary.add_row("Anomalous handoffs", str(dag_summary.anomalous_handoffs))
    summary.add_row("Affected tasks", str(dag_summary.affected_tasks))
    summary.add_row(
        "Structural signals",
        f"{dag_summary.change_points} change points, {dag_summary.trends} trends",
    )

    if analysis.primary_origin is not None:
        summary.add_row("Primary origin", analysis.primary_origin.task_id)
        summary.add_row("Classification", str(analysis.primary_origin.classification))
        summary.add_row(
            "Propagation score",
            f"{analysis.primary_origin.propagation_score:.2f}",
        )

    console.print(Panel(summary, title="FlowSense Analysis", expand=False))


def _render_task_drift(console: Console, analysis: DAGAnalysis) -> None:
    table = Table(title="Task Drift")
    table.add_column("Task")
    table.add_column("Baseline", justify="right")
    table.add_column("Current", justify="right")
    table.add_column("Deviation", justify="right")
    table.add_column("Z-Score", justify="right")
    table.add_column("Severity")
    table.add_column("Impact")

    ordered_results = sorted(
        analysis.drift_results.items(),
        key=lambda item: (-SEVERITY_SCORE[item[1].severity], item[0]),
    )

    for task_id, result in ordered_results:
        impact = analysis.task_impacts.get(task_id)
        table.add_row(
            task_id,
            f"{result.baseline:.2f}s",
            f"{result.current:.2f}s",
            f"{result.deviation_percent:+.1f}%",
            f"{result.robust_z_score:.2f}",
            _severity_text(result.severity),
            str(impact.classification) if impact else "-",
        )

    console.print(table)


def _render_handoff_drift(console: Console, analysis: DAGAnalysis) -> None:
    if not analysis.handoff_drift_results:
        return

    table = Table(title="Handoff Drift")
    table.add_column("Edge")
    table.add_column("Baseline", justify="right")
    table.add_column("Current", justify="right")
    table.add_column("Deviation", justify="right")
    table.add_column("Z-Score", justify="right")
    table.add_column("Severity")

    ordered_results = sorted(
        analysis.handoff_drift_results.items(),
        key=lambda item: (-SEVERITY_SCORE[item[1].severity], item[0]),
    )

    for (upstream, downstream), result in ordered_results:
        table.add_row(
            f"{upstream} -> {downstream}",
            f"{result.baseline:.2f}s",
            f"{result.current:.2f}s",
            f"{result.deviation_percent:+.1f}%",
            f"{result.robust_z_score:.2f}",
            _severity_text(result.severity),
        )

    console.print(table)


def _render_change_points(console: Console, analysis: DAGAnalysis) -> None:
    results = [
        *analysis.change_point_results.values(),
        *analysis.handoff_change_point_results.values(),
    ]
    if not results:
        return

    table = Table(title="Change Points")
    table.add_column("Subject")
    table.add_column("Direction")
    table.add_column("Observation", justify="right")
    table.add_column("Before", justify="right")
    table.add_column("After", justify="right")
    table.add_column("Change", justify="right")
    table.add_column("Score", justify="right")

    for result in sorted(results, key=lambda item: item.subject_id):
        table.add_row(
            result.subject_id,
            str(result.direction),
            str(result.change_index + 1),
            f"{result.before_median:.2f}s",
            f"{result.after_median:.2f}s",
            _percent(result.change_percent),
            f"{result.score:.2f}",
        )

    console.print(table)


def _render_trends(console: Console, analysis: DAGAnalysis) -> None:
    results = [
        *analysis.trend_results.values(),
        *analysis.handoff_trend_results.values(),
    ]
    if not results:
        return

    table = Table(title="Trends")
    table.add_column("Subject")
    table.add_column("Direction")
    table.add_column("Slope / run", justify="right")
    table.add_column("Est. change", justify="right")
    table.add_column("Change", justify="right")
    table.add_column("Consistency", justify="right")
    table.add_column("Score", justify="right")

    for result in sorted(results, key=lambda item: item.subject_id):
        table.add_row(
            result.subject_id,
            str(result.direction),
            f"{result.slope_per_observation:+.2f}s",
            f"{result.estimated_change:+.2f}s",
            _percent(result.change_percent),
            f"{result.directional_consistency:.0%}",
            f"{result.score:.2f}",
        )

    console.print(table)


def _render_propagation(console: Console, analysis: DAGAnalysis) -> None:
    if not analysis.propagation_results:
        return

    table = Table(title="Propagation")
    table.add_column("Origin")
    table.add_column("Path")
    table.add_column("Affected", justify="right")
    table.add_column("Score", justify="right")

    for result in sorted(
        analysis.propagation_results,
        key=lambda item: (item.origin_task, item.path),
    ):
        table.add_row(
            result.origin_task,
            " -> ".join(result.path),
            str(len(result.affected_tasks)),
            f"{result.propagation_score:.2f}",
        )

    console.print(table)


def _render_diagnostics(console: Console, analysis: DAGAnalysis) -> None:
    if not analysis.diagnostics:
        return

    table = Table(title="Diagnostics", title_style="bold yellow")
    table.add_column("Code", style="yellow")
    table.add_column("Subject")
    table.add_column("Message")

    for diagnostic in sorted(
        analysis.diagnostics,
        key=lambda item: (item.code, item.subject_id),
    ):
        table.add_row(diagnostic.code, diagnostic.subject_id, diagnostic.message)

    console.print(table)


def render_analysis(console: Console, analysis: DAGAnalysis) -> None:
    """Render a complete human-readable analysis report."""
    _render_summary(console, analysis)
    _render_task_drift(console, analysis)
    _render_handoff_drift(console, analysis)
    _render_change_points(console, analysis)
    _render_trends(console, analysis)
    _render_propagation(console, analysis)
    _render_diagnostics(console, analysis)
