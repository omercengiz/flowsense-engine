from __future__ import annotations

from mcp.server import MCPServer

from flowsense.application import analyze_dag
from flowsense.domain import (
    AnalysisPolicy,
    DAGAnalysis,
    MappedTaskAggregation,
)
from flowsense.infrastructure.airflow import AirflowApiError, AirflowClient

mcp = MCPServer("FlowSense Engine")


def serialize_analysis(analysis: DAGAnalysis) -> dict:
    return {
        "dag_id": analysis.dag_id,
        "runs_analyzed": analysis.runs_analyzed,
        "overall_severity": analysis.overall_severity,
        "policy": {
            "minimum_history": analysis.policy.minimum_history,
            "baseline_window": analysis.policy.baseline_window,
            "medium_threshold": analysis.policy.medium_threshold,
            "high_threshold": analysis.policy.high_threshold,
            "critical_threshold": analysis.policy.critical_threshold,
            "mapped_task_aggregation": analysis.policy.mapped_task_aggregation,
        },
        "primary_origin": (
            {
                "task_id": analysis.primary_origin.task_id,
                "classification": analysis.primary_origin.classification,
                "severity": analysis.primary_origin.severity,
                "propagation_score": analysis.primary_origin.propagation_score,
            }
            if analysis.primary_origin
            else None
        ),
        "drift_results": {
            task_id: {
                "baseline": result.baseline,
                "current": result.current,
                "mad": result.mad,
                "robust_z_score": result.robust_z_score,
                "deviation_percent": result.deviation_percent,
                "severity": result.severity,
            }
            for task_id, result in analysis.drift_results.items()
        },
        "change_point_results": {
            task_id: {
                "change_index": result.change_index,
                "before_median": result.before_median,
                "after_median": result.after_median,
                "change_percent": result.change_percent,
                "score": result.score,
                "direction": result.direction,
            }
            for task_id, result in analysis.change_point_results.items()
        },
        "handoff_drift_results": {
            f"{upstream}->{downstream}": {
                "baseline": result.baseline,
                "current": result.current,
                "mad": result.mad,
                "robust_z_score": result.robust_z_score,
                "deviation_percent": result.deviation_percent,
                "severity": result.severity,
            }
            for (
                upstream,
                downstream,
            ), result in analysis.handoff_drift_results.items()
        },
        "handoff_change_point_results": {
            f"{upstream}->{downstream}": {
                "change_index": result.change_index,
                "before_median": result.before_median,
                "after_median": result.after_median,
                "change_percent": result.change_percent,
                "score": result.score,
                "direction": result.direction,
            }
            for (
                upstream,
                downstream,
            ), result in analysis.handoff_change_point_results.items()
        },
        "task_impacts": {
            task_id: {
                "classification": impact.classification,
                "task_severity": impact.task_severity,
                "upstream_handoff_severity": impact.upstream_handoff_severity,
            }
            for task_id, impact in analysis.task_impacts.items()
        },
        "propagation_results": [
            {
                "origin_task": result.origin_task,
                "affected_tasks": result.affected_tasks,
                "path": result.path,
                "propagation_score": result.propagation_score,
            }
            for result in analysis.propagation_results
        ],
        "dependencies": analysis.dependencies,
        "diagnostics": [
            {
                "code": diagnostic.code,
                "subject_id": diagnostic.subject_id,
                "message": diagnostic.message,
            }
            for diagnostic in analysis.diagnostics
        ],
    }


@mcp.tool()
def analyze_airflow_dag(
    dag_id: str,
    minimum_history: int = 5,
    baseline_window: int | None = None,
    medium_threshold: float = 2.0,
    high_threshold: float = 3.5,
    critical_threshold: float = 5.0,
    mapped_task_aggregation: MappedTaskAggregation = MappedTaskAggregation.MAX,
) -> dict:
    """Analyze an Apache Airflow DAG for temporal drift and propagation."""
    try:
        with AirflowClient() as source:
            analysis = analyze_dag(
                dag_id=dag_id,
                source=source,
                policy=AnalysisPolicy(
                    minimum_history=minimum_history,
                    baseline_window=baseline_window,
                    medium_threshold=medium_threshold,
                    high_threshold=high_threshold,
                    critical_threshold=critical_threshold,
                    mapped_task_aggregation=mapped_task_aggregation,
                ),
            )
    except AirflowApiError as exc:
        raise RuntimeError(str(exc)) from exc

    return serialize_analysis(analysis)


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
