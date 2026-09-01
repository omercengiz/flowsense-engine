from __future__ import annotations

from mcp.server import MCPServer

from flowsense.application import analyze_dag
from flowsense.domain import DAGAnalysis
from flowsense.infrastructure.airflow import AirflowClient

mcp = MCPServer("FlowSense Engine")


def serialize_analysis(analysis: DAGAnalysis) -> dict:
    return {
        "dag_id": analysis.dag_id,
        "runs_analyzed": analysis.runs_analyzed,
        "overall_severity": analysis.overall_severity,
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
def analyze_airflow_dag(dag_id: str) -> dict:
    """Analyze an Apache Airflow DAG for temporal drift and propagation."""
    with AirflowClient() as source:
        analysis = analyze_dag(
            dag_id=dag_id,
            source=source,
        )
    return serialize_analysis(analysis)


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
