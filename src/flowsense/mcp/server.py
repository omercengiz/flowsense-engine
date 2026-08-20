from __future__ import annotations

from mcp.server import MCPServer

from flowsense.engine.analyzer import analyze_dag
from flowsense.models import DAGAnalysis

mcp = MCPServer("FlowSense Engine")


def serialize_analysis(analysis: DAGAnalysis) -> dict:
    return {
        "dag_id": analysis.dag_id,
        "runs_analyzed": analysis.runs_analyzed,
        "overall_severity": analysis.overall_severity,
        "primary_origin": analysis.primary_origin,
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
    }


@mcp.tool()
def analyze_airflow_dag(dag_id: str) -> dict:
    """Analyze an Apache Airflow DAG for temporal drift and propagation."""
    analysis = analyze_dag(dag_id)
    return serialize_analysis(analysis)


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
