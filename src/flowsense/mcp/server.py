from __future__ import annotations

from mcp.server import MCPServer

from flowsense.application import AnalysisRequest, AnalyzeDAG, serialize_analysis
from flowsense.domain import AnalysisPolicy, FlowSenseError, MappedTaskAggregation
from flowsense.infrastructure.airflow import create_airflow_data_source

mcp = MCPServer("FlowSense Engine")


@mcp.tool()
def analyze_airflow_dag(
    dag_id: str,
    minimum_history: int = 5,
    baseline_window: int | None = None,
    medium_threshold: float = 2.0,
    high_threshold: float = 3.5,
    critical_threshold: float = 5.0,
    minimum_relative_dispersion: float = 0.01,
    minimum_absolute_dispersion: float = 0.001,
    change_point_detection_enabled: bool = True,
    change_point_minimum_segment_size: int = 3,
    change_point_score_threshold: float = 3.5,
    trend_detection_enabled: bool = True,
    trend_minimum_observations: int = 5,
    trend_score_threshold: float = 3.5,
    trend_minimum_directional_consistency: float = 0.6,
    mapped_task_aggregation: MappedTaskAggregation = MappedTaskAggregation.MAX,
    history_run_limit: int | None = None,
    dag_run_id: str | None = None,
) -> dict[str, object]:
    """Analyze an Apache Airflow DAG for temporal drift and propagation."""
    try:
        request = AnalysisRequest(
            dag_id=dag_id,
            policy=AnalysisPolicy(
                minimum_history=minimum_history,
                baseline_window=baseline_window,
                medium_threshold=medium_threshold,
                high_threshold=high_threshold,
                critical_threshold=critical_threshold,
                minimum_relative_dispersion=minimum_relative_dispersion,
                minimum_absolute_dispersion=minimum_absolute_dispersion,
                mapped_task_aggregation=mapped_task_aggregation,
                change_point_detection_enabled=change_point_detection_enabled,
                change_point_minimum_segment_size=change_point_minimum_segment_size,
                change_point_score_threshold=change_point_score_threshold,
                trend_detection_enabled=trend_detection_enabled,
                trend_minimum_observations=trend_minimum_observations,
                trend_score_threshold=trend_score_threshold,
                trend_minimum_directional_consistency=(
                    trend_minimum_directional_consistency
                ),
            ),
            history_run_limit=history_run_limit,
            dag_run_id=dag_run_id,
        )
        analysis = AnalyzeDAG(create_airflow_data_source).execute(request)
    except FlowSenseError as exc:
        raise RuntimeError(str(exc)) from exc

    return serialize_analysis(analysis)


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
