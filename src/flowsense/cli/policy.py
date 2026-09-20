from flowsense.domain import AnalysisPolicy, MappedTaskAggregation


def build_analysis_policy(
    *,
    minimum_history: int,
    baseline_window: int | None,
    medium_threshold: float,
    high_threshold: float,
    critical_threshold: float,
    mapped_task_aggregation: MappedTaskAggregation,
    change_point_detection: bool,
    change_point_minimum_segment_size: int,
    change_point_score_threshold: float,
    trend_detection: bool,
    trend_minimum_observations: int,
    trend_score_threshold: float,
    trend_minimum_directional_consistency: float,
) -> AnalysisPolicy:
    """Translate CLI policy options into the domain policy."""
    return AnalysisPolicy(
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
        trend_minimum_directional_consistency=(trend_minimum_directional_consistency),
    )
