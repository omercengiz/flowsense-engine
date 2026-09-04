from dataclasses import dataclass

from flowsense.domain.enums import MappedTaskAggregation


@dataclass(frozen=True)
class AnalysisPolicy:
    minimum_history: int = 5
    baseline_window: int | None = None
    medium_threshold: float = 2.0
    high_threshold: float = 3.5
    critical_threshold: float = 5.0
    mapped_task_aggregation: MappedTaskAggregation = MappedTaskAggregation.MAX
    change_point_detection_enabled: bool = True
    change_point_minimum_segment_size: int = 3
    change_point_score_threshold: float = 3.5
    trend_detection_enabled: bool = True
    trend_minimum_observations: int = 5
    trend_score_threshold: float = 3.5
    trend_minimum_directional_consistency: float = 0.6

    def __post_init__(self) -> None:
        if self.minimum_history < 2:
            raise ValueError("minimum_history must be at least 2.")
        if self.baseline_window is not None:
            if self.baseline_window < 1:
                raise ValueError("baseline_window must be at least 1.")
            if self.baseline_window < self.minimum_history - 1:
                raise ValueError(
                    "baseline_window must contain enough values for minimum_history."
                )
        if not (
            0 < self.medium_threshold < self.high_threshold < self.critical_threshold
        ):
            raise ValueError("Severity thresholds must be positive and increasing.")
        if self.change_point_minimum_segment_size < 2:
            raise ValueError("change_point_minimum_segment_size must be at least 2.")
        if self.change_point_score_threshold <= 0:
            raise ValueError("change_point_score_threshold must be positive.")
        if self.trend_minimum_observations < 3:
            raise ValueError("trend_minimum_observations must be at least 3.")
        if self.trend_score_threshold <= 0:
            raise ValueError("trend_score_threshold must be positive.")
        if not 0 < self.trend_minimum_directional_consistency <= 1:
            raise ValueError("trend_minimum_directional_consistency must be in (0, 1].")


DEFAULT_ANALYSIS_POLICY = AnalysisPolicy()
