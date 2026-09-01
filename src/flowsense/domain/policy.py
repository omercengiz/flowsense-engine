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


DEFAULT_ANALYSIS_POLICY = AnalysisPolicy()
