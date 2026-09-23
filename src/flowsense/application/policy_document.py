from typing import Literal

from pydantic import BaseModel, ConfigDict

from flowsense.domain import MappedTaskAggregation

ANALYSIS_POLICY_SCHEMA_VERSION = "1.0"


class AnalysisPolicyDocument(BaseModel):
    """Versioned external representation of an analysis policy."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["1.0"]
    minimum_history: int = 5
    baseline_window: int | None = None
    medium_threshold: float = 2.0
    high_threshold: float = 3.5
    critical_threshold: float = 5.0
    minimum_relative_dispersion: float = 0.01
    minimum_absolute_dispersion: float = 0.001
    mapped_task_aggregation: MappedTaskAggregation = MappedTaskAggregation.MAX
    change_point_detection: bool = True
    change_point_minimum_segment_size: int = 3
    change_point_score_threshold: float = 3.5
    trend_detection: bool = True
    trend_minimum_observations: int = 5
    trend_score_threshold: float = 3.5
    trend_minimum_directional_consistency: float = 0.6


def analysis_policy_json_schema() -> dict[str, object]:
    """Return the versioned analysis-policy JSON Schema."""
    return AnalysisPolicyDocument.model_json_schema()
