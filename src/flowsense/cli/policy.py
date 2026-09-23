import json
from pathlib import Path

import typer
from pydantic import ValidationError

from flowsense.application import AnalysisPolicyDocument
from flowsense.domain import (
    AnalysisPolicy,
    ConfigurationError,
    MappedTaskAggregation,
)

POLICY_OPTION_NAMES = (
    "minimum_history",
    "baseline_window",
    "medium_threshold",
    "high_threshold",
    "critical_threshold",
    "minimum_relative_dispersion",
    "minimum_absolute_dispersion",
    "mapped_task_aggregation",
    "change_point_detection",
    "change_point_minimum_segment_size",
    "change_point_score_threshold",
    "trend_detection",
    "trend_minimum_observations",
    "trend_score_threshold",
    "trend_minimum_directional_consistency",
)


def build_analysis_policy(
    *,
    minimum_history: int,
    baseline_window: int | None,
    medium_threshold: float,
    high_threshold: float,
    critical_threshold: float,
    minimum_relative_dispersion: float,
    minimum_absolute_dispersion: float,
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
        minimum_relative_dispersion=minimum_relative_dispersion,
        minimum_absolute_dispersion=minimum_absolute_dispersion,
        mapped_task_aggregation=mapped_task_aggregation,
        change_point_detection_enabled=change_point_detection,
        change_point_minimum_segment_size=change_point_minimum_segment_size,
        change_point_score_threshold=change_point_score_threshold,
        trend_detection_enabled=trend_detection,
        trend_minimum_observations=trend_minimum_observations,
        trend_score_threshold=trend_score_threshold,
        trend_minimum_directional_consistency=(trend_minimum_directional_consistency),
    )


def resolve_analysis_policy(
    context: typer.Context,
    policy_file: Path | None,
    **options: object,
) -> AnalysisPolicy:
    """Load a policy document and apply explicitly supplied CLI overrides."""
    if policy_file is None:
        return build_analysis_policy(**options)  # type: ignore[arg-type]

    document = _load_policy_document(policy_file)
    resolved = document.model_dump(exclude={"schema_version"})
    for name in POLICY_OPTION_NAMES:
        source = context.get_parameter_source(name)
        if source is not None and source.name == "COMMANDLINE":
            resolved[name] = options[name]
    return build_analysis_policy(**resolved)


def load_analysis_policy(path: Path) -> AnalysisPolicy:
    """Load a versioned policy document without command-line overrides."""
    document = _load_policy_document(path)
    values = document.model_dump(exclude={"schema_version"})
    return build_analysis_policy(**values)


def _load_policy_document(path: Path) -> AnalysisPolicyDocument:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return AnalysisPolicyDocument.model_validate(payload)
    except (OSError, json.JSONDecodeError, ValidationError) as exc:
        raise ConfigurationError(
            f"Invalid analysis policy file '{path}': {exc}"
        ) from exc
