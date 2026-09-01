from __future__ import annotations

import math

import numpy as np

from flowsense.domain import TrendDirection, TrendResult


def _theil_sen_slope(observations: np.ndarray) -> float:
    slopes = [
        float((observations[end] - observations[start]) / (end - start))
        for start in range(len(observations) - 1)
        for end in range(start + 1, len(observations))
    ]
    return float(np.median(slopes))


def detect_trend(
    subject_id: str,
    values: list[float],
    *,
    minimum_observations: int = 5,
    score_threshold: float = 3.5,
    minimum_directional_consistency: float = 0.6,
) -> TrendResult | None:
    """Detect a sustained linear trend using a robust Theil-Sen slope."""
    if minimum_observations < 3:
        raise ValueError("minimum_observations must be at least 3")

    if score_threshold <= 0:
        raise ValueError("score_threshold must be positive")

    if not 0 < minimum_directional_consistency <= 1:
        raise ValueError("minimum_directional_consistency must be in (0, 1]")

    if len(values) < minimum_observations:
        return None

    observations = np.asarray(values, dtype=float)
    slope = _theil_sen_slope(observations)

    if math.isclose(slope, 0.0):
        return None

    consecutive_changes = np.diff(observations)
    aligned_changes = np.count_nonzero(consecutive_changes * slope > 0)
    directional_consistency = float(aligned_changes / len(consecutive_changes))

    if directional_consistency < minimum_directional_consistency:
        return None

    indices = np.arange(len(observations), dtype=float)
    intercept = float(np.median(observations - slope * indices))
    residuals = np.abs(observations - (intercept + slope * indices))
    residual_mad = float(np.median(residuals))
    estimated_change = slope * (len(observations) - 1)

    if math.isclose(residual_mad, 0.0):
        score = 5.0
    else:
        score = abs(0.6745 * estimated_change / residual_mad)

    if score < score_threshold:
        return None

    change_percent = (
        estimated_change / intercept * 100 if not math.isclose(intercept, 0.0) else None
    )

    return TrendResult(
        subject_id=subject_id,
        direction=(
            TrendDirection.INCREASING if slope > 0 else TrendDirection.DECREASING
        ),
        slope_per_observation=slope,
        estimated_change=estimated_change,
        change_percent=change_percent,
        score=score,
        directional_consistency=directional_consistency,
        observations=len(observations),
    )
