from __future__ import annotations

import math

import numpy as np

from flowsense.domain import ChangeDirection, ChangePointResult


def _candidate_score(
    before: np.ndarray, after: np.ndarray
) -> tuple[float, float, float]:
    before_median = float(np.median(before))
    after_median = float(np.median(after))
    difference = after_median - before_median

    residuals = np.concatenate(
        (
            np.abs(before - before_median),
            np.abs(after - after_median),
        )
    )
    noise = float(np.median(residuals))

    if math.isclose(noise, 0.0):
        score = 0.0 if math.isclose(difference, 0.0) else 5.0
    else:
        score = abs(0.6745 * difference / noise)

    return score, before_median, after_median


def detect_change_point(
    subject_id: str,
    values: list[float],
    *,
    minimum_segment_size: int = 3,
    score_threshold: float = 3.5,
) -> ChangePointResult | None:
    """Detect the strongest persistent level shift in an ordered time series."""
    if minimum_segment_size < 2:
        raise ValueError("minimum_segment_size must be at least 2")

    if score_threshold <= 0:
        raise ValueError("score_threshold must be positive")

    if len(values) < minimum_segment_size * 2:
        return None

    observations = np.asarray(values, dtype=float)
    best_candidate: tuple[float, int, float, float] | None = None

    for change_index in range(
        minimum_segment_size,
        len(observations) - minimum_segment_size + 1,
    ):
        score, before_median, after_median = _candidate_score(
            observations[:change_index],
            observations[change_index:],
        )

        if best_candidate is None or score > best_candidate[0]:
            best_candidate = (score, change_index, before_median, after_median)

    if best_candidate is None or best_candidate[0] < score_threshold:
        return None

    score, change_index, before_median, after_median = best_candidate
    change_percent = (
        (after_median - before_median) / before_median * 100
        if not math.isclose(before_median, 0.0)
        else None
    )

    return ChangePointResult(
        subject_id=subject_id,
        change_index=change_index,
        before_median=before_median,
        after_median=after_median,
        change_percent=change_percent,
        score=score,
        direction=(
            ChangeDirection.INCREASE
            if after_median > before_median
            else ChangeDirection.DECREASE
        ),
    )
