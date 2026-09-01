from __future__ import annotations

import math

import numpy as np

from flowsense.domain import (
    DEFAULT_ANALYSIS_POLICY,
    AnalysisPolicy,
    DriftResult,
    InsufficientHistoryError,
    Severity,
)


def calculate_drift(
    task_id: str,
    durations: list[float],
    policy: AnalysisPolicy = DEFAULT_ANALYSIS_POLICY,
) -> DriftResult:
    if len(durations) < policy.minimum_history:
        raise InsufficientHistoryError(
            subject_id=task_id,
            required=policy.minimum_history,
            actual=len(durations),
        )

    baseline_durations = durations[:-1]
    if policy.baseline_window is not None:
        baseline_durations = baseline_durations[-policy.baseline_window :]

    baseline_values = np.array(
        baseline_durations,
        dtype=float,
    )

    current = float(durations[-1])

    median = float(np.median(baseline_values))

    absolute_deviations = np.abs(baseline_values - median)

    mad = float(np.median(absolute_deviations))

    if mad == 0:
        if math.isclose(current, median):
            robust_z_score = 0.0
        else:
            robust_z_score = math.copysign(
                policy.critical_threshold,
                current - median,
            )
    else:
        robust_z_score = 0.6745 * (current - median) / mad

    if median == 0:
        deviation_percent = 0.0
    else:
        deviation_percent = (current - median) / median * 100

    absolute_z = abs(robust_z_score)

    if absolute_z >= policy.critical_threshold:
        severity = Severity.CRITICAL
    elif absolute_z >= policy.high_threshold:
        severity = Severity.HIGH
    elif absolute_z >= policy.medium_threshold:
        severity = Severity.MEDIUM
    else:
        severity = Severity.NORMAL

    return DriftResult(
        task_id=task_id,
        baseline=median,
        current=current,
        mad=mad,
        robust_z_score=robust_z_score,
        deviation_percent=deviation_percent,
        severity=severity,
    )
