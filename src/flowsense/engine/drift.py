from __future__ import annotations

from dataclasses import dataclass

import numpy as np


@dataclass
class DriftResult:
    task_id: str
    baseline: float
    current: float
    mad: float
    robust_z_score: float
    deviation_percent: float
    severity: str


def calculate_drift(
    task_id: str,
    durations: list[float],
) -> DriftResult:
    if len(durations) < 5:
        raise ValueError(f"{task_id} için drift hesaplamak için en az 5 run gerekli.")

    baseline_values = np.array(
        durations[:-1],
        dtype=float,
    )

    current = float(durations[-1])

    median = float(np.median(baseline_values))

    absolute_deviations = np.abs(baseline_values - median)

    mad = float(np.median(absolute_deviations))

    if mad == 0:
        robust_z_score = 0.0
    else:
        robust_z_score = 0.6745 * (current - median) / mad

    if median == 0:
        deviation_percent = 0.0
    else:
        deviation_percent = (current - median) / median * 100

    absolute_z = abs(robust_z_score)

    if absolute_z >= 5:
        severity = "CRITICAL"
    elif absolute_z >= 3.5:
        severity = "HIGH"
    elif absolute_z >= 2:
        severity = "MEDIUM"
    else:
        severity = "NORMAL"

    return DriftResult(
        task_id=task_id,
        baseline=median,
        current=current,
        mad=mad,
        robust_z_score=robust_z_score,
        deviation_percent=deviation_percent,
        severity=severity,
    )
