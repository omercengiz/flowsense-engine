from __future__ import annotations

from flowsense.domain import (
    DriftResult,
    ImpactClassification,
    Severity,
    TaskImpact,
)
from flowsense.domain.enums import SEVERITY_SCORE


def classify_task_impact(
    task_id: str,
    task_drift: DriftResult,
    upstream_handoff_drifts: list[DriftResult],
) -> TaskImpact:
    anomalous_handoffs = [
        drift
        for drift in upstream_handoff_drifts
        if drift.severity in {Severity.MEDIUM, Severity.HIGH, Severity.CRITICAL}
    ]

    task_is_anomalous = task_drift.severity in {
        Severity.MEDIUM,
        Severity.HIGH,
        Severity.CRITICAL,
    }

    if task_is_anomalous and anomalous_handoffs:
        classification = ImpactClassification.COMBINED
    elif task_is_anomalous:
        classification = ImpactClassification.OWN_DRIFT
    elif anomalous_handoffs:
        classification = ImpactClassification.INHERITED_DELAY
    else:
        classification = ImpactClassification.NORMAL

    upstream_handoff_severity = None

    if anomalous_handoffs:
        upstream_handoff_severity = max(
            anomalous_handoffs,
            key=lambda result: SEVERITY_SCORE[result.severity],
        ).severity

    return TaskImpact(
        task_id=task_id,
        classification=classification,
        task_severity=task_drift.severity,
        upstream_handoff_severity=upstream_handoff_severity,
    )
