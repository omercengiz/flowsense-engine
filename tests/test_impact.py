from flowsense.engine.drift import DriftResult
from flowsense.engine.impact import classify_task_impact


def test_classifies_inherited_delay() -> None:
    task_drift = DriftResult(
        task_id="transform",
        baseline=3.0,
        current=3.1,
        mad=0.2,
        robust_z_score=0.3,
        deviation_percent=3.3,
        severity="NORMAL",
    )

    upstream_handoff_drifts = [
        DriftResult(
            task_id="extract->transform",
            baseline=2.0,
            current=8.0,
            mad=0.2,
            robust_z_score=6.0,
            deviation_percent=300.0,
            severity="CRITICAL",
        )
    ]

    result = classify_task_impact(
        task_id="transform",
        task_drift=task_drift,
        upstream_handoff_drifts=upstream_handoff_drifts,
    )

    assert result.task_id == "transform"
    assert result.classification == "INHERITED_DELAY"
    assert result.task_severity == "NORMAL"
    assert result.upstream_handoff_severity == "CRITICAL"


def test_classifies_own_drift() -> None:
    task_drift = DriftResult(
        task_id="transform",
        baseline=3.0,
        current=9.0,
        mad=0.3,
        robust_z_score=6.5,
        deviation_percent=200.0,
        severity="CRITICAL",
    )

    upstream_handoff_drifts = [
        DriftResult(
            task_id="extract->transform",
            baseline=2.0,
            current=2.1,
            mad=0.2,
            robust_z_score=0.3,
            deviation_percent=5.0,
            severity="NORMAL",
        )
    ]

    result = classify_task_impact(
        task_id="transform",
        task_drift=task_drift,
        upstream_handoff_drifts=upstream_handoff_drifts,
    )

    assert result.classification == "OWN_DRIFT"
    assert result.task_severity == "CRITICAL"
    assert result.upstream_handoff_severity is None


def test_classifies_normal_task() -> None:
    task_drift = DriftResult(
        task_id="transform",
        baseline=3.0,
        current=3.1,
        mad=0.2,
        robust_z_score=0.2,
        deviation_percent=3.3,
        severity="NORMAL",
    )

    result = classify_task_impact(
        task_id="transform",
        task_drift=task_drift,
        upstream_handoff_drifts=[],
    )

    assert result.classification == "NORMAL"
    assert result.task_severity == "NORMAL"
    assert result.upstream_handoff_severity is None


def test_classifies_combined_impact() -> None:
    task_drift = DriftResult(
        task_id="transform",
        baseline=3.0,
        current=8.0,
        mad=0.3,
        robust_z_score=5.5,
        deviation_percent=166.7,
        severity="CRITICAL",
    )

    upstream_handoff_drifts = [
        DriftResult(
            task_id="extract->transform",
            baseline=2.0,
            current=7.0,
            mad=0.2,
            robust_z_score=5.0,
            deviation_percent=250.0,
            severity="CRITICAL",
        )
    ]

    result = classify_task_impact(
        task_id="transform",
        task_drift=task_drift,
        upstream_handoff_drifts=upstream_handoff_drifts,
    )

    assert result.classification == "COMBINED"
    assert result.task_severity == "CRITICAL"
    assert result.upstream_handoff_severity == "CRITICAL"


def test_classifies_medium_task_as_own_drift() -> None:
    task_drift = DriftResult(
        task_id="load",
        baseline=1.4,
        current=2.1,
        mad=0.2,
        robust_z_score=3.17,
        deviation_percent=50.0,
        severity="MEDIUM",
    )

    result = classify_task_impact(
        task_id="load",
        task_drift=task_drift,
        upstream_handoff_drifts=[],
    )

    assert result.classification == "OWN_DRIFT"
    assert result.task_severity == "MEDIUM"
    assert result.upstream_handoff_severity is None
