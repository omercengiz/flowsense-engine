import pytest

from flowsense.domain import InsufficientHistoryError
from flowsense.engine.drift import calculate_drift


def test_calculate_drift_normal() -> None:
    durations = [
        3.0,
        3.1,
        2.9,
        3.2,
        3.0,
        3.1,
    ]

    result = calculate_drift(
        "transform",
        durations,
    )

    assert result.task_id == "transform"
    assert result.severity == "NORMAL"
    assert abs(result.baseline - 3.0) < 0.2


def test_calculate_drift_critical() -> None:
    durations = [
        3.0,
        3.1,
        2.9,
        3.2,
        3.0,
        9.5,
    ]

    result = calculate_drift(
        "transform",
        durations,
    )

    assert result.severity == "CRITICAL"
    assert result.current == 9.5
    assert result.deviation_percent > 100


def test_calculate_drift_detects_change_when_mad_is_zero() -> None:
    result = calculate_drift(
        "transform",
        [3.0, 3.0, 3.0, 3.0, 9.0],
    )

    assert result.mad == 0.0
    assert result.robust_z_score == 5.0
    assert result.severity == "CRITICAL"


def test_calculate_drift_remains_normal_when_mad_and_change_are_zero() -> None:
    result = calculate_drift(
        "transform",
        [3.0, 3.0, 3.0, 3.0, 3.0],
    )

    assert result.mad == 0.0
    assert result.robust_z_score == 0.0
    assert result.severity == "NORMAL"


def test_calculate_drift_requires_minimum_history() -> None:
    durations = [
        3.0,
        3.1,
        3.2,
        3.3,
    ]

    with pytest.raises(InsufficientHistoryError) as exc_info:
        calculate_drift(
            "transform",
            durations,
        )

    assert exc_info.value.subject_id == "transform"
    assert exc_info.value.required == 5
    assert exc_info.value.actual == 4
