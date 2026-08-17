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


def test_calculate_drift_requires_minimum_history() -> None:
    durations = [
        3.0,
        3.1,
        3.2,
        3.3,
    ]

    try:
        calculate_drift(
            "transform",
            durations,
        )
    except ValueError as exc:
        assert "en az 5 run" in str(exc)
    else:
        raise AssertionError("Expected ValueError for insufficient history.")
