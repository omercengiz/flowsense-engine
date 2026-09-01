import pytest

from flowsense.engine.trend import detect_trend


def test_detects_increasing_trend() -> None:
    result = detect_trend("transform", [10.0, 11.1, 12.0, 13.2, 14.0, 15.1])

    assert result is not None
    assert result.subject_id == "transform"
    assert result.direction == "INCREASING"
    assert result.slope_per_observation == pytest.approx(1.0, abs=0.1)
    assert result.estimated_change == pytest.approx(5.0, abs=0.5)
    assert result.change_percent is not None
    assert result.change_percent > 45
    assert result.directional_consistency == 1.0
    assert result.observations == 6


def test_detects_decreasing_trend() -> None:
    result = detect_trend("transform", [20.0, 18.0, 16.0, 14.0, 12.0, 10.0])

    assert result is not None
    assert result.direction == "DECREASING"
    assert result.slope_per_observation == -2.0
    assert result.estimated_change == -10.0


def test_does_not_treat_level_shift_as_gradual_trend() -> None:
    result = detect_trend("transform", [10.0, 10.0, 10.0, 20.0, 20.0, 20.0])

    assert result is None


def test_does_not_treat_latest_outlier_as_trend() -> None:
    result = detect_trend("transform", [10.0, 10.1, 9.9, 10.0, 10.2, 50.0])

    assert result is None


def test_returns_none_for_short_or_stable_history() -> None:
    assert detect_trend("short", [1.0, 2.0, 3.0, 4.0]) is None
    assert detect_trend("stable", [1.0] * 6) is None


def test_validates_detector_configuration() -> None:
    with pytest.raises(ValueError, match="minimum_observations"):
        detect_trend("task", [1.0] * 5, minimum_observations=2)

    with pytest.raises(ValueError, match="score_threshold"):
        detect_trend("task", [1.0] * 5, score_threshold=0.0)

    with pytest.raises(ValueError, match="minimum_directional_consistency"):
        detect_trend("task", [1.0] * 5, minimum_directional_consistency=1.1)
