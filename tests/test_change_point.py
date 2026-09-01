import pytest

from flowsense.engine.change_point import detect_change_point


def test_detects_persistent_increase() -> None:
    result = detect_change_point(
        "transform",
        [10.0, 10.2, 9.8, 20.0, 20.2, 19.8],
    )

    assert result is not None
    assert result.subject_id == "transform"
    assert result.change_index == 3
    assert result.before_median == 10.0
    assert result.after_median == 20.0
    assert result.change_percent == 100.0
    assert result.direction == "INCREASE"
    assert result.score >= 3.5


def test_detects_persistent_decrease() -> None:
    result = detect_change_point(
        "transform",
        [20.0, 20.2, 19.8, 10.0, 10.2, 9.8],
    )

    assert result is not None
    assert result.change_index == 3
    assert result.direction == "DECREASE"
    assert result.change_percent == -50.0


def test_does_not_treat_latest_outlier_as_change_point() -> None:
    result = detect_change_point(
        "transform",
        [10.0, 10.1, 9.9, 10.0, 10.2, 50.0],
    )

    assert result is None


def test_returns_none_for_short_or_stable_history() -> None:
    assert detect_change_point("short", [1.0, 1.0, 1.0, 2.0, 2.0]) is None
    assert detect_change_point("stable", [1.0] * 8) is None


def test_validates_detector_configuration() -> None:
    with pytest.raises(ValueError, match="minimum_segment_size"):
        detect_change_point("task", [1.0] * 6, minimum_segment_size=1)

    with pytest.raises(ValueError, match="score_threshold"):
        detect_change_point("task", [1.0] * 6, score_threshold=0.0)
