from collections.abc import Callable

import pytest

from flowsense.domain import InvalidObservationError
from flowsense.engine.change_point import detect_change_point
from flowsense.engine.drift import calculate_drift
from flowsense.engine.trend import detect_trend


@pytest.mark.parametrize(
    "analyze",
    [
        lambda values: calculate_drift("extract", values),
        lambda values: detect_change_point("extract", values),
        lambda values: detect_trend("extract", values),
    ],
)
@pytest.mark.parametrize("invalid_value", [float("nan"), float("inf"), -float("inf")])
def test_analysis_rejects_non_finite_observations(
    analyze: Callable[[list[float]], object],
    invalid_value: float,
) -> None:
    values = [1.0, 1.1, 0.9, 1.0, invalid_value, 1.2]

    with pytest.raises(InvalidObservationError, match="extract"):
        analyze(values)
