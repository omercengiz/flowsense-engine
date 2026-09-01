import pytest

from flowsense import AnalysisPolicy, MappedTaskAggregation
from flowsense.engine.drift import calculate_drift
from flowsense.engine.history import build_duration_history
from flowsense.models import TaskRun


def test_policy_validates_history_and_thresholds() -> None:
    with pytest.raises(ValueError):
        AnalysisPolicy(minimum_history=1)

    with pytest.raises(ValueError):
        AnalysisPolicy(
            medium_threshold=3.5,
            high_threshold=2.0,
        )

    with pytest.raises(ValueError):
        AnalysisPolicy(minimum_history=5, baseline_window=3)


def test_drift_uses_configured_minimum_history_and_thresholds() -> None:
    policy = AnalysisPolicy(
        minimum_history=3,
        medium_threshold=1.0,
        high_threshold=2.0,
        critical_threshold=3.0,
    )

    result = calculate_drift(
        "transform",
        [1.0, 1.1, 2.0],
        policy=policy,
    )

    assert result.severity == "CRITICAL"


def test_drift_uses_recent_baseline_window() -> None:
    policy = AnalysisPolicy(minimum_history=3, baseline_window=2)

    result = calculate_drift(
        "transform",
        [100.0, 100.0, 2.0, 2.2, 2.1],
        policy=policy,
    )

    assert result.baseline == pytest.approx(2.1)
    assert result.severity == "NORMAL"


@pytest.mark.parametrize(
    ("aggregation", "expected"),
    [
        (MappedTaskAggregation.MAX, 5.0),
        (MappedTaskAggregation.MEAN, 3.5),
        (MappedTaskAggregation.SUM, 7.0),
    ],
)
def test_history_uses_mapped_task_aggregation(
    aggregation: MappedTaskAggregation,
    expected: float,
) -> None:
    runs = [
        TaskRun(
            dag_id="demo",
            dag_run_id="run_1",
            task_id="mapped",
            state="success",
            duration=duration,
            map_index=index,
        )
        for index, duration in enumerate([2.0, 5.0])
    ]

    assert build_duration_history(runs, aggregation=aggregation) == {
        "mapped": [expected]
    }
