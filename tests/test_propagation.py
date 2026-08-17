from flowsense.engine.drift import DriftResult
from flowsense.engine.propagation import analyze_propagation


def test_detects_downstream_propagation() -> None:
    drift_results = {
        "extract": DriftResult(
            task_id="extract",
            baseline=1.5,
            current=1.6,
            mad=0.1,
            robust_z_score=0.3,
            deviation_percent=6.7,
            severity="NORMAL",
        ),
        "transform": DriftResult(
            task_id="transform",
            baseline=3.0,
            current=9.0,
            mad=0.5,
            robust_z_score=8.0,
            deviation_percent=200.0,
            severity="CRITICAL",
        ),
        "load": DriftResult(
            task_id="load",
            baseline=1.4,
            current=2.0,
            mad=0.2,
            robust_z_score=2.5,
            deviation_percent=42.9,
            severity="MEDIUM",
        ),
    }

    dependencies = {
        "extract": ["transform"],
        "transform": ["load"],
        "load": [],
    }

    results = analyze_propagation(
        drift_results,
        dependencies,
    )

    assert len(results) == 1

    result = results[0]

    assert result.origin_task == "transform"
    assert result.affected_tasks == ["load"]
    assert result.path == ["transform", "load"]
    assert result.propagation_score > 0


def test_no_propagation_when_downstream_is_normal() -> None:
    drift_results = {
        "transform": DriftResult(
            task_id="transform",
            baseline=3.0,
            current=9.0,
            mad=0.5,
            robust_z_score=8.0,
            deviation_percent=200.0,
            severity="CRITICAL",
        ),
        "load": DriftResult(
            task_id="load",
            baseline=1.4,
            current=1.5,
            mad=0.2,
            robust_z_score=0.3,
            deviation_percent=7.1,
            severity="NORMAL",
        ),
    }

    dependencies = {
        "transform": ["load"],
        "load": [],
    }

    results = analyze_propagation(
        drift_results,
        dependencies,
    )

    assert results == []


def test_medium_task_is_not_treated_as_origin() -> None:
    drift_results = {
        "extract": DriftResult(
            task_id="extract",
            baseline=1.5,
            current=2.2,
            mad=0.2,
            robust_z_score=2.5,
            deviation_percent=46.7,
            severity="MEDIUM",
        ),
        "transform": DriftResult(
            task_id="transform",
            baseline=3.0,
            current=4.0,
            mad=0.3,
            robust_z_score=2.4,
            deviation_percent=33.3,
            severity="MEDIUM",
        ),
    }

    dependencies = {
        "extract": ["transform"],
        "transform": [],
    }

    results = analyze_propagation(
        drift_results,
        dependencies,
    )

    assert results == []
