from flowsense.engine.drift import DriftResult
from flowsense.engine.propagation import analyze_propagation


def _drift(task_id: str, severity: str) -> DriftResult:
    return DriftResult(
        task_id=task_id,
        baseline=1.0,
        current=2.0,
        mad=0.1,
        robust_z_score=3.0,
        deviation_percent=100.0,
        severity=severity,
    )


def test_propagation_score_is_bounded_when_downstream_is_more_severe() -> None:
    drift_results = {
        "origin": _drift("origin", "HIGH"),
        "critical": _drift("critical", "CRITICAL"),
    }

    results = analyze_propagation(
        drift_results,
        {"origin": ["critical"], "critical": []},
    )

    assert results[0].propagation_score == 1.0


def test_propagation_score_gives_closer_tasks_more_weight() -> None:
    closer_critical_results = analyze_propagation(
        {
            "origin": _drift("origin", "CRITICAL"),
            "critical": _drift("critical", "CRITICAL"),
            "medium": _drift("medium", "MEDIUM"),
        },
        {
            "origin": ["critical"],
            "critical": ["medium"],
            "medium": [],
        },
    )
    farther_critical_results = analyze_propagation(
        {
            "origin": _drift("origin", "CRITICAL"),
            "medium": _drift("medium", "MEDIUM"),
            "critical": _drift("critical", "CRITICAL"),
        },
        {
            "origin": ["medium"],
            "medium": ["critical"],
            "critical": [],
        },
    )

    closer_critical = closer_critical_results[0].propagation_score
    farther_critical = farther_critical_results[0].propagation_score

    assert closer_critical > farther_critical
    assert 0.0 <= farther_critical <= closer_critical <= 1.0


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


def test_detects_multi_hop_propagation() -> None:
    drift_results = {
        "extract": DriftResult(
            task_id="extract",
            baseline=1.0,
            current=4.0,
            mad=0.2,
            robust_z_score=6.0,
            deviation_percent=300.0,
            severity="CRITICAL",
        ),
        "transform": DriftResult(
            task_id="transform",
            baseline=3.0,
            current=6.0,
            mad=0.5,
            robust_z_score=4.0,
            deviation_percent=100.0,
            severity="HIGH",
        ),
        "load": DriftResult(
            task_id="load",
            baseline=1.0,
            current=1.8,
            mad=0.2,
            robust_z_score=2.5,
            deviation_percent=80.0,
            severity="MEDIUM",
        ),
    }

    dependencies = {
        "extract": ["transform"],
        "transform": ["load"],
        "load": [],
    }

    results = analyze_propagation(
        drift_results=drift_results,
        dependencies=dependencies,
    )

    extract_result = next(
        result for result in results if result.origin_task == "extract"
    )

    assert extract_result.path == [
        "extract",
        "transform",
        "load",
    ]
    assert extract_result.affected_tasks == [
        "transform",
        "load",
    ]
    assert extract_result.propagation_score > 0


def test_does_not_report_downstream_task_as_duplicate_origin() -> None:
    drift_results = {
        "extract": DriftResult(
            task_id="extract",
            baseline=1.0,
            current=4.0,
            mad=0.2,
            robust_z_score=6.0,
            deviation_percent=300.0,
            severity="CRITICAL",
        ),
        "transform": DriftResult(
            task_id="transform",
            baseline=3.0,
            current=6.0,
            mad=0.5,
            robust_z_score=4.0,
            deviation_percent=100.0,
            severity="HIGH",
        ),
        "load": DriftResult(
            task_id="load",
            baseline=1.0,
            current=1.8,
            mad=0.2,
            robust_z_score=2.5,
            deviation_percent=80.0,
            severity="MEDIUM",
        ),
    }

    dependencies = {
        "extract": ["transform"],
        "transform": ["load"],
        "load": [],
    }

    results = analyze_propagation(
        drift_results=drift_results,
        dependencies=dependencies,
    )

    origins = [result.origin_task for result in results]

    assert origins == ["extract"]


def test_detects_branching_propagation_paths() -> None:
    drift_results = {
        "extract": DriftResult(
            task_id="extract",
            baseline=1.0,
            current=4.0,
            mad=0.2,
            robust_z_score=6.0,
            deviation_percent=300.0,
            severity="CRITICAL",
        ),
        "transform_a": DriftResult(
            task_id="transform_a",
            baseline=2.0,
            current=5.0,
            mad=0.3,
            robust_z_score=4.5,
            deviation_percent=150.0,
            severity="HIGH",
        ),
        "load_a": DriftResult(
            task_id="load_a",
            baseline=1.0,
            current=1.8,
            mad=0.2,
            robust_z_score=2.5,
            deviation_percent=80.0,
            severity="MEDIUM",
        ),
        "transform_b": DriftResult(
            task_id="transform_b",
            baseline=2.0,
            current=4.0,
            mad=0.3,
            robust_z_score=3.8,
            deviation_percent=100.0,
            severity="HIGH",
        ),
        "load_b": DriftResult(
            task_id="load_b",
            baseline=1.0,
            current=1.6,
            mad=0.2,
            robust_z_score=2.2,
            deviation_percent=60.0,
            severity="MEDIUM",
        ),
    }

    dependencies = {
        "extract": ["transform_a", "transform_b"],
        "transform_a": ["load_a"],
        "load_a": [],
        "transform_b": ["load_b"],
        "load_b": [],
    }

    results = analyze_propagation(
        drift_results=drift_results,
        dependencies=dependencies,
    )

    extract_results = [result for result in results if result.origin_task == "extract"]

    paths = [result.path for result in extract_results]

    assert len(extract_results) == 2

    assert [
        "extract",
        "transform_a",
        "load_a",
    ] in paths

    assert [
        "extract",
        "transform_b",
        "load_b",
    ] in paths


def test_preserves_origin_after_normal_dependency_gap() -> None:
    drift_results = {
        "upstream": DriftResult(
            task_id="upstream",
            baseline=1.0,
            current=4.0,
            mad=0.2,
            robust_z_score=6.0,
            deviation_percent=300.0,
            severity="CRITICAL",
        ),
        "normal_bridge": DriftResult(
            task_id="normal_bridge",
            baseline=2.0,
            current=2.1,
            mad=0.3,
            robust_z_score=0.2,
            deviation_percent=5.0,
            severity="NORMAL",
        ),
        "independent_origin": DriftResult(
            task_id="independent_origin",
            baseline=3.0,
            current=7.0,
            mad=0.5,
            robust_z_score=5.4,
            deviation_percent=133.3,
            severity="CRITICAL",
        ),
        "downstream": DriftResult(
            task_id="downstream",
            baseline=1.0,
            current=1.8,
            mad=0.2,
            robust_z_score=2.5,
            deviation_percent=80.0,
            severity="MEDIUM",
        ),
    }

    dependencies = {
        "upstream": ["normal_bridge"],
        "normal_bridge": ["independent_origin"],
        "independent_origin": ["downstream"],
        "downstream": [],
    }

    results = analyze_propagation(
        drift_results=drift_results,
        dependencies=dependencies,
    )

    assert len(results) == 1
    assert results[0].origin_task == "independent_origin"
    assert results[0].path == ["independent_origin", "downstream"]
