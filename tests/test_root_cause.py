from flowsense.engine.drift import DriftResult
from flowsense.engine.impact import TaskImpact
from flowsense.engine.propagation import PropagationResult
from flowsense.engine.root_cause import select_primary_origin


def _drift(
    task_id: str,
    severity: str,
) -> DriftResult:
    return DriftResult(
        task_id=task_id,
        baseline=1.0,
        current=2.0,
        mad=0.1,
        robust_z_score=3.0,
        deviation_percent=100.0,
        severity=severity,
    )


def _impact(
    task_id: str,
    classification: str,
    severity: str,
) -> TaskImpact:
    return TaskImpact(
        task_id=task_id,
        classification=classification,
        task_severity=severity,
        upstream_handoff_severity=None,
    )


def test_selects_single_own_drift_as_primary_origin() -> None:
    drift_results = {
        "transform": _drift(
            "transform",
            "CRITICAL",
        ),
    }

    task_impacts = {
        "transform": _impact(
            "transform",
            "OWN_DRIFT",
            "CRITICAL",
        ),
    }

    result = select_primary_origin(
        drift_results=drift_results,
        task_impacts=task_impacts,
        dependencies={
            "transform": [],
        },
        propagation_results=[],
    )

    assert result is not None
    assert result.task_id == "transform"


def test_inherited_delay_is_not_primary_origin() -> None:
    drift_results = {
        "transform": _drift(
            "transform",
            "NORMAL",
        ),
    }

    task_impacts = {
        "transform": _impact(
            "transform",
            "INHERITED_DELAY",
            "NORMAL",
        ),
    }

    result = select_primary_origin(
        drift_results=drift_results,
        task_impacts=task_impacts,
        dependencies={
            "transform": [],
        },
        propagation_results=[],
    )

    assert result is None


def test_prefers_upstream_root_over_downstream_combined_task() -> None:
    drift_results = {
        "extract": _drift(
            "extract",
            "CRITICAL",
        ),
        "transform": _drift(
            "transform",
            "CRITICAL",
        ),
    }

    task_impacts = {
        "extract": _impact(
            "extract",
            "OWN_DRIFT",
            "CRITICAL",
        ),
        "transform": _impact(
            "transform",
            "COMBINED",
            "CRITICAL",
        ),
    }

    result = select_primary_origin(
        drift_results=drift_results,
        task_impacts=task_impacts,
        dependencies={
            "extract": ["transform"],
            "transform": [],
        },
        propagation_results=[],
    )

    assert result is not None
    assert result.task_id == "extract"


def test_selects_more_severe_independent_root() -> None:
    drift_results = {
        "extract_a": _drift(
            "extract_a",
            "MEDIUM",
        ),
        "extract_b": _drift(
            "extract_b",
            "CRITICAL",
        ),
    }

    task_impacts = {
        "extract_a": _impact(
            "extract_a",
            "OWN_DRIFT",
            "MEDIUM",
        ),
        "extract_b": _impact(
            "extract_b",
            "OWN_DRIFT",
            "CRITICAL",
        ),
    }

    result = select_primary_origin(
        drift_results=drift_results,
        task_impacts=task_impacts,
        dependencies={
            "extract_a": [],
            "extract_b": [],
        },
        propagation_results=[],
    )

    assert result is not None
    assert result.task_id == "extract_b"


def test_prefers_higher_propagation_score_between_independent_roots() -> None:
    drift_results = {
        "extract_a": _drift(
            "extract_a",
            "CRITICAL",
        ),
        "extract_b": _drift(
            "extract_b",
            "CRITICAL",
        ),
    }

    task_impacts = {
        "extract_a": _impact(
            "extract_a",
            "OWN_DRIFT",
            "CRITICAL",
        ),
        "extract_b": _impact(
            "extract_b",
            "OWN_DRIFT",
            "CRITICAL",
        ),
    }

    propagation_results = [
        PropagationResult(
            origin_task="extract_a",
            affected_tasks=["load_a"],
            path=["extract_a", "load_a"],
            propagation_score=0.4,
        ),
        PropagationResult(
            origin_task="extract_b",
            affected_tasks=[
                "transform_b",
                "load_b",
            ],
            path=[
                "extract_b",
                "transform_b",
                "load_b",
            ],
            propagation_score=0.8,
        ),
    ]

    result = select_primary_origin(
        drift_results=drift_results,
        task_impacts=task_impacts,
        dependencies={
            "extract_a": ["load_a"],
            "load_a": [],
            "extract_b": ["transform_b"],
            "transform_b": ["load_b"],
            "load_b": [],
        },
        propagation_results=propagation_results,
    )

    assert result is not None
    assert result.task_id == "extract_b"
    assert result.propagation_score == 0.8
