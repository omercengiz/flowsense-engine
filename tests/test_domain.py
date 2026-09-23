import warnings

import pytest

from flowsense.domain import (
    AnalysisDiagnostic,
    DAGAnalysis,
    DriftResult,
    ImpactClassification,
    PropagationResult,
    RootCauseResult,
    Severity,
    TaskImpact,
    TaskRun,
    severity_meets_threshold,
)
from flowsense.engine.drift import DriftResult as LegacyDriftResult
from flowsense.engine.impact import TaskImpact as LegacyTaskImpact
from flowsense.engine.propagation import PropagationResult as LegacyPropagationResult
from flowsense.engine.root_cause import RootCauseResult as LegacyRootCauseResult

with warnings.catch_warnings():
    warnings.simplefilter("ignore", DeprecationWarning)
    from flowsense.models import (
        AnalysisDiagnostic as LegacyAnalysisDiagnostic,
    )
    from flowsense.models import (
        DAGAnalysis as LegacyDAGAnalysis,
    )
    from flowsense.models import (
        TaskRun as LegacyTaskRun,
    )


def test_domain_enums_are_string_compatible() -> None:
    assert Severity.CRITICAL == "CRITICAL"
    assert ImpactClassification.OWN_DRIFT == "OWN_DRIFT"


def test_severity_threshold_comparison_uses_domain_ordering() -> None:
    assert severity_meets_threshold(Severity.HIGH, Severity.HIGH)
    assert severity_meets_threshold(Severity.CRITICAL, Severity.MEDIUM)
    assert not severity_meets_threshold(Severity.MEDIUM, Severity.HIGH)


@pytest.mark.parametrize("duration", [-1.0, float("nan"), float("inf"), -float("inf")])
def test_task_run_rejects_invalid_duration(duration: float) -> None:
    with pytest.raises(ValueError, match="duration must be finite and non-negative"):
        TaskRun(
            dag_id="demo",
            dag_run_id="run_1",
            task_id="extract",
            duration=duration,
        )


def test_task_run_is_an_immutable_domain_value() -> None:
    task_run = TaskRun(
        dag_id="demo",
        dag_run_id="run_1",
        task_id="extract",
        duration=1.0,
    )

    with pytest.raises(AttributeError):
        task_run.duration = 2.0


def test_legacy_model_imports_reexport_domain_types() -> None:
    assert LegacyAnalysisDiagnostic is AnalysisDiagnostic
    assert LegacyDAGAnalysis is DAGAnalysis
    assert LegacyDriftResult is DriftResult
    assert LegacyPropagationResult is PropagationResult
    assert LegacyRootCauseResult is RootCauseResult
    assert LegacyTaskImpact is TaskImpact
    assert LegacyTaskRun is TaskRun
