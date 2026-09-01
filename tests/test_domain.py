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
)
from flowsense.engine.drift import DriftResult as LegacyDriftResult
from flowsense.engine.impact import TaskImpact as LegacyTaskImpact
from flowsense.engine.propagation import PropagationResult as LegacyPropagationResult
from flowsense.engine.root_cause import RootCauseResult as LegacyRootCauseResult
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


def test_legacy_model_imports_reexport_domain_types() -> None:
    assert LegacyAnalysisDiagnostic is AnalysisDiagnostic
    assert LegacyDAGAnalysis is DAGAnalysis
    assert LegacyDriftResult is DriftResult
    assert LegacyPropagationResult is PropagationResult
    assert LegacyRootCauseResult is RootCauseResult
    assert LegacyTaskImpact is TaskImpact
    assert LegacyTaskRun is TaskRun
