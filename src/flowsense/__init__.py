from importlib.metadata import PackageNotFoundError, version

from flowsense.application import DAGDataSource, analyze_dag
from flowsense.domain import (
    DEFAULT_ANALYSIS_POLICY,
    AnalysisDiagnostic,
    AnalysisPolicy,
    ChangeDirection,
    ChangePointResult,
    DAGAnalysis,
    DriftResult,
    FlowSenseError,
    ImpactClassification,
    InsufficientHistoryError,
    InvalidTaskTimingError,
    MappedTaskAggregation,
    PropagationResult,
    RootCauseResult,
    Severity,
    TaskImpact,
    TaskRun,
    TrendDirection,
    TrendResult,
)
from flowsense.infrastructure.airflow import AirflowApiError, AirflowClient

try:
    __version__ = version("flowsense")
except PackageNotFoundError:
    __version__ = "0.0.0"

__all__ = [
    "DEFAULT_ANALYSIS_POLICY",
    "AirflowApiError",
    "AirflowClient",
    "AnalysisDiagnostic",
    "AnalysisPolicy",
    "ChangeDirection",
    "ChangePointResult",
    "DAGAnalysis",
    "DAGDataSource",
    "DriftResult",
    "FlowSenseError",
    "ImpactClassification",
    "InsufficientHistoryError",
    "InvalidTaskTimingError",
    "MappedTaskAggregation",
    "PropagationResult",
    "RootCauseResult",
    "Severity",
    "TaskImpact",
    "TaskRun",
    "TrendDirection",
    "TrendResult",
    "__version__",
    "analyze_dag",
]
