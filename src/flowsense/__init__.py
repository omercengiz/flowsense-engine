from importlib.metadata import PackageNotFoundError, version

from flowsense.application import (
    ANALYSIS_SCHEMA_VERSION,
    AnalysisDocument,
    DAGDataSource,
    analysis_json_schema,
    analyze_dag,
    build_analysis_document,
    serialize_analysis,
)
from flowsense.domain import (
    DEFAULT_ANALYSIS_POLICY,
    AnalysisDiagnostic,
    AnalysisPolicy,
    ChangeDirection,
    ChangePointResult,
    ConfigurationError,
    DAGAnalysis,
    DAGAnalysisSummary,
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
from flowsense.infrastructure.airflow import (
    AirflowApiError,
    AirflowClient,
    AirflowDataError,
)

try:
    __version__ = version("flowsense")
except PackageNotFoundError:
    __version__ = "0.0.0"

__all__ = [
    "ANALYSIS_SCHEMA_VERSION",
    "DEFAULT_ANALYSIS_POLICY",
    "AirflowApiError",
    "AirflowClient",
    "AirflowDataError",
    "AnalysisDiagnostic",
    "AnalysisDocument",
    "AnalysisPolicy",
    "ChangeDirection",
    "ChangePointResult",
    "ConfigurationError",
    "DAGAnalysis",
    "DAGAnalysisSummary",
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
    "analysis_json_schema",
    "analyze_dag",
    "build_analysis_document",
    "serialize_analysis",
]
