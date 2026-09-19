"""Deprecated compatibility namespace for domain models."""

import warnings

from flowsense.domain import AnalysisDiagnostic, DAGAnalysis, TaskRun

warnings.warn(
    "flowsense.models is deprecated; use flowsense.domain instead.",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = [
    "AnalysisDiagnostic",
    "DAGAnalysis",
    "TaskRun",
]
