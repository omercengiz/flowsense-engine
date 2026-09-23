import pytest
from pydantic import ValidationError

from flowsense import (
    ANALYSIS_SCHEMA_VERSION,
    AnalysisDocument,
    ChangeDirection,
    ChangePointResult,
    DAGAnalysis,
    Severity,
    analysis_json_schema,
    build_analysis_document,
    serialize_analysis,
)


def _empty_analysis() -> DAGAnalysis:
    return DAGAnalysis(
        dag_id="demo",
        runs_analyzed=0,
        overall_severity=Severity.NORMAL,
        primary_origin=None,
        drift_results={},
        handoff_drift_results={},
        task_impacts={},
        propagation_results=[],
        dependencies={},
    )


def test_builds_typed_immutable_analysis_document() -> None:
    document = build_analysis_document(_empty_analysis())

    assert isinstance(document, AnalysisDocument)
    assert document.schema_version == ANALYSIS_SCHEMA_VERSION
    assert document.overall_severity is Severity.NORMAL

    with pytest.raises(ValidationError):
        document.dag_id = "changed"


def test_serializer_preserves_typed_document_contract() -> None:
    document = build_analysis_document(_empty_analysis())

    assert serialize_analysis(_empty_analysis()) == document.model_dump(mode="json")
    assert serialize_analysis(_empty_analysis())["current_dag_run_id"] is None


def test_typed_document_preserves_nullable_change_percent() -> None:
    analysis = _empty_analysis()
    analysis.change_point_results["task"] = ChangePointResult(
        subject_id="task",
        change_index=3,
        before_median=0.0,
        after_median=2.0,
        change_percent=None,
        score=4.0,
        direction=ChangeDirection.INCREASE,
    )

    document = build_analysis_document(analysis)

    assert document.change_point_results["task"].change_percent is None


def test_analysis_json_schema_describes_version_and_severity() -> None:
    schema = analysis_json_schema()

    assert schema["properties"]["schema_version"]["const"] == (ANALYSIS_SCHEMA_VERSION)
    assert schema["properties"]["overall_severity"]["$ref"].endswith("/$defs/Severity")
    assert schema["$defs"]["Severity"]["enum"] == [
        "NORMAL",
        "MEDIUM",
        "HIGH",
        "CRITICAL",
    ]
    assert schema["$defs"]["DriftDirection"]["enum"] == [
        "INCREASE",
        "DECREASE",
        "UNCHANGED",
    ]
    assert "effective_mad" in schema["$defs"]["DriftOutput"]["properties"]
