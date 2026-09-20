import pytest
from pydantic import ValidationError

from flowsense import (
    ANALYSIS_SCHEMA_VERSION,
    BATCH_ANALYSIS_SCHEMA_VERSION,
    BatchAnalysisDocument,
    BatchAnalysisResult,
    ConfigurationError,
    DAGAnalysis,
    Severity,
    batch_analysis_json_schema,
    build_batch_analysis_document,
    serialize_batch_analysis,
)


def _batch_result() -> BatchAnalysisResult:
    analysis = DAGAnalysis(
        dag_id="healthy",
        runs_analyzed=0,
        overall_severity=Severity.NORMAL,
        primary_origin=None,
        drift_results={},
        handoff_drift_results={},
        task_impacts={},
        propagation_results=[],
        dependencies={},
    )
    return BatchAnalysisResult(
        requested_dag_ids=("healthy", "broken"),
        analyses={"healthy": analysis},
        failures={"broken": ConfigurationError("invalid Airflow configuration")},
    )


def test_builds_typed_immutable_batch_document() -> None:
    document = build_batch_analysis_document(_batch_result())

    assert isinstance(document, BatchAnalysisDocument)
    assert document.schema_version == BATCH_ANALYSIS_SCHEMA_VERSION
    assert document.requested_dag_ids == ["healthy", "broken"]
    assert document.successful_count == 1
    assert document.failed_count == 1
    assert document.analyses["healthy"].schema_version == ANALYSIS_SCHEMA_VERSION
    assert document.failures["broken"].error_type == "ConfigurationError"
    assert document.failures["broken"].message == "invalid Airflow configuration"

    with pytest.raises(ValidationError):
        document.successful_count = 2


def test_batch_serializer_preserves_typed_document_contract() -> None:
    result = _batch_result()
    document = build_batch_analysis_document(result)

    assert serialize_batch_analysis(result) == document.model_dump(mode="json")


def test_batch_json_schema_describes_envelope_and_nested_analysis() -> None:
    schema = batch_analysis_json_schema()

    assert schema["properties"]["schema_version"]["const"] == (
        BATCH_ANALYSIS_SCHEMA_VERSION
    )
    assert schema["properties"]["analyses"]["additionalProperties"]["$ref"].endswith(
        "/$defs/AnalysisDocument"
    )
    assert schema["properties"]["failures"]["additionalProperties"]["$ref"].endswith(
        "/$defs/BatchFailureOutput"
    )
