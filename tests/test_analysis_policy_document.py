import pytest
from pydantic import ValidationError

from flowsense import (
    ANALYSIS_POLICY_SCHEMA_VERSION,
    AnalysisPolicyDocument,
    analysis_policy_json_schema,
)


def test_analysis_policy_document_is_versioned_and_strict() -> None:
    document = AnalysisPolicyDocument(
        schema_version=ANALYSIS_POLICY_SCHEMA_VERSION,
        minimum_history=10,
    )

    assert document.minimum_history == 10
    assert document.schema_version == "1.0"

    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        AnalysisPolicyDocument.model_validate(
            {"schema_version": "1.0", "unknown_setting": True}
        )


def test_analysis_policy_schema_describes_version_and_options() -> None:
    schema = analysis_policy_json_schema()

    assert schema["properties"]["schema_version"]["const"] == "1.0"
    assert "minimum_history" in schema["properties"]
    assert "mapped_task_aggregation" in schema["properties"]
