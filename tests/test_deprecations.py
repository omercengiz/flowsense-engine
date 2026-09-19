import importlib
import warnings

import pytest


@pytest.mark.parametrize(
    ("module_name", "replacement"),
    [
        ("flowsense.collector", "flowsense.infrastructure.airflow"),
        ("flowsense.models", "flowsense.domain"),
        ("flowsense.config", "flowsense.infrastructure.airflow"),
        ("flowsense.engine.analyzer", "flowsense.AnalyzeDAG"),
    ],
)
def test_legacy_module_emits_deprecation_warning(
    module_name: str,
    replacement: str,
) -> None:
    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always")
        module = importlib.import_module(module_name)
        importlib.reload(module)

    messages = [str(item.message) for item in captured]
    assert any(replacement in message for message in messages)
