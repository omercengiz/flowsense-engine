import pytest

from flowsense import AnalysisPolicy, AnalysisRequest, ConfigurationError


def test_accepts_consistent_analysis_request() -> None:
    policy = AnalysisPolicy(minimum_history=10)

    request = AnalysisRequest(
        dag_id="demo",
        policy=policy,
        history_run_limit=25,
    )

    assert request.dag_id == "demo"
    assert request.policy is policy
    assert request.history_run_limit == 25


def test_rejects_blank_dag_id() -> None:
    with pytest.raises(ConfigurationError, match="dag_id"):
        AnalysisRequest(dag_id="  ")


def test_rejects_blank_dag_run_id() -> None:
    with pytest.raises(ConfigurationError, match="dag_run_id"):
        AnalysisRequest(dag_id="demo", dag_run_id="  ")


def test_rejects_history_limit_below_minimum_history() -> None:
    with pytest.raises(ConfigurationError, match="history_run_limit"):
        AnalysisRequest(
            dag_id="demo",
            policy=AnalysisPolicy(minimum_history=10),
            history_run_limit=9,
        )
