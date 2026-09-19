from unittest.mock import patch

from flowsense.application import AnalysisRequest
from flowsense.infrastructure.airflow import AirflowClient, AirflowConfig
from flowsense.infrastructure.airflow.factory import create_airflow_data_source


def test_factory_loads_environment_config_and_applies_request_scope() -> None:
    config = AirflowConfig(
        base_url="http://airflow.test",
        username="airflow",
        password="airflow",
    )
    request = AnalysisRequest(
        dag_id="demo",
        history_run_limit=25,
        dag_run_id="run_42",
    )

    with patch(
        "flowsense.infrastructure.airflow.factory.load_airflow_config",
        return_value=config,
    ) as load_config:
        source = create_airflow_data_source(request)

    load_config.assert_called_once_with()
    assert source.base_url == "http://airflow.test"
    assert source.history_run_limit == 25
    assert source.target_dag_run_id == "run_42"
    source.close()


def test_client_construction_does_not_read_environment() -> None:
    config = AirflowConfig(
        base_url="http://airflow.test",
        username="airflow",
        password="airflow",
    )

    with patch(
        "flowsense.infrastructure.airflow.config.load_airflow_config"
    ) as load_config:
        source = AirflowClient(config)

    load_config.assert_not_called()
    source.close()
