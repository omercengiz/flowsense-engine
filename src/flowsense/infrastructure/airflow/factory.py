from flowsense.application import AnalysisRequest, DAGDataSourceFactory
from flowsense.infrastructure.airflow.auth import AirflowAuthProvider
from flowsense.infrastructure.airflow.client import AirflowClient
from flowsense.infrastructure.airflow.config import AirflowConfig, load_airflow_config


def create_airflow_data_source_factory(
    config: AirflowConfig,
    *,
    auth_provider: AirflowAuthProvider | None = None,
) -> DAGDataSourceFactory:
    """Bind explicit Airflow configuration to a reusable data-source factory."""

    def factory(request: AnalysisRequest) -> AirflowClient:
        return _create_airflow_client(config, request, auth_provider)

    return factory


def create_airflow_data_source(request: AnalysisRequest) -> AirflowClient:
    """Create an environment-configured Airflow adapter for a request."""
    return _create_airflow_client(load_airflow_config(), request)


def _create_airflow_client(
    config: AirflowConfig,
    request: AnalysisRequest,
    auth_provider: AirflowAuthProvider | None = None,
) -> AirflowClient:
    return AirflowClient(
        config=config,
        history_run_limit=request.history_run_limit,
        target_dag_run_id=request.dag_run_id,
        auth_provider=auth_provider,
    )
