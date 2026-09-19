from flowsense.application import AnalysisRequest
from flowsense.infrastructure.airflow.client import AirflowClient


def create_airflow_data_source(request: AnalysisRequest) -> AirflowClient:
    """Create the Airflow adapter required by an analysis request."""
    return AirflowClient(
        history_run_limit=request.history_run_limit,
        target_dag_run_id=request.dag_run_id,
    )
