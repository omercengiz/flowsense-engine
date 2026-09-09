from flowsense.domain import FlowSenseError


class AirflowApiError(FlowSenseError):
    def __init__(
        self,
        method: str,
        endpoint: str,
        status_code: int | None = None,
    ) -> None:
        self.method = method
        self.endpoint = endpoint
        self.status_code = status_code

        status = f" with status {status_code}" if status_code is not None else ""
        super().__init__(f"Airflow API {method} {endpoint} failed{status}.")


class AirflowDataError(FlowSenseError):
    def __init__(self, resource: str) -> None:
        self.resource = resource
        super().__init__(f"Airflow returned invalid {resource} data.")


class AirflowDagRunNotFoundError(FlowSenseError):
    def __init__(self, dag_id: str, dag_run_id: str) -> None:
        self.dag_id = dag_id
        self.dag_run_id = dag_run_id
        super().__init__(
            f"Successful Airflow DAG run {dag_run_id!r} was not found for {dag_id!r}."
        )
