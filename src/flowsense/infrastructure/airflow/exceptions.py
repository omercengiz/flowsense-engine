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
