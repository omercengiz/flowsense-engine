from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import StrEnum

from flowsense.domain import FlowSenseError
from flowsense.infrastructure.airflow import AirflowClient, load_airflow_config


class DiagnosticStatus(StrEnum):
    PASS = "pass"
    FAIL = "fail"
    SKIP = "skip"


@dataclass(frozen=True)
class DiagnosticCheck:
    name: str
    status: DiagnosticStatus
    message: str


@dataclass(frozen=True)
class DoctorReport:
    checks: tuple[DiagnosticCheck, ...]

    @property
    def successful(self) -> bool:
        return all(check.status is not DiagnosticStatus.FAIL for check in self.checks)

    def to_dict(self) -> dict[str, object]:
        return {
            "successful": self.successful,
            "checks": [asdict(check) for check in self.checks],
        }


def run_airflow_diagnostics(*, include_paused: bool = False) -> DoctorReport:
    """Validate configuration and read-only Airflow API access."""
    try:
        config = load_airflow_config()
    except FlowSenseError as exc:
        return DoctorReport(
            checks=(
                DiagnosticCheck("configuration", DiagnosticStatus.FAIL, str(exc)),
                DiagnosticCheck(
                    "airflow_api",
                    DiagnosticStatus.SKIP,
                    "Skipped because configuration is invalid.",
                ),
                DiagnosticCheck(
                    "dag_visibility",
                    DiagnosticStatus.SKIP,
                    "Skipped because configuration is invalid.",
                ),
            )
        )

    checks = [
        DiagnosticCheck(
            "configuration",
            DiagnosticStatus.PASS,
            (
                f"{config.base_url.rstrip('/')} using API {config.api_version} "
                f"and {config.auth_mode} authentication."
            ),
        )
    ]

    try:
        with AirflowClient(config) as airflow:
            dag_ids = airflow.list_dag_ids(include_paused=include_paused)
    except FlowSenseError as exc:
        checks.extend(
            [
                DiagnosticCheck("airflow_api", DiagnosticStatus.FAIL, str(exc)),
                DiagnosticCheck(
                    "dag_visibility",
                    DiagnosticStatus.SKIP,
                    "Skipped because the Airflow API check failed.",
                ),
            ]
        )
        return DoctorReport(checks=tuple(checks))

    checks.append(
        DiagnosticCheck(
            "airflow_api",
            DiagnosticStatus.PASS,
            f"Authenticated GET /api/{config.api_version}/dags succeeded.",
        )
    )
    if dag_ids:
        checks.append(
            DiagnosticCheck(
                "dag_visibility",
                DiagnosticStatus.PASS,
                f"Discovered {len(dag_ids)} visible DAG(s).",
            )
        )
    else:
        checks.append(
            DiagnosticCheck(
                "dag_visibility",
                DiagnosticStatus.FAIL,
                "No visible DAGs were discovered.",
            )
        )

    return DoctorReport(checks=tuple(checks))
