from dataclasses import dataclass

from flowsense.domain import (
    DEFAULT_ANALYSIS_POLICY,
    AnalysisPolicy,
    ConfigurationError,
)


@dataclass(frozen=True)
class AnalysisRequest:
    """Validated input contract shared by FlowSense delivery adapters."""

    dag_id: str
    policy: AnalysisPolicy = DEFAULT_ANALYSIS_POLICY
    history_run_limit: int | None = None

    def __post_init__(self) -> None:
        if not self.dag_id.strip():
            raise ConfigurationError("dag_id must not be empty.")

        if (
            self.history_run_limit is not None
            and self.history_run_limit < self.policy.minimum_history
        ):
            raise ConfigurationError(
                "history_run_limit must be at least minimum_history."
            )
