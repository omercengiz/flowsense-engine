from collections.abc import Iterable, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from types import MappingProxyType

from flowsense.application.request import AnalysisRequest
from flowsense.application.use_cases import AnalyzeDAG
from flowsense.domain import (
    DEFAULT_ANALYSIS_POLICY,
    AnalysisPolicy,
    ConfigurationError,
    DAGAnalysis,
    FlowSenseError,
)


@dataclass(frozen=True)
class BatchAnalysisResult:
    """Immutable results and expected failures from a multi-DAG analysis."""

    analyses: Mapping[str, DAGAnalysis]
    failures: Mapping[str, FlowSenseError]

    def __post_init__(self) -> None:
        object.__setattr__(self, "analyses", MappingProxyType(dict(self.analyses)))
        object.__setattr__(self, "failures", MappingProxyType(dict(self.failures)))

    @property
    def successful_count(self) -> int:
        return len(self.analyses)

    @property
    def failed_count(self) -> int:
        return len(self.failures)

    @property
    def is_successful(self) -> bool:
        return not self.failures


class AnalyzeDAGBatch:
    """Coordinate independent DAG analyses with bounded concurrency."""

    def __init__(self, analyze_dag: AnalyzeDAG) -> None:
        self._analyze_dag = analyze_dag

    def execute(
        self,
        dag_ids: Iterable[str],
        *,
        policy: AnalysisPolicy = DEFAULT_ANALYSIS_POLICY,
        history_run_limit: int | None = None,
        max_concurrency: int = 1,
    ) -> BatchAnalysisResult:
        normalized_dag_ids = self._validate_input(
            dag_ids,
            policy,
            history_run_limit,
            max_concurrency,
        )

        def analyze_one(dag_id: str) -> DAGAnalysis | FlowSenseError:
            try:
                return self._analyze_dag.execute(
                    AnalysisRequest(
                        dag_id=dag_id,
                        policy=policy,
                        history_run_limit=history_run_limit,
                    )
                )
            except FlowSenseError as exc:
                return exc

        if max_concurrency == 1 or len(normalized_dag_ids) == 1:
            outcomes = map(analyze_one, normalized_dag_ids)
            return self._build_result(normalized_dag_ids, outcomes)

        with ThreadPoolExecutor(
            max_workers=min(max_concurrency, len(normalized_dag_ids)),
            thread_name_prefix="flowsense-analysis",
        ) as executor:
            outcomes = executor.map(analyze_one, normalized_dag_ids)
            return self._build_result(normalized_dag_ids, outcomes)

    @staticmethod
    def _build_result(
        dag_ids: tuple[str, ...],
        outcomes: Iterable[DAGAnalysis | FlowSenseError],
    ) -> BatchAnalysisResult:
        analyses: dict[str, DAGAnalysis] = {}
        failures: dict[str, FlowSenseError] = {}
        for dag_id, outcome in zip(dag_ids, outcomes, strict=True):
            if isinstance(outcome, FlowSenseError):
                failures[dag_id] = outcome
            else:
                analyses[dag_id] = outcome

        return BatchAnalysisResult(analyses=analyses, failures=failures)

    @staticmethod
    def _validate_input(
        dag_ids: Iterable[str],
        policy: AnalysisPolicy,
        history_run_limit: int | None,
        max_concurrency: int,
    ) -> tuple[str, ...]:
        if max_concurrency < 1:
            raise ConfigurationError("max_concurrency must be at least 1.")

        normalized = tuple(dag_ids)
        if not normalized:
            raise ConfigurationError("dag_ids must contain at least one DAG id.")

        for dag_id in normalized:
            AnalysisRequest(
                dag_id=dag_id,
                policy=policy,
                history_run_limit=history_run_limit,
            )

        if len(set(normalized)) != len(normalized):
            raise ConfigurationError("dag_ids must not contain duplicates.")

        return normalized
