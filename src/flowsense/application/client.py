from flowsense.application.analysis_engine import DEFAULT_DAG_ANALYSIS_ENGINE
from flowsense.application.ports import DAGAnalysisEngine
from flowsense.application.request import AnalysisRequest
from flowsense.application.use_cases import AnalyzeDAG, DAGDataSourceFactory
from flowsense.domain import DEFAULT_ANALYSIS_POLICY, AnalysisPolicy, DAGAnalysis


class FlowSenseClient:
    """Convenient, reusable entry point for Python library consumers."""

    def __init__(
        self,
        source_factory: DAGDataSourceFactory,
        analysis_engine: DAGAnalysisEngine = DEFAULT_DAG_ANALYSIS_ENGINE,
    ) -> None:
        self._analyze_dag = AnalyzeDAG(
            source_factory=source_factory,
            analysis_engine=analysis_engine,
        )

    def analyze(
        self,
        dag_id: str,
        *,
        policy: AnalysisPolicy = DEFAULT_ANALYSIS_POLICY,
        history_run_limit: int | None = None,
        dag_run_id: str | None = None,
    ) -> DAGAnalysis:
        """Analyze a DAG without requiring callers to construct a request DTO."""
        return self.execute(
            AnalysisRequest(
                dag_id=dag_id,
                policy=policy,
                history_run_limit=history_run_limit,
                dag_run_id=dag_run_id,
            )
        )

    def execute(self, request: AnalysisRequest) -> DAGAnalysis:
        """Execute an already validated analysis request."""
        return self._analyze_dag.execute(request)
