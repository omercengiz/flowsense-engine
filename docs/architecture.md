# FlowSense Architecture

FlowSense uses an inward-facing layered architecture. Delivery mechanisms and
external systems depend on application contracts; the analysis domain does not
depend on Airflow, CLI, MCP, or environment configuration.

## Dependency direction

```text
CLI / MCP
    |
    v
Application use cases -----> Domain models and policies
    |                              ^
    v                              |
Application ports <----- Infrastructure adapters
                              |
                              v
                         Apache Airflow
```

Dependencies must point toward the application and domain layers. The
composition roots in the CLI and MCP adapters are responsible for selecting
concrete infrastructure implementations.

## Layers

### Domain

`flowsense.domain` contains the analysis vocabulary: task runs, policies,
severity and impact classifications, results, and expected domain failures.
It must not import application, infrastructure, CLI, MCP, or compatibility
modules. Domain entities and results use standard-library dataclasses; they do
not depend on validation or transport frameworks.

`flowsense.engine` currently contains pure analysis services such as drift,
trend, handoff, propagation, and root-cause calculations. These services may
depend on the domain but not on delivery or infrastructure code.

### Application

`flowsense.application` coordinates use cases without knowing how Airflow data
is retrieved or how results are displayed.

`AnalysisRequest` is the validated input contract. `AnalyzeDAG.execute()` is the
primary use-case boundary. `DAGDataSource` and `DAGDataSourceFactory` are ports
implemented by infrastructure adapters. The existing functional
`analyze_dag(dag_id, source, policy)` API remains available for callers that
already own a data source.

`FlowSenseClient` is the supported convenience facade for embedded Python
consumers. It translates the concise `analyze(...)` call into an
`AnalysisRequest` and delegates to `AnalyzeDAG`; it contains no collection or
analysis logic of its own.

`DAGAnalysisEngine` is the coarse-grained algorithm extension port. The default
implementation owns the complete statistical workflow after data collection.
Alternative engines can be injected into `AnalyzeDAG` without changing CLI,
MCP, data-source, or output-contract code. Individual detector interfaces are
intentionally avoided until independent detector replacement is required.

### Infrastructure

`flowsense.infrastructure.airflow` implements Airflow HTTP access. Pydantic DTOs
validate external API payloads and mappers translate them into domain objects.
Pydantic is intentionally restricted to infrastructure DTOs and application
output contracts, where runtime boundary validation and JSON Schema generation
are required.

`AirflowClient` receives an explicit `AirflowConfig` and never reads environment
variables. `load_airflow_config()` and `create_airflow_data_source()` belong to
the infrastructure composition boundary.

Authentication is supplied through `AirflowAuthProvider`. Built-in providers
cover Basic Auth, Airflow login-token exchange, static bearer tokens, and custom
headers without coupling request collection to a deployment's identity system.

### Delivery adapters

`flowsense.cli`, `flowsense.mcp`, and `flowsense.observability` translate user input into an
`AnalysisRequest`, invoke `AnalyzeDAG`, and translate the result into their own
output mechanism. Prometheus export implements the `AnalysisMetricsSink` port
and keeps a bounded latest-value snapshot. Business analysis and data-collection
orchestration must not be duplicated in these adapters.

## Main analysis flow

```text
Input options
    -> AnalysisRequest
    -> AnalyzeDAG
    -> DAGDataSourceFactory
    -> DAGDataSource
    -> task and handoff histories
    -> drift / change-point / trend analysis
    -> impact and propagation analysis
    -> primary root-cause selection
    -> DAGAnalysis
    -> versioned AnalysisDocument
```

## Extension points

- Add another data source by implementing `DAGDataSource` and providing a
  `DAGDataSourceFactory`.
- Replace the complete analysis workflow by implementing `DAGAnalysisEngine`
  and injecting it into `AnalyzeDAG`.
- Add a delivery mechanism by creating an adapter that builds an
  `AnalysisRequest` and invokes `AnalyzeDAG`.
- Extend output consumers through `AnalysisDocument`; incompatible contract
  changes require a schema-version change.
- Keep external payload models inside their infrastructure adapter and map them
  into domain models before analysis.

## Compatibility policy

The following pre-layered import paths remain temporarily available:

- `flowsense.models` -> `flowsense.domain`
- `flowsense.collector` -> `flowsense.infrastructure.airflow`
- `flowsense.config` -> `flowsense.infrastructure.airflow`
- `flowsense.engine.analyzer` -> `flowsense.AnalyzeDAG`

They emit `DeprecationWarning` and must not be used by new internal code. They
will be removed only in an explicitly announced breaking release.

## Automated guardrails

`tests/test_architecture.py` parses internal imports and fails when a layer
introduces a forbidden outward dependency. The compatibility analyzer is the
only documented exception. New exceptions require an architectural decision and
must not be added merely to make the test pass.
