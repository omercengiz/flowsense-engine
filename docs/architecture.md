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
modules.

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

### Infrastructure

`flowsense.infrastructure.airflow` implements Airflow HTTP access. Pydantic DTOs
validate external API payloads and mappers translate them into domain objects.

`AirflowClient` receives an explicit `AirflowConfig` and never reads environment
variables. `load_airflow_config()` and `create_airflow_data_source()` belong to
the infrastructure composition boundary.

### Delivery adapters

`flowsense.cli` and `flowsense.mcp` translate user input into an
`AnalysisRequest`, invoke `AnalyzeDAG`, and translate the result into their own
output mechanism. Business analysis and data-collection orchestration must not
be duplicated in these adapters.

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
