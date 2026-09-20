# Python library API

`FlowSenseClient` is the recommended entry point for applications embedding
FlowSense. It is a lightweight in-process facade: it does not start a server,
own a scheduler, or store analysis results.

## Environment-configured Airflow

Use the standard Airflow environment variables for the shortest setup:

```python
from flowsense import FlowSenseClient
from flowsense.infrastructure.airflow import create_airflow_data_source

client = FlowSenseClient(create_airflow_data_source)
analysis = client.analyze("example_dag")

print(analysis.overall_severity)
print(analysis.primary_origin)
```

## Explicit Airflow configuration

Applications that own their configuration should avoid process-global
environment state:

```python
from flowsense import FlowSenseClient
from flowsense.infrastructure.airflow import (
    AirflowConfig,
    BasicAuthProvider,
    create_airflow_data_source_factory,
)

config = AirflowConfig(
    base_url="https://airflow.example.com",
    api_version="v2",
    history_run_limit=100,
)
source_factory = create_airflow_data_source_factory(
    config,
    auth_provider=BasicAuthProvider("flowsense", "secret"),
)
client = FlowSenseClient(source_factory)

analysis = client.analyze(
    "example_dag",
    history_run_limit=50,
    dag_run_id="scheduled__2026-09-20T00:00:00+00:00",
)
```

Secrets are deliberately supplied to an authentication provider rather than
stored in application code. See [authentication.md](authentication.md) for
built-in and custom providers.

## Typed requests and policies

Use `execute()` when a request is created at another application boundary:

```python
from flowsense import AnalysisPolicy, AnalysisRequest

request = AnalysisRequest(
    dag_id="example_dag",
    history_run_limit=50,
    policy=AnalysisPolicy(
        minimum_history=10,
        baseline_window=30,
    ),
)
analysis = client.execute(request)
```

Both `analyze()` and `execute()` return the domain-level `DAGAnalysis` model.
Use `serialize_analysis()` or `build_analysis_document()` when a versioned
external output contract is required.

## Analyze multiple DAGs

Airflow-backed applications can discover active DAG ids before starting a
batch. Discovery uses the configured, paginated Airflow 2 or 3 REST API:

```python
from flowsense.infrastructure.airflow import AirflowClient

with AirflowClient(config, auth_provider=auth_provider) as airflow:
    dag_ids = airflow.list_dag_ids()

result = client.analyze_many(dag_ids, max_concurrency=3)
```

Paused DAGs are excluded by default. Pass `include_paused=True` when they are
also required.

`analyze_many()` is intended for applications that monitor several DAGs in one
process. It preserves the requested DAG order and separates successful analyses
from expected FlowSense failures:

```python
result = client.analyze_many(
    ["orders", "payments", "inventory"],
    history_run_limit=50,
    max_concurrency=3,
)

for dag_id, analysis in result.analyses.items():
    print(dag_id, analysis.overall_severity)

for dag_id, failure in result.failures.items():
    print(dag_id, failure)
```

Batch results have their own versioned external contract. Raw exception objects
are converted to stable failure records containing `error_type` and `message`:

```python
from flowsense import serialize_batch_analysis

payload = serialize_batch_analysis(result)
```

Use `build_batch_analysis_document()` for a typed Pydantic document or
`batch_analysis_json_schema()` for the corresponding JSON Schema. The current
batch envelope schema version is `1.0`; each successful analysis retains its
own analysis schema version.

Concurrency is opt-in and bounded; `max_concurrency` defaults to `1`. Each DAG
receives its own context-managed data source. Expected `FlowSenseError`
instances are isolated in `failures`, while unexpected programming or adapter
errors still propagate to the caller. Use individual `AnalysisRequest` objects
when each DAG needs a different policy or historical run id. When concurrency
is greater than one, custom data-source factories and injected analysis engines
must be safe to call from multiple threads.

## Custom data sources and engines

`FlowSenseClient` depends on the `DAGDataSourceFactory` and
`DAGAnalysisEngine` application ports. A new platform adapter can implement the
data-source protocol and supply a context-managed factory without depending on
Airflow. A complete alternative analysis workflow can be injected through the
optional `analysis_engine` constructor argument.
