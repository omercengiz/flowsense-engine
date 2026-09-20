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

## Custom data sources and engines

`FlowSenseClient` depends on the `DAGDataSourceFactory` and
`DAGAnalysisEngine` application ports. A new platform adapter can implement the
data-source protocol and supply a context-managed factory without depending on
Airflow. A complete alternative analysis workflow can be injected through the
optional `analysis_engine` constructor argument.
