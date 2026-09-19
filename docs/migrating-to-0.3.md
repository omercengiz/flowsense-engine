# Migrating to FlowSense 0.3

FlowSense 0.3 strengthens the library architecture without changing the
versioned analysis output contract. The schema remains at version `1.1`, so
existing CLI JSON and MCP consumers do not need a response migration.

## Supported imports

Library integrations should import public contracts directly from `flowsense`:

```python
from flowsense import AnalysisRequest, AnalyzeDAG, TaskRun
```

The following compatibility namespaces still work in 0.3 but emit
`DeprecationWarning` and are planned for removal in a future breaking release:

| Deprecated import | Replacement |
| --- | --- |
| `flowsense.models` | `flowsense.domain` or public names from `flowsense` |
| `flowsense.collector` | `flowsense.infrastructure.airflow` |
| `flowsense.config` | `flowsense.infrastructure.airflow` |
| `flowsense.engine.analyzer` | `flowsense.AnalyzeDAG` |

Applications should migrate these imports now; no compatibility namespace is
used by FlowSense internals.

## Analysis entry point

Use `AnalyzeDAG` when FlowSense owns data-source creation:

```python
from flowsense import AnalysisRequest, AnalyzeDAG
from flowsense.infrastructure.airflow import create_airflow_data_source

analysis = AnalyzeDAG(create_airflow_data_source).execute(
    AnalysisRequest(dag_id="example_dag")
)
```

Existing integrations that already own a `DAGDataSource` can continue using
the public `analyze_dag` function. Custom analysis implementations can implement
`DAGAnalysisEngine` and inject that engine into `AnalyzeDAG`.

## Airflow configuration

`AirflowClient` now receives an explicit immutable `AirflowConfig`. Environment
access belongs to `load_airflow_config()` or `create_airflow_data_source()`:

```python
from flowsense.infrastructure.airflow import AirflowClient, load_airflow_config

client = AirflowClient(load_airflow_config())
```

CLI and MCP users can keep using the existing `AIRFLOW_*` environment variables.
For Airflow 2 use `AIRFLOW_API_VERSION=v1` with the authentication mode provided
by the deployment, commonly `basic`. Airflow 3 normally uses
`AIRFLOW_API_VERSION=v2` and `token` authentication.

## Domain model behavior

`TaskRun` is now an immutable standard-library dataclass. Code that mutated a
task run after construction must instead create a new value, for example with
`dataclasses.replace`. Invalid negative or non-finite durations are rejected at
construction time.
