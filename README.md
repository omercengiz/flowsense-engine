# FlowSense Engine

Temporal drift, anomaly detection, and dependency-aware propagation analysis for Apache Airflow.

Current package release: `0.2.1`. See [CHANGELOG.md](CHANGELOG.md) for release
notes and [docs/releasing.md](docs/releasing.md) for the release checklist.

FlowSense analyzes historical DAG executions to identify abnormal task behavior and trace how anomalies propagate through downstream dependencies.

## Why FlowSense?

Airflow provides rich execution metadata, but identifying behavioral drift across historical runs still requires manual analysis.

FlowSense is designed to answer questions such as:

- Which task started behaving differently?
- How large is the deviation from its historical baseline?
- Is the anomaly isolated or affecting downstream tasks?
- Where is the most likely origin of the slowdown?

## Current Features

- Apache Airflow 3 REST API integration
- JWT-based Airflow authentication
- DAG run collection
- Task instance collection
- Automatic DAG dependency discovery
- Task duration history generation
- Median-based historical baselines
- MAD-based robust Z-score drift detection
- Severity classification
- Task handoff delay analysis
- Task impact classification (`OWN_DRIFT`, `INHERITED_DELAY`, and `COMBINED`)
- Multi-hop and branching propagation analysis
- Primary root-cause selection
- CLI-based DAG analysis
- MCP server integration

## Example

```bash
flowsense analyze flowsense_demo
```

Example output:

```text
FlowSense Analysis — flowsense_demo

┏━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━━┓
┃ Task      ┃ Baseline ┃ Current ┃ Deviation ┃ Z-Score ┃ Severity ┃ Impact    ┃
┡━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━━┩
│ extract   │ 1.56s    │ 1.61s   │ +3.4%     │ 0.17    │ NORMAL   │ NORMAL    │
│ transform │ 3.34s    │ 9.61s   │ +187.6%   │ 7.61    │ CRITICAL │ OWN_DRIFT │
│ load      │ 1.40s    │ 2.11s   │ +50.2%    │ 3.17    │ MEDIUM   │ COMBINED  │
└───────────┴──────────┴─────────┴───────────┴─────────┴──────────┴───────────┘

Overall Severity: CRITICAL
Primary Origin: transform
Reason: OWN_DRIFT
Severity: CRITICAL
Propagation Score: 0.33

Propagation Analysis

Origin: transform
Path: transform -> load
Propagation Score: 0.33
```

## Architecture

```text
Apache Airflow
      │
      ▼
  Collector
      │
      ▼
Task Run and Handoff History
      │
      ▼
Drift and Impact Analysis
      │
      ▼
Propagation and Root-Cause Analysis
      │
      ├── CLI
      └── MCP Server
```

## Installation

FlowSense currently requires Python 3.12 or newer.
Python 3.12 and 3.13 are covered by the CI test matrix.

Clone the repository:

```bash
git clone <repository-url>
cd flowsense-engine
```

Create a virtual environment and install the project:

```bash
uv venv --python 3.12
source .venv/bin/activate
uv pip install -e ".[dev,mcp]"
```

Once published on PyPI, install the distribution with:

```bash
pip install flowsense-engine
```

The distribution name is `flowsense-engine`; Python imports and CLI commands
remain `flowsense`.

Check the installed package version:

```bash
flowsense --version
```

## Configuration

FlowSense connects to Apache Airflow through its REST API.

Copy the example environment file:

```bash
cp .env.example .env
```

Configure:

```env
AIRFLOW_BASE_URL=http://localhost:8080
AIRFLOW_USERNAME=your_username
AIRFLOW_PASSWORD=your_password
AIRFLOW_API_VERSION=v2
AIRFLOW_AUTH_MODE=token
AIRFLOW_CONNECT_TIMEOUT=10
AIRFLOW_READ_TIMEOUT=10
AIRFLOW_MAX_RETRIES=2
AIRFLOW_RETRY_BACKOFF=0.5
AIRFLOW_HISTORY_RUN_LIMIT=100
```

Use `AIRFLOW_API_VERSION=v1` with `AIRFLOW_AUTH_MODE=basic` for Airflow 2.x
Stable REST API deployments. Airflow 3.x uses the `v2` API and typically uses
token authentication. Authentication still depends on the API auth backend
configured in the Airflow deployment.

Transient transport failures and HTTP `429`, `502`, `503`, and `504` responses
are retried with exponential backoff. `Retry-After` is honored when Airflow
provides it. Connect/read timeouts, retry count, and base backoff can be tuned
with the environment variables above; permanent client errors are returned
without retrying.

Load the environment variables:

```bash
export $(grep -v '^#' .env | xargs)
```

Then run:

```bash
flowsense analyze <dag_id>
```

The CLI report includes a DAG summary and separate tables for task drift,
handoff drift, change points, trends, propagation paths, and diagnostics.
Results are ordered by severity or subject so repeated analyses remain easy to
compare.

For automation and CI/CD integrations, request the versioned JSON document:

```bash
flowsense analyze <dag_id> --output json
```

CI jobs can also fail when the analysis reaches a selected severity:

```bash
flowsense analyze <dag_id> --output json --fail-on high
```

`--fail-on` accepts `medium`, `high`, or `critical`. The report is always
written before FlowSense exits: code `0` means the severity is below the
threshold, code `2` means the threshold was reached, and code `1` remains
reserved for analysis or Airflow request failures.

By default, FlowSense collects task instances for the most recent 100
successful DAG runs. This prevents long-lived DAGs from generating an
unbounded number of task-instance API requests. Change the default with
`AIRFLOW_HISTORY_RUN_LIMIT`, or override it for one CLI analysis:

```bash
flowsense analyze <dag_id> --history-run-limit 250
```

The MCP tool exposes the same override as `history_run_limit`. The limit must
be at least `2` and cannot be lower than `minimum_history`; contradictory
requests are rejected before FlowSense connects to Airflow.

To investigate or backtest a specific successful DAG run, select it as the
current observation. FlowSense excludes every newer run from its baseline:

```bash
flowsense analyze <dag_id> --dag-run-id <dag_run_id>
```

The MCP tool exposes the same option as `dag_run_id`. Analysis output includes
`current_dag_run_id`; this field was introduced in output schema version `1.1`.

The JSON document and MCP tool response share the same serialization contract
and include a `schema_version` field. The serializer is also available from the
public library API as `flowsense.serialize_analysis`.

For typed integrations, `flowsense.build_analysis_document` returns a
validated Pydantic `AnalysisDocument`. Its versioned JSON Schema is available
through `flowsense.analysis_json_schema()`, allowing consumers to validate or
generate models for the CLI and MCP response contract.

The same schema can be emitted without connecting to Airflow:

```bash
flowsense schema > flowsense-analysis.schema.json
```

This output is deterministic and can be used in CI contract checks, editor
tooling, or client code generation.

## Library API

FlowSense can also be used as a Python library through its supported top-level
API:

```python
from flowsense import AnalysisRequest, AnalyzeDAG
from flowsense.infrastructure.airflow import create_airflow_data_source

analysis = AnalyzeDAG(create_airflow_data_source).execute(
    AnalysisRequest(dag_id="flowsense_demo")
)

print(analysis.overall_severity)
print(analysis.primary_origin)
```

Analysis behavior can be customized with an immutable policy:

```python
from flowsense import AnalysisPolicy, MappedTaskAggregation

policy = AnalysisPolicy(
    minimum_history=10,
    baseline_window=30,
    medium_threshold=2.5,
    high_threshold=4.0,
    critical_threshold=6.0,
    mapped_task_aggregation=MappedTaskAggregation.MAX,
    change_point_minimum_segment_size=4,
    change_point_score_threshold=4.0,
    trend_minimum_observations=8,
    trend_score_threshold=4.0,
    trend_minimum_directional_consistency=0.75,
)
```

`baseline_window` limits the number of historical values used before the current
run. Mapped task durations can be aggregated with `MAX`, `MEAN`, or `SUM`. The
same policy options are available through the CLI and MCP tool.
Change-point and trend detection can also be disabled independently with
`change_point_detection_enabled=False` or `trend_detection_enabled=False`.

CLI and MCP inputs are normalized into an immutable `AnalysisRequest` before
execution. Library integrations may use the same public DTO when they need to
validate a DAG id, policy, and explicit history collection limit together.

Every `DAGAnalysis` exposes a derived `summary` with task-analysis coverage,
severity distribution, anomalous task and handoff counts, uniquely affected
tasks, structural signals, and diagnostics. The same DAG-level summary is
included in CLI and MCP output.

Custom data sources can implement the `DAGDataSource` protocol and be passed to
`analyze_dag`. Names exported directly from `flowsense` form the supported public
API. Imports from internal packages such as `flowsense.engine` should be treated
as implementation details and may change before version 1.0.

Expected operational failures derive from `FlowSenseError`. Library consumers
can catch `ConfigurationError`, `AirflowApiError`, or `AirflowDataError` for
more specific handling. CLI failures return exit code `1`; MCP converts these
failures into tool errors without exposing response bodies or parser details.

## MCP Server

Start the FlowSense MCP server over stdio:

```bash
flowsense-mcp
```

The server exposes the `analyze_airflow_dag` tool, which returns task drift,
handoff drift, impact classification, propagation paths, and primary root-cause
information for a DAG.

## Development

Run unit tests:

```bash
python -m pytest -m "not integration" -v
```

Run the complete test suite when a local Airflow instance is available:

```bash
python -m pytest -v
```

Lint:

```bash
ruff check .
```

Check formatting:

```bash
ruff format --check .
```

Apply formatting:

```bash
ruff format .
```

## Project Structure

```text
src/flowsense/
├── application/
│   ├── analyzer.py
│   ├── pipeline.py
│   └── ports.py
├── domain/
├── engine/
│   ├── change_point.py
│   ├── trend.py
│   ├── drift.py
│   ├── history.py
│   ├── impact.py
│   ├── propagation.py
│   ├── root_cause.py
│   └── timing.py
├── infrastructure/
│   └── airflow/
├── cli/
├── mcp/
├── collector/  # backward-compatible imports
└── models/     # backward-compatible imports
```

## Detection Approach

The current drift detector uses robust statistics rather than machine learning.

For each task, historical execution durations are used to calculate a median baseline and Median Absolute Deviation (MAD).

The latest execution is compared against that baseline using a robust Z-score.

This makes the detector less sensitive to historical outliers than approaches based only on mean and standard deviation.

Propagation scores are normalized to the `0.0–1.0` range. Downstream task
severity is weighted by graph distance with a `0.8` decay per hop, so anomalies
closer to the origin contribute more strongly than anomalies farther along the
same path.

FlowSense also scans ordered task-duration and handoff-delay histories for
persistent level shifts. Each candidate split must leave at least three
observations on both sides. Candidates are compared with a robust, MAD-based
score, and detected changes report their location, direction, before/after
medians, percentage change, and score. This prevents a single latest-run outlier
from being reported as a structural change.

FlowSense detects sustained increasing and decreasing trends in ordered task
durations and handoff delays with a robust Theil-Sen slope. A trend must contain
at least five observations, meet a minimum directional-consistency ratio, and
exceed a MAD-based score threshold. Results include the per-run slope, estimated
total and percentage change, direction, consistency, and score.

The application layer coordinates analysis through explicit task and handoff
pipeline stages. Each stage returns typed drift, structural-signal, and
diagnostic results, while `analyze_dag` remains the composition point for impact,
propagation, root-cause, and DAG-level output.

## Project Status

FlowSense is currently in early development.

The current implementation should be considered experimental and is not yet intended for production use.

## License

Licensed under the Apache License 2.0.
