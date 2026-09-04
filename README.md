# FlowSense Engine

Temporal drift, anomaly detection, and dependency-aware propagation analysis for Apache Airflow.

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
```

Use `AIRFLOW_API_VERSION=v1` with `AIRFLOW_AUTH_MODE=basic` for Airflow 2.x
Stable REST API deployments. Airflow 3.x uses the `v2` API and typically uses
token authentication. Authentication still depends on the API auth backend
configured in the Airflow deployment.

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

## Library API

FlowSense can also be used as a Python library through its supported top-level
API:

```python
from flowsense import AirflowClient, analyze_dag

with AirflowClient() as source:
    analysis = analyze_dag(
        dag_id="flowsense_demo",
        source=source,
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

Every `DAGAnalysis` exposes a derived `summary` with task-analysis coverage,
severity distribution, anomalous task and handoff counts, uniquely affected
tasks, structural signals, and diagnostics. The same DAG-level summary is
included in CLI and MCP output.

Custom data sources can implement the `DAGDataSource` protocol and be passed to
`analyze_dag`. Names exported directly from `flowsense` form the supported public
API. Imports from internal packages such as `flowsense.engine` should be treated
as implementation details and may change before version 1.0.

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

## Project Status

FlowSense is currently in early development.

The current implementation should be considered experimental and is not yet intended for production use.

## License

Licensed under the Apache License 2.0.
