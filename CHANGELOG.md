# Changelog

All notable changes to FlowSense are documented in this file. The project uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Added a production quickstart and executable bounded batch-analysis example.
- Added `flowsense doctor` for safe configuration, Airflow API, authentication,
  and DAG-visibility diagnostics.

## [0.4.0] - 2026-09-20

### Added

- Added paginated Airflow 2 and 3 DAG discovery with optional paused-DAG
  inclusion.
- Added an immutable multi-DAG analysis result and bounded, opt-in concurrency
  through `FlowSenseClient.analyze_many()`.
- Added a versioned, typed batch output document with serialization and JSON
  Schema helpers.
- Added the `FlowSenseClient` facade for concise embedded Python usage and an
  explicit Airflow data-source factory for application-owned configuration.
- Added pluggable Airflow authentication providers for Basic Auth, login-token
  exchange, static or rotating bearer tokens, and deployment-specific headers.
- Added a bounded Prometheus metrics exporter and the optional
  `flowsense serve-metrics` command for continuous DAG analysis.
- Added a provisioned Grafana dashboard and local Docker Compose example for
  inspecting analysis health, coverage, severity, task drift, and handoffs.
- Added Prometheus alert rules, Alertmanager configuration, synthetic rule
  tests, and CI validation for FlowSense analysis failures and anomalies.

### Changed

- Kept observability integrations as optional delivery adapters so the core
  analysis package remains independent from Prometheus, Grafana, and hosted
  service infrastructure.

## [0.3.0] - 2026-09-20

### Added

- Added `AnalyzeDAG` as the shared application use-case boundary for CLI, MCP,
  and library integrations.
- Added the `DAGAnalysisEngine` extension port and injectable default analysis
  engine.
- Added versioned golden regression coverage for the complete analysis output.
- Added deterministic performance benchmarks with machine-readable results and
  a manually triggered GitHub Actions workflow.
- Added an Airflow 2 and Airflow 3 REST contract test matrix covering
  authentication, routing, validation, mapping, and end-to-end analysis.

### Changed

- Separated environment loading and Airflow client construction through explicit
  `AirflowConfig` and infrastructure factories.
- Made the `TaskRun` domain entity immutable and framework-independent while
  keeping Pydantic at external DTO and output-contract boundaries.
- Added automated enforcement preventing the domain layer from importing
  Pydantic.
- Added architecture dependency guardrails and documented supported extension
  points and compatibility boundaries.

### Fixed

- Rejected non-finite and negative analysis inputs at the domain boundary.
- Made root-cause selection deterministic when candidates have equal scores.

## [0.2.1] - 2026-09-10

### Changed

- Renamed the Python distribution to `flowsense-engine` while preserving the
  `flowsense` import package and CLI command.
- Added tokenless PyPI publishing through GitHub Actions Trusted Publishing.

## [0.2.0] - 2026-09-10

### Added

- Task handoff drift, impact classification, propagation, and root-cause analysis.
- Configurable drift, mapped-task aggregation, change-point, and trend policies.
- Apache Airflow 2 and 3 API compatibility with pagination and bounded history.
- Retry, timeout, backoff, and `Retry-After` handling for Airflow requests.
- Historical DAG run analysis with preceding-history isolation.
- Rich CLI reports, JSON output, severity-based exit thresholds, and schema output.
- MCP analysis tool with the same policy and versioned response contract as the CLI.
- Typed `AnalysisRequest` and `AnalysisDocument` contracts with JSON Schema support.
- Supported top-level library API, typed package marker, and Python 3.12/3.13 CI.

### Changed

- Split the analysis workflow into domain, application, infrastructure, and adapter
  boundaries.
- Analysis output schema advanced to version `1.1` with `current_dag_run_id`.
- Expected configuration, Airflow API, and payload failures now use a unified error
  hierarchy.

### Fixed

- Preserved anomaly origins across normal dependency gaps.
- Detected drift when the historical median absolute deviation is zero.
- Aggregated dynamically mapped task instances before analysis.
- Reported insufficient history and invalid handoff timing as diagnostics.

## [0.1.0] - 2026-08-18

### Added

- Initial FlowSense prototype with Airflow task-duration collection and robust
  median/MAD drift detection.
