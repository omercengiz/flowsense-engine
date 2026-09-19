# Changelog

All notable changes to FlowSense are documented in this file. The project uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- Made the `TaskRun` domain entity immutable and framework-independent while
  keeping Pydantic at external DTO and output-contract boundaries.
- Added automated enforcement preventing the domain layer from importing
  Pydantic.
- Added a coarse-grained `DAGAnalysisEngine` port so alternative analysis
  workflows can be injected without coupling delivery adapters to algorithms.

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
