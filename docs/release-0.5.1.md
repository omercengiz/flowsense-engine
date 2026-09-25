# FlowSense 0.5.1

This patch fixes Airflow 3 state filtering and adds a reproducible multi-DAG
showcase and updated project branding.

## Highlights

- Correct `state` filters for Airflow 3 DAG runs and task instances. A running
  run no longer consumes a slot in the bounded successful-run history window.
- Six controlled demo DAGs, four tasks each, with 20 seeded runs per DAG and
  optional bounded live execution waves.
- Read-only verification of 120 successful seed runs and 480 task executions.
- Dedicated Grafana showcase with per-DAG analyses, severity history, task
  deviations, and separately collected running/queued activity.
- Improved overview dashboard and a wordmark in the README/PyPI description.

Demo workloads use controlled delays. Drift analysis evaluates completed
successful runs; live activity monitoring is separate. Output schemas and
public API contracts are unchanged.

See [the showcase guide](showcase.md) for setup and verification.
