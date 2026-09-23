# FlowSense 0.5.0

FlowSense 0.5.0 strengthens the package for larger Airflow installations and
more reliable production analysis. It adds bounded multi-DAG workflows,
improves Airflow API efficiency and authentication resilience, and corrects
zero-MAD anomaly scoring.

## Highlights

- Bounded multi-DAG analysis through the Python API and CLI.
- Active DAG discovery with optional paused-DAG inclusion.
- Versioned batch JSON output and JSON Schema generation.
- Server-side DAG run filtering and batched task-instance collection with
  compatibility fallbacks.
- Automatic one-time refresh for expired Airflow login tokens.
- Regularized MAD scoring for constant and near-constant baselines.
- Explicit drift direction and effective MAD in analysis results.
- Reusable JSON policy files across single, batch, and metrics commands.
- Optional Prometheus metrics, Grafana dashboard, and alerting assets.
- Expanded Ruff checks and a 90% CI coverage gate.

## Output contract

The analysis output schema is now `1.2`. Drift results include `direction` and
`effective_mad`; policy output includes relative and absolute dispersion floors.
The batch envelope remains at schema version `1.0`.

Review [Migrating to FlowSense 0.5](migrating-to-0.5.md) before upgrading JSON
consumers or alerting rules.

## Installation

```bash
pip install "flowsense-engine==0.5.0"
```

For MCP support:

```bash
pip install "flowsense-engine[mcp]==0.5.0"
```

## Verification

```bash
flowsense --version
flowsense doctor
```
