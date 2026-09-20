# Production quickstart

This guide runs FlowSense as an installed Python package against an existing
Apache Airflow deployment. FlowSense remains an in-process library and CLI; it
does not require a separate hosted service or database.

## 1. Install an isolated version

FlowSense requires Python 3.12 or 3.13.

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install "flowsense-engine==0.4.0"
flowsense --version
```

The expected version is `0.4.0`. Pin the version in production dependency files
and upgrade deliberately after reviewing the changelog.

## 2. Configure Airflow access

Keep credentials in a secret manager or injected environment variables rather
than source control.

For an Airflow 3 deployment using login-token exchange:

```bash
export AIRFLOW_BASE_URL="https://airflow.example.com"
export AIRFLOW_API_VERSION="v2"
export AIRFLOW_AUTH_MODE="token"
export AIRFLOW_USERNAME="flowsense"
export AIRFLOW_PASSWORD="replace-from-secret-manager"
```

For an Airflow 2 deployment using Basic Auth:

```bash
export AIRFLOW_BASE_URL="https://airflow.example.com"
export AIRFLOW_API_VERSION="v1"
export AIRFLOW_AUTH_MODE="basic"
export AIRFLOW_USERNAME="flowsense"
export AIRFLOW_PASSWORD="replace-from-secret-manager"
```

Static bearer tokens use `AIRFLOW_AUTH_MODE=bearer` and
`AIRFLOW_BEARER_TOKEN`. Custom identity gateways can use the Python
authentication provider API described in [authentication.md](authentication.md).

Use a read-only Airflow identity with access to DAG definitions, DAG runs, and
task instances. Configure conservative network and history bounds:

```bash
export AIRFLOW_CONNECT_TIMEOUT="10"
export AIRFLOW_READ_TIMEOUT="30"
export AIRFLOW_MAX_RETRIES="2"
export AIRFLOW_RETRY_BACKOFF="0.5"
export AIRFLOW_HISTORY_RUN_LIMIT="100"
```

## 3. Run a single-DAG smoke test

Validate configuration, authentication, API routing, and DAG visibility without
running an analysis:

```bash
flowsense doctor
```

For automation, `flowsense doctor --output json` emits structured checks and
returns exit code `1` when a required check fails.

List the active DAGs visible to the configured identity:

```bash
flowsense dags
flowsense dags --output json
```

Paused DAGs are excluded unless `--include-paused` is provided.

Analyze an explicit, bounded DAG set directly from the CLI:

```bash
flowsense analyze-batch orders payments inventory \
  --history-run-limit 50 \
  --max-concurrency 3 \
  --fail-on high \
  --output-file flowsense-batch.json
```

The command always writes the versioned batch JSON when analysis completes. It
returns exit code `1` if any DAG has an expected failure, while preserving all
successful results and failure records in the output file.

Batch analysis accepts the same policy controls as single-DAG analysis,
including history requirements, severity thresholds, mapped-task aggregation,
and change-point or trend detector settings.

With `--fail-on`, exit code `2` means at least one successful DAG analysis
reached the selected severity. Operational DAG failures take precedence and
retain exit code `1`.

Start with a DAG that has several successful historical runs:

```bash
flowsense analyze example_dag --output json
```

Exit code `0` means analysis completed. Exit code `1` represents a configuration,
Airflow, or analysis failure. When `--fail-on` is used, exit code `2` means the
selected severity threshold was reached.

For CI:

```bash
flowsense analyze example_dag --output json --fail-on high > analysis.json
```

## 4. Discover and analyze several DAGs

The repository includes a bounded production example:

```bash
python examples/python/production_batch.py \
  --dag-limit 10 \
  --history-run-limit 50 \
  --max-concurrency 3 \
  --output flowsense-batch.json
```

The example:

1. loads validated Airflow configuration;
2. discovers active DAGs through the paginated Airflow API;
3. limits the number of selected DAGs;
4. runs independent analyses with bounded concurrency;
5. writes the versioned batch output contract.

Paused DAGs are excluded unless `--include-paused` is supplied. The command
returns exit code `1` when one or more expected DAG-level failures are present,
while still writing successful results and failure records.

## 5. Size the workload

Each DAG analysis reads DAG runs, task instances for the selected successful
runs, and the current DAG structure. Start with:

- `--dag-limit 5` or `10`;
- `--history-run-limit 30` to `100`;
- `--max-concurrency 1` to `3`.

Increase these values only after observing Airflow API latency and rate limits.
FlowSense retries transient `429`, `502`, `503`, and `504` responses, but
concurrency should remain below the capacity of the Airflow webserver.

## 6. Operate safely

- Pin the FlowSense package version.
- Inject secrets at runtime and never log them.
- Use a read-only Airflow account.
- Retain the output `schema_version` fields with stored JSON.
- Treat batch `failures` separately from anomaly severity.
- Alert when analysis stops succeeding, not only when severity increases.
- Validate upgrades in a staging Airflow environment.

Prometheus and Grafana are optional adapters. See [prometheus.md](prometheus.md),
[grafana.md](grafana.md), and [alerting.md](alerting.md) when metrics-based
operation is required.

## Troubleshooting

`401` or `403` responses usually indicate a mismatched authentication mode or
insufficient Airflow permissions. Confirm the API version and auth backend.

Insufficient-history diagnostics mean the DAG has fewer successful observations
than the active analysis policy requires. Increase the available history or
adjust the policy deliberately; do not treat missing history as a normal result.

For timeouts or rate limiting, lower batch concurrency and history limits before
increasing retry counts. See [authentication.md](authentication.md) and
[python-api.md](python-api.md) for deeper integration options.
