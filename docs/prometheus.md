# Prometheus metrics

FlowSense can continuously analyze one or more Airflow DAGs and expose the
latest results using the Prometheus text exposition format. The endpoint uses
only the Python standard library, so no additional runtime dependency is
required.

Start the service:

```bash
flowsense serve-metrics example_dag another_dag \
  --host 0.0.0.0 \
  --port 9108 \
  --interval-seconds 60 \
  --policy-file flowsense-policy.json
```

`--policy-file` is optional. When supplied, every collection cycle uses the
same validated, versioned analysis policy as the single and batch CLI commands.

The command uses the same `AIRFLOW_*` configuration as `flowsense analyze`.
Configure Prometheus to scrape `http://<flowsense-host>:9108/metrics`.

## Metric contract

All metrics are gauges. DAG-level metrics carry only the `dag_id` label:

- `flowsense_analysis_success`
- `flowsense_analysis_duration_seconds`
- `flowsense_analysis_last_success_timestamp_seconds`
- `flowsense_analysis_last_failure_timestamp_seconds`
- `flowsense_dag_overall_severity`
- `flowsense_dag_runs_analyzed`
- `flowsense_dag_analysis_coverage_ratio`
- `flowsense_dag_anomalous_tasks`
- `flowsense_dag_anomalous_handoffs`
- `flowsense_dag_affected_tasks`
- `flowsense_dag_change_points`
- `flowsense_dag_trends`
- `flowsense_dag_diagnostics`
- `flowsense_dag_dropped_tasks`

Task-level metrics additionally carry `task_id`:

- `flowsense_task_severity`
- `flowsense_task_deviation_percent`
- `flowsense_task_robust_z_score`

Severity values are `0` for normal, `1` for medium, `2` for high, and `3` for
critical.

## Cardinality policy

Task metrics are limited to 200 task IDs per DAG by default. Task IDs are sorted
to make selection deterministic. Use `--max-tasks-per-dag` to change the
limit, or set it to `0` to publish DAG-level metrics only. The exporter reports
the number of omitted tasks through `flowsense_dag_dropped_tasks`.

The exporter retains the latest successful analysis snapshot when a later
collection fails. In that case `flowsense_analysis_success` becomes `0` and the
failure timestamp advances, while the last known DAG and task measurements
remain available.
