# Prometheus alerting

The local observability stack loads a tested set of FlowSense alert rules and
routes firing alerts to Alertmanager.

## Included alerts

| Alert | Default condition | Severity |
| --- | --- | --- |
| `FlowSenseExporterDown` | Metrics endpoint unavailable for 2 minutes | Critical |
| `FlowSenseAnalysisFailed` | Latest analysis failed for 2 minutes | Warning |
| `FlowSenseAnalysisStale` | No successful analysis for 10 minutes in total | Warning |
| `FlowSenseCriticalDAG` | DAG severity remains critical for 2 minutes | Critical |
| `FlowSenseCriticalTask` | Task severity remains critical for 2 minutes | Critical |
| `FlowSenseTaskMetricsDropped` | Cardinality limit omits tasks for 5 minutes | Warning |
| `FlowSenseAnalysisSlow` | Analysis remains above 30 seconds for 5 minutes | Warning |

The stale rule expression becomes true after five minutes without a successful
analysis and must remain true for another five minutes before firing. Adjust
thresholds in `deploy/observability/alerts.yml` to match the configured analysis
interval and operational expectations.

## Validate rules

Run syntax and unit checks with the Prometheus image pinned by the Compose file:

```bash
docker run --rm \
  --entrypoint promtool \
  -v "$PWD/deploy/observability:/workspace:ro" \
  -w /workspace \
  prom/prometheus:v3.14.0 \
  check rules alerts.yml

docker run --rm \
  --entrypoint promtool \
  -v "$PWD/deploy/observability:/workspace:ro" \
  -w /workspace \
  prom/prometheus:v3.14.0 \
  test rules alerts.test.yml
```

## Configure notifications

The committed Alertmanager configuration intentionally uses an empty local
receiver. Firing alerts are visible in the Alertmanager UI and the Grafana
dashboard, but no external notification is sent by default.

For production, replace the receiver in
`deploy/observability/alertmanager.yml` with the organization's managed email,
Slack, PagerDuty, webhook, or other supported integration. Keep credentials out
of Git and inject them through the deployment platform's secret management
mechanism.

Alertmanager is available locally at `http://localhost:9093`. Verify routing and
notification delivery in a non-production environment before enabling it for
operational incidents.
