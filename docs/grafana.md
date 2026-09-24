# Grafana dashboard

FlowSense ships a provisioned local observability stack containing Prometheus
and Grafana. Airflow credentials stay in the host process running FlowSense;
the containers only scrape the exported metrics.

## Start FlowSense metrics

Configure the required `AIRFLOW_*` environment variables, then run:

```bash
flowsense serve-metrics example_dag another_dag \
  --host 0.0.0.0 \
  --port 9108 \
  --interval-seconds 60 \
  --history-run-limit 50 \
  --policy-file flowsense-policy.json
```

Omit `--policy-file` to use the default analysis policy.

Binding to `0.0.0.0` makes the endpoint reachable from the local Prometheus
container. Do not expose this port to untrusted networks without an appropriate
network policy or reverse proxy.

## Start Prometheus and Grafana

From the repository root:

```bash
docker compose -f deploy/observability/docker-compose.yml up -d
```

Open:

- Grafana: `http://localhost:3000` (`admin` / `admin` by default)
- Prometheus: `http://localhost:9090`
- Alertmanager: `http://localhost:9093`

Set `GRAFANA_ADMIN_USER` and `GRAFANA_ADMIN_PASSWORD` before starting Compose
to override the local defaults. The Prometheus data source and `FlowSense
Overview` dashboard are provisioned automatically.

## Dashboard panels

The dashboard provides:

- current DAG severity and analysis health;
- analysis coverage and runtime;
- anomalous task and handoff counts;
- DAG severity history;
- task duration deviation and robust z-score history;
- current task severity;
- task cardinality omissions;
- last successful analysis time.
- currently firing FlowSense alerts.

Use the DAG and task variables at the top of the dashboard to narrow the view.
The default time range is 15 minutes and the dashboard refreshes every 30 seconds.
Summary cards use instant queries, so they remain populated independently of
the selected history window. Current task comparisons include both DAG and
task names to distinguish tasks across multiple DAGs.

The top row summarizes the selected DAGs: monitored count, highest severity,
analysis health (healthy only when every selected DAG succeeded), mean coverage,
and total anomalous and affected tasks. Task comparisons and history appear
below; collection diagnostics and active alerts follow.

Prometheus history starts when the metrics service is scraped; historical
Airflow runs are analyzed but are not backfilled into these charts. After
starting the exporter, use a recent time range and allow a few scrapes. An
existing browser URL can retain an older time range; select Last 15 minutes
manually in that case.
See [alerting.md](alerting.md) for rule behavior, validation, and production
notification configuration.

## Stop the stack

```bash
docker compose -f deploy/observability/docker-compose.yml down
```

Prometheus and Grafana data remain in named volumes. To explicitly remove those
local volumes, add `--volumes` to the command.

## Multi-DAG demo with execution evidence

For six real Airflow workloads, 20 seeded runs per DAG, continued live activity,
and a dedicated presentation dashboard, see [showcase.md](showcase.md).
