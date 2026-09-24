# Live multi-DAG showcase

This is a controlled demo, executed by real Airflow workers. It is not production
traffic or a scalability benchmark. No run states, task durations, or Prometheus
history are fabricated or backdated.

The six DAGs in `examples/dags/flowsense_showcase.py` each contain four tasks:
orders, payments, inventory, analytics, ML features, and customer 360. The first
14 runs establish a baseline; the next six introduce different controlled sleep
patterns. Worker and scheduler overhead are included in the measured durations,
so actual severities can vary. The orders workload has no injected slowdown.

## Run locally

The repository's Airflow Compose configuration mounts `examples/dags`. If an
existing installation uses a different DAG mount, copy the file to its actual
DAG folder. Wait for all six `flowsense_showcase_*` DAGs to be parsed.

With Airflow credentials in environment variables or the local `.env`:

```bash
PYTHONPATH=src .venv/bin/python examples/python/showcase.py \
  --seed --live-cycles 30 --host 0.0.0.0
```

Only one exporter can use port 9108 at a time; stop an existing `serve-metrics`
process before starting this one. The showcase seeds 20 real executions per DAG
(120 runs, 480 task executions), skipping seed run IDs that already exist. It
unpauses only the six showcase DAGs. Failed seed runs are reported, not silently
marked successful or retried. Once each DAG has at least 20 successes, up to 30
additional waves are submitted, one wave per minute. Live waves triple injected
slow task delays so running activity is visible across scrape intervals; the
unchanged orders workload remains a control. Run counts can therefore
exceed 20; each analysis still uses the latest 20 successful runs. After the last
wave, collection continues without submitting additional work. Ctrl-C stops the
collector; already submitted Airflow runs continue to completion.

Start the observability stack as described in [grafana.md](grafana.md), then open
[FlowSense · Live Multi-DAG Showcase](http://localhost:3000/d/flowsense-showcase?from=now-5m&to=now&kiosk).
The dashboard is automatically provisioned alongside FlowSense Overview.

## What the dashboard proves

- `flowsense_dag_runs_analyzed`: actual successful runs consumed by the engine,
  per DAG; the sum is the current analysis-window total, not lifetime throughput.
- `flowsense_task_*`: measured duration deviations, robust z-scores, and severity.
- DAG-level affected tasks, change points, trends, and handoff anomalies are
  computed by the existing FlowSense engine.
- `airflow_showcase_runs`: queued, running, successful, and failed run counts read
  separately from the Airflow REST API by the demo collector.

**Running-run counts are activity monitoring, not in-flight drift analysis.**
The engine analyzes completed successful runs. Both activity and analysis update
on each collection loop, followed by the normal Prometheus scrape interval.
Brief runs can finish between scrapes. Time-series charts show actual observations
since collection began, not all historical Airflow run timestamps.

`logs/showcase-evidence/runs.json` records Airflow run IDs, states, timestamps,
and demo configuration. The sibling per-DAG JSON documents contain full FlowSense
analyses, including the current run ID, baseline, deviations, and root cause.
These are continuously updated local evidence files, not committed credentials.
For a durable report, copy a snapshot into `artifacts/showcase-evidence` alongside
the screenshot, recording the observation time.

A defensible description for sharing is: “FlowSense analyzed a rolling window
of 20 successful executions across each of six controlled Airflow pipelines
(120 runs, 24 task definitions), while separately monitoring live run activity.”
Use the captured evidence for anomaly counts, which change between observations.

Re-run the read-only execution proof against Airflow:

```bash
PYTHONPATH=src .venv/bin/python examples/python/showcase.py \
  --verify-seed --evidence-dir artifacts/showcase-evidence
```

This fails if a seed run is missing/unsuccessful or a run lacks four successful
task instances with start/end timestamps. It writes `seed-execution-proof.json`
only after validating all six DAGs.
