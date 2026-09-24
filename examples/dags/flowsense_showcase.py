"""Six controlled workloads; real task executions, deliberately varied latency."""

from __future__ import annotations

import time
from datetime import UTC, datetime

from airflow.sdk import DAG, get_current_context, task

SCENARIOS = {
    "orders": (
        "extract_orders",
        "validate_orders",
        "aggregate_orders",
        "publish_orders",
    ),
    "payments": ("fetch_payments", "validate_payments", "reconcile", "settle"),
    "inventory": ("read_stock", "normalize_stock", "forecast_stock", "sync_stock"),
    "analytics": (
        "collect_events",
        "enrich_events",
        "build_sessions",
        "publish_metrics",
    ),
    "ml_features": (
        "read_events",
        "clean_features",
        "compute_features",
        "write_features",
    ),
    "customer360": (
        "fetch_profiles",
        "deduplicate",
        "join_profiles",
        "publish_profiles",
    ),
}


def build_dag(name: str, task_names: tuple[str, ...]) -> DAG:
    with DAG(
        dag_id=f"flowsense_showcase_{name}",
        start_date=datetime(2026, 1, 1, tzinfo=UTC),
        schedule=None,
        catchup=False,
        max_active_runs=3,
        max_active_tasks=6,
        tags=["flowsense", "controlled-demo", name],
        doc_md="Controlled latency demo. Real Airflow executions; no production data.",
    ) as dag:

        @task
        def work(stage: int, scenario: str) -> dict:
            run = get_current_context()["dag_run"]
            conf = run.conf or {}
            index = int(conf.get("sample", 0))
            # Small deterministic baseline variation, then different incident patterns.
            delay = 0.35 + ((index * 5 + stage * 3) % 7) * 0.04
            if index >= 14:
                if scenario == "payments" and stage == 2:
                    delay += 3.0
                elif scenario == "inventory" and stage == 2:
                    delay += (index - 13) * 0.45
                elif scenario == "analytics" and stage in (1, 2):
                    delay += 1.8 if index % 3 else 0.2
                elif scenario == "ml_features" and stage == 2:
                    delay += 4.0
                elif scenario == "customer360" and stage == 1:
                    delay += 1.2
            # Live waves intensify the incident so activity spans multiple scrapes.
            if "_live_" in run.run_id and delay > 1.0:
                delay *= 3
            time.sleep(delay)
            return {
                "scenario": scenario,
                "sample": index,
                "stage": stage,
                "sleep_seconds": delay,
            }

        previous = None
        for stage, task_name in enumerate(task_names):
            current = work.override(task_id=task_name)(stage, name)
            if previous is not None:
                previous >> current
            previous = current
    return dag


for scenario, names in SCENARIOS.items():
    globals()[f"flowsense_showcase_{scenario}"] = build_dag(scenario, names)
