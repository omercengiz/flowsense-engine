"""Run and observe six real Airflow demo workloads (local Airflow 3 only).

The analysis engine evaluates completed runs. A separate, explicitly named
activity metric reports queued/running runs; it is not in-flight drift detection.
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import threading
import time
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

from flowsense.application import AnalysisRequest, AnalyzeDAG
from flowsense.application.serialization import serialize_analysis
from flowsense.infrastructure.airflow.client import AirflowClient
from flowsense.infrastructure.airflow.config import load_airflow_config
from flowsense.infrastructure.airflow.factory import create_airflow_data_source_factory
from flowsense.observability.prometheus import PrometheusExporter, create_metrics_server

DAGS = [
    f"flowsense_showcase_{name}"
    for name in (
        "orders",
        "payments",
        "inventory",
        "analytics",
        "ml_features",
        "customer360",
    )
]


class ShowcaseExporter(PrometheusExporter):
    """FlowSense analysis plus separately labelled Airflow demo activity."""

    def __init__(self):
        super().__init__()
        self.activity = ""

    def render(self):
        return super().render() + self.activity


def verify_seed_runs(client: AirflowClient, evidence_dir: Path) -> dict:
    """Verify real successful seed runs and task timestamps, without mutations."""
    proof = {
        "observed_at": datetime.now(UTC).isoformat(),
        "controlled_demo": True,
        "source": "Airflow REST API",
        "dags": {},
    }
    expected = {f"flowsense_showcase_seed_{index:02d}" for index in range(20)}
    for dag in DAGS:
        seeds = [
            run
            for run in client.get_dag_runs(dag)["dag_runs"]
            if run["dag_run_id"] in expected
        ]
        if {r["dag_run_id"] for r in seeds} != expected or any(
            r["state"] != "success" for r in seeds
        ):
            raise ValueError(f"{dag}: expected 20 successful seed runs")
        tasks = []
        for run in seeds:
            instances = client.get_task_instances(dag, run["dag_run_id"])[
                "task_instances"
            ]
            if len(instances) != 4 or any(
                t["state"] != "success"
                or not t.get("start_date")
                or not t.get("end_date")
                for t in instances
            ):
                raise ValueError(
                    f"{dag}/{run['dag_run_id']}: expected four successful timed tasks"
                )
            tasks.extend(
                {
                    key: task.get(key)
                    for key in (
                        "dag_id",
                        "dag_run_id",
                        "task_id",
                        "state",
                        "start_date",
                        "end_date",
                        "duration",
                        "try_number",
                    )
                }
                for task in instances
            )
        proof["dags"][dag] = {
            "successful_seed_runs": len(seeds),
            "successful_task_executions": len(tasks),
            "task_instances": tasks,
        }
    evidence_dir.mkdir(parents=True, exist_ok=True)
    (evidence_dir / "seed-execution-proof.json").write_text(
        json.dumps(proof, indent=2) + "\n"
    )
    return proof


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--seed",
        action="store_true",
        help="Create 20 real runs per demo DAG, idempotently.",
    )
    parser.add_argument(
        "--live-cycles",
        type=int,
        default=0,
        help="Additional waves after seeding; one wave per minute.",
    )
    parser.add_argument(
        "--verify-seed",
        action="store_true",
        help="Verify 120 seed runs and 480 task executions without triggering work.",
    )
    parser.add_argument("--port", type=int, default=9108)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument(
        "--evidence-dir", type=Path, default=Path("logs/showcase-evidence")
    )
    args = parser.parse_args()
    if args.live_cycles < 0 or (args.verify_seed and (args.seed or args.live_cycles)):
        parser.error(
            "Use nonnegative live cycles; verification cannot be combined with execution."
        )
    # Parse values as data, never execute the .env file as shell code.
    if Path(".env").exists():
        for line in Path(".env").read_text().splitlines():
            key, sep, value = line.strip().removeprefix("export ").partition("=")
            if sep and key.startswith("AIRFLOW_"):
                os.environ.setdefault(key, " ".join(shlex.split(value, comments=True)))
    config = load_airflow_config()
    client = AirflowClient(config)
    if args.verify_seed:
        try:
            verify_seed_runs(client, args.evidence_dir)
            print(
                "Verified 120 successful seed runs and 480 successful task executions."
            )
        finally:
            client.close()
        return

    def api(method, path, **kwargs):
        response = client._authenticated_request(
            method, client._api_url(path), **kwargs
        )
        response.raise_for_status()
        return response.json()

    def trigger(dag, run_id, sample):
        api(
            "POST",
            f"/dags/{dag}/dagRuns",
            json={
                "dag_run_id": run_id,
                "logical_date": None,
                "conf": {"sample": sample, "controlled_demo": True},
                "note": "FlowSense showcase: real execution, controlled latency workload.",
            },
        )

    args.evidence_dir.mkdir(parents=True, exist_ok=True)
    exporter = ShowcaseExporter()
    server = create_metrics_server(exporter, host=args.host, port=args.port)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    analyze = AnalyzeDAG(create_airflow_data_source_factory(config)).execute
    if args.seed:
        for dag in DAGS:
            api("PATCH", f"/dags/{dag}", json={"is_paused": False})
            existing = {r["dag_run_id"] for r in client.get_dag_runs(dag)["dag_runs"]}
            for index in range(20):
                run_id = f"flowsense_showcase_seed_{index:02d}"
                if run_id not in existing:
                    trigger(dag, run_id, index)
            print(f"{dag}: 20 seed runs submitted", flush=True)

    cycles = 0
    last_wave = 0.0
    session = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
    try:
        while True:
            snapshot = {
                "observed_at": datetime.now(UTC).isoformat(),
                "controlled_demo": True,
                "dags": {},
            }
            activity = [
                "# HELP airflow_showcase_runs Real Airflow run counts for controlled demo DAGs.",
                "# TYPE airflow_showcase_runs gauge",
            ]
            ready = True
            for dag in DAGS:
                started = time.monotonic()
                try:
                    runs = client.get_dag_runs(dag)["dag_runs"]
                    counts = Counter(r["state"] for r in runs)
                    ready = ready and counts["success"] >= 20
                    for state in ("queued", "running", "success", "failed"):
                        activity.append(
                            f'airflow_showcase_runs{{dag_id="{dag}",state="{state}"}} {counts[state]}'
                        )
                    snapshot["dags"][dag] = {
                        "states": dict(counts),
                        "runs": [
                            {
                                k: r.get(k)
                                for k in (
                                    "dag_run_id",
                                    "state",
                                    "start_date",
                                    "end_date",
                                    "conf",
                                )
                            }
                            for r in runs
                        ],
                    }
                    if counts["success"] >= 6:
                        analysis = analyze(
                            AnalysisRequest(dag_id=dag, history_run_limit=20)
                        )
                        exporter.record_success(
                            analysis, duration_seconds=time.monotonic() - started
                        )
                        output = serialize_analysis(analysis)
                        (args.evidence_dir / f"{dag}.json").write_text(
                            json.dumps(output, indent=2) + "\n"
                        )
                        snapshot["dags"][dag]["analysis"] = {
                            "runs_analyzed": analysis.runs_analyzed,
                            "severity": str(analysis.overall_severity),
                            "anomalous_tasks": analysis.summary.anomalous_tasks,
                        }
                except Exception as exc:
                    ready = False
                    exporter.record_failure(
                        dag, duration_seconds=time.monotonic() - started
                    )
                    snapshot["dags"][dag] = {"error": type(exc).__name__}
                    print(f"{dag}: {type(exc).__name__}: {exc}", flush=True)
            exporter.activity = "\n".join(activity) + "\n"
            (args.evidence_dir / "runs.json").write_text(
                json.dumps(snapshot, indent=2) + "\n"
            )
            print(
                json.dumps(
                    {
                        dag: value.get("states", value)
                        for dag, value in snapshot["dags"].items()
                    }
                ),
                flush=True,
            )
            if (
                ready
                and cycles < args.live_cycles
                and time.monotonic() - last_wave >= 60
            ):
                for dag in DAGS:
                    trigger(
                        dag,
                        f"flowsense_showcase_live_{session}_{cycles:02d}",
                        14 + cycles % 6,
                    )
                cycles += 1
                last_wave = time.monotonic()
                print(f"Live wave {cycles}/{args.live_cycles} submitted", flush=True)
            time.sleep(2)
    finally:
        server.shutdown()
        server.server_close()
        client.close()


if __name__ == "__main__":
    main()
