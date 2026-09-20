"""Discover and analyze a bounded set of Airflow DAGs with FlowSense."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from flowsense import FlowSenseClient, serialize_batch_analysis
from flowsense.infrastructure.airflow import (
    AirflowClient,
    create_airflow_data_source_factory,
    load_airflow_config,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Discover Airflow DAGs and emit a FlowSense batch analysis."
    )
    parser.add_argument(
        "--dag-limit",
        type=int,
        default=10,
        help="Maximum number of discovered DAGs to analyze (default: 10).",
    )
    parser.add_argument(
        "--history-run-limit",
        type=int,
        default=None,
        help="Override the number of successful runs collected per DAG.",
    )
    parser.add_argument(
        "--max-concurrency",
        type=int,
        default=1,
        help="Maximum concurrent DAG analyses (default: 1).",
    )
    parser.add_argument(
        "--include-paused",
        action="store_true",
        help="Include paused DAGs in discovery.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Write JSON to this path instead of standard output.",
    )
    args = parser.parse_args()
    if args.dag_limit < 1:
        parser.error("--dag-limit must be at least 1")
    if args.max_concurrency < 1:
        parser.error("--max-concurrency must be at least 1")
    return args


def main() -> int:
    args = parse_args()
    config = load_airflow_config()

    with AirflowClient(config) as airflow:
        dag_ids = airflow.list_dag_ids(include_paused=args.include_paused)[
            : args.dag_limit
        ]

    if not dag_ids:
        raise SystemExit("No matching Airflow DAGs were discovered.")

    client = FlowSenseClient(create_airflow_data_source_factory(config))
    result = client.analyze_many(
        dag_ids,
        history_run_limit=args.history_run_limit,
        max_concurrency=args.max_concurrency,
    )
    rendered = json.dumps(serialize_batch_analysis(result), indent=2, sort_keys=True)

    if args.output is None:
        print(rendered)
    else:
        args.output.write_text(f"{rendered}\n", encoding="utf-8")

    return 1 if result.failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
