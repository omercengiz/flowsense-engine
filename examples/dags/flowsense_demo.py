from __future__ import annotations

import random
import time
from datetime import UTC, datetime

from airflow.sdk import DAG, task

with DAG(
    dag_id="flowsense_demo",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    schedule=None,
    catchup=False,
    tags=["flowsense", "demo"],
):

    @task
    def extract() -> dict:
        delay = random.uniform(1.0, 2.0)
        time.sleep(delay)

        return {
            "records": random.randint(900, 1100),
            "delay": delay,
        }

    @task
    def transform(data: dict) -> dict:
        delay = random.uniform(8.0, 10.0)

        time.sleep(delay)

        return {
            "records": data["records"],
            "delay": delay,
        }

    @task
    def load(data: dict) -> None:
        delay = random.uniform(1.0, 2.0)
        time.sleep(delay)

        print(f"Loaded {data['records']} records in {delay:.2f} seconds")

    extracted = extract()
    transformed = transform(extracted)

    load(transformed)
