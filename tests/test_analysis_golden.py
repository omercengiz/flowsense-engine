from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from flowsense import TaskRun, analyze_dag, serialize_analysis

GOLDEN_FILE = Path(__file__).parent / "fixtures" / "golden" / "analysis_v1_1.json"


class GoldenDAGDataSource:
    def collect_task_runs(self, dag_id: str) -> list[TaskRun]:
        base_time = datetime(2026, 1, 1, tzinfo=UTC)
        task_runs: list[TaskRun] = []

        transform_durations = [3.0, 4.0, 5.0, 6.0, 7.0, 12.0]
        load_durations = [2.0, 3.0, 4.0, 5.0, 6.0, 10.0]
        checkpoint_durations = [2.0, 2.0, 2.0, 6.0, 6.0, 6.0]
        first_handoff_delays = [1.0, 2.0, 3.0, 4.0, 5.0, 9.0]
        second_handoff_delays = [0.5, 1.0, 1.5, 2.0, 2.5, 6.0]

        for index in range(6):
            dag_run_id = f"run_{index + 1}"
            extract_start = base_time + timedelta(days=index)
            extract_end = extract_start + timedelta(seconds=1)
            transform_start = extract_end + timedelta(
                seconds=first_handoff_delays[index]
            )
            transform_end = transform_start + timedelta(
                seconds=transform_durations[index]
            )
            load_start = transform_end + timedelta(seconds=second_handoff_delays[index])
            load_end = load_start + timedelta(seconds=load_durations[index])

            task_runs.extend(
                [
                    TaskRun(
                        dag_id=dag_id,
                        dag_run_id=dag_run_id,
                        task_id="extract",
                        state="success",
                        start_date=extract_start,
                        end_date=extract_end,
                        duration=1.0,
                    ),
                    TaskRun(
                        dag_id=dag_id,
                        dag_run_id=dag_run_id,
                        task_id="transform",
                        state="success",
                        start_date=transform_start,
                        end_date=transform_end,
                        duration=transform_durations[index],
                    ),
                    TaskRun(
                        dag_id=dag_id,
                        dag_run_id=dag_run_id,
                        task_id="load",
                        state="success",
                        start_date=load_start,
                        end_date=load_end,
                        duration=load_durations[index],
                    ),
                    TaskRun(
                        dag_id=dag_id,
                        dag_run_id=dag_run_id,
                        task_id="checkpoint",
                        state="success",
                        duration=checkpoint_durations[index],
                    ),
                ]
            )

            if index >= 4:
                task_runs.append(
                    TaskRun(
                        dag_id=dag_id,
                        dag_run_id=dag_run_id,
                        task_id="audit",
                        state="success",
                        duration=0.5,
                    )
                )

        return task_runs

    def get_dag_dependencies(self, dag_id: str) -> dict[str, list[str]]:
        return {
            "extract": ["transform"],
            "transform": ["load"],
            "load": [],
            "checkpoint": [],
            "audit": [],
        }


def build_golden_document() -> dict[str, object]:
    analysis = analyze_dag("golden_dag", GoldenDAGDataSource())
    return serialize_analysis(analysis)


def test_analysis_document_matches_versioned_golden_file() -> None:
    actual = json.dumps(
        build_golden_document(),
        indent=2,
        sort_keys=True,
        ensure_ascii=False,
    )

    assert actual + "\n" == GOLDEN_FILE.read_text(encoding="utf-8")
