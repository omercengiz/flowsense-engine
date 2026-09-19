from __future__ import annotations

import argparse
import gc
import json
import platform
import statistics
import sys
import time
import tracemalloc
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from flowsense import DEFAULT_ANALYSIS_POLICY, TaskRun, serialize_analysis
from flowsense.application import DefaultDAGAnalysisEngine


@dataclass(frozen=True)
class BenchmarkScenario:
    tasks: int = 100
    runs: int = 50
    iterations: int = 5
    warmups: int = 1

    def __post_init__(self) -> None:
        for field_name in ("tasks", "runs", "iterations"):
            if getattr(self, field_name) < 1:
                raise ValueError(f"{field_name} must be positive")
        if self.warmups < 0:
            raise ValueError("warmups must be non-negative")


@dataclass(frozen=True)
class BenchmarkSample:
    analysis_seconds: float
    total_seconds: float
    peak_memory_bytes: int
    analyzed_tasks: int


def build_synthetic_dag(
    scenario: BenchmarkScenario,
) -> tuple[list[TaskRun], dict[str, list[str]]]:
    """Build an ordered, deterministic chain DAG with realistic timing data."""
    task_ids = [f"task_{index:04d}" for index in range(scenario.tasks)]
    dependencies = {
        task_id: ([task_ids[index + 1]] if index + 1 < len(task_ids) else [])
        for index, task_id in enumerate(task_ids)
    }
    task_runs: list[TaskRun] = []
    epoch = datetime(2026, 1, 1, tzinfo=UTC)

    for run_index in range(scenario.runs):
        dag_run_id = f"benchmark_run_{run_index:04d}"
        cursor = epoch + timedelta(days=run_index)

        for task_index, task_id in enumerate(task_ids):
            baseline = 1.0 + (task_index % 20) * 0.1
            variation = ((run_index + task_index) % 5 - 2) * 0.01
            anomaly = (
                baseline * 2.5
                if run_index == scenario.runs - 1 and task_index % 10 == 0
                else 0.0
            )
            duration = baseline + variation + anomaly
            start_date = cursor + timedelta(milliseconds=task_index % 3)
            end_date = start_date + timedelta(seconds=duration)

            task_runs.append(
                TaskRun(
                    dag_id="benchmark_dag",
                    dag_run_id=dag_run_id,
                    task_id=task_id,
                    state="success",
                    start_date=start_date,
                    end_date=end_date,
                    duration=duration,
                )
            )
            cursor = end_date

    return task_runs, dependencies


def _measure_once(
    engine: DefaultDAGAnalysisEngine,
    task_runs: list[TaskRun],
    dependencies: dict[str, list[str]],
) -> BenchmarkSample:
    gc.collect()
    tracemalloc.start()
    total_started = time.perf_counter()
    analysis_started = time.perf_counter()
    analysis = engine.analyze(
        dag_id="benchmark_dag",
        task_runs=task_runs,
        dependencies=dependencies,
        policy=DEFAULT_ANALYSIS_POLICY,
    )
    analysis_seconds = time.perf_counter() - analysis_started
    serialize_analysis(analysis)
    total_seconds = time.perf_counter() - total_started
    _, peak_memory_bytes = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    return BenchmarkSample(
        analysis_seconds=analysis_seconds,
        total_seconds=total_seconds,
        peak_memory_bytes=peak_memory_bytes,
        analyzed_tasks=analysis.summary.analyzed_tasks,
    )


def run_benchmark(scenario: BenchmarkScenario) -> dict[str, object]:
    task_runs, dependencies = build_synthetic_dag(scenario)
    engine = DefaultDAGAnalysisEngine()

    for _ in range(scenario.warmups):
        engine.analyze(
            dag_id="benchmark_dag",
            task_runs=task_runs,
            dependencies=dependencies,
            policy=DEFAULT_ANALYSIS_POLICY,
        )

    samples = [
        _measure_once(engine, task_runs, dependencies)
        for _ in range(scenario.iterations)
    ]
    analysis_times = [sample.analysis_seconds for sample in samples]
    total_times = [sample.total_seconds for sample in samples]
    peak_memory = [sample.peak_memory_bytes for sample in samples]

    return {
        "benchmark_version": "1",
        "environment": {
            "implementation": platform.python_implementation(),
            "platform": platform.platform(),
            "python_version": platform.python_version(),
        },
        "scenario": {
            **asdict(scenario),
            "edges": max(0, scenario.tasks - 1),
            "task_run_records": len(task_runs),
        },
        "results": {
            "analysis_seconds": {
                "maximum": max(analysis_times),
                "median": statistics.median(analysis_times),
                "minimum": min(analysis_times),
            },
            "total_seconds": {
                "maximum": max(total_times),
                "median": statistics.median(total_times),
                "minimum": min(total_times),
            },
            "peak_memory_bytes": {
                "maximum": max(peak_memory),
                "median": statistics.median(peak_memory),
                "minimum": min(peak_memory),
            },
            "analyzed_tasks": samples[-1].analyzed_tasks,
        },
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark FlowSense analysis.")
    parser.add_argument("--tasks", type=int, default=100)
    parser.add_argument("--runs", type=int, default=50)
    parser.add_argument("--iterations", type=int, default=5)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    try:
        scenario = BenchmarkScenario(
            tasks=args.tasks,
            runs=args.runs,
            iterations=args.iterations,
            warmups=args.warmups,
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc

    output = json.dumps(run_benchmark(scenario), indent=2, sort_keys=True) + "\n"
    if args.output is None:
        sys.stdout.write(output)
    else:
        args.output.write_text(output, encoding="utf-8")


if __name__ == "__main__":
    main()
