import pytest

from benchmarks.benchmark_analysis import (
    BenchmarkScenario,
    build_synthetic_dag,
    run_benchmark,
)


def test_synthetic_benchmark_dag_is_deterministic_and_ordered() -> None:
    scenario = BenchmarkScenario(tasks=4, runs=3, iterations=1, warmups=0)

    first_runs, first_dependencies = build_synthetic_dag(scenario)
    second_runs, second_dependencies = build_synthetic_dag(scenario)

    assert first_runs == second_runs
    assert first_dependencies == second_dependencies
    assert len(first_runs) == 12
    assert first_dependencies == {
        "task_0000": ["task_0001"],
        "task_0001": ["task_0002"],
        "task_0002": ["task_0003"],
        "task_0003": [],
    }
    assert first_runs[-1].dag_run_id == "benchmark_run_0002"


@pytest.mark.parametrize(
    "overrides",
    [
        {"tasks": 0},
        {"runs": 0},
        {"iterations": 0},
        {"warmups": -1},
    ],
)
def test_benchmark_scenario_rejects_invalid_sizes(
    overrides: dict[str, int],
) -> None:
    with pytest.raises(ValueError):
        BenchmarkScenario(**overrides)


def test_benchmark_runs_the_analysis_engine() -> None:
    result = run_benchmark(BenchmarkScenario(tasks=3, runs=5, iterations=1, warmups=0))

    assert result["benchmark_version"] == "1"
    assert result["scenario"] == {
        "tasks": 3,
        "runs": 5,
        "iterations": 1,
        "warmups": 0,
        "edges": 2,
        "task_run_records": 15,
    }
    assert result["results"]["analyzed_tasks"] == 3
