# Performance benchmarks

FlowSense includes a deterministic synthetic benchmark for measuring the
default analysis engine independently from Airflow and network latency.

The benchmark creates an ordered chain DAG, generates repeatable task timings,
injects anomalies into the latest run, and reports:

- analysis execution time;
- total time including versioned output serialization;
- peak Python memory observed through `tracemalloc`;
- scenario dimensions and analyzed task count.

Run the default scenario:

```bash
uv run python -m benchmarks.benchmark_analysis
```

Run a larger scenario and save machine-readable results:

```bash
uv run python -m benchmarks.benchmark_analysis \
  --tasks 500 \
  --runs 100 \
  --iterations 5 \
  --output benchmark-results.json
```

The GitHub `Analysis benchmark` workflow provides the same parameters and
uploads the JSON result as a workflow artifact.

Benchmark results are observational and do not use hard pass/fail time limits.
Shared CI runners have variable performance, so fixed thresholds would create
flaky checks. Compare runs produced with the same scenario, Python version, and
similar hardware. Introduce performance gates only after a stable baseline has
been collected on controlled runners.
