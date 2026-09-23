# Migrating to FlowSense 0.5

FlowSense 0.5 improves production-scale Airflow collection, authentication
resilience, and statistical drift accuracy. Existing Python, CLI, and MCP entry
points remain available, but JSON consumers must account for analysis output
schema version `1.2`.

## Analysis output schema 1.2

Each task and handoff drift result now includes:

- `direction`: `INCREASE`, `DECREASE`, or `UNCHANGED`;
- `effective_mad`: the regularized dispersion used to calculate the robust
  z-score.

The embedded policy also includes `minimum_relative_dispersion` and
`minimum_absolute_dispersion`. Consumers that validate JSON should regenerate
their model or schema with:

```bash
flowsense schema --document analysis
```

The batch envelope remains at schema version `1.0`; each successful analysis
inside it now uses schema version `1.2`.

## Drift behavior

Raw MAD is still reported without modification. Score calculation now uses the
largest of raw MAD, the configured relative floor, and the configured absolute
floor. This prevents negligible timing noise on constant baselines from being
classified as an automatic critical anomaly.

The defaults are:

```json
{
  "minimum_relative_dispersion": 0.01,
  "minimum_absolute_dispersion": 0.001
}
```

Pin explicit values in a policy file if existing alert thresholds were tuned
around the previous zero-MAD behavior.

## Airflow collection and authentication

Recent successful DAG runs and task instances use bounded or batch API calls
when supported. Older Airflow deployments automatically fall back to the
paginated per-run endpoints.

Airflow login tokens obtained through `AIRFLOW_AUTH_MODE=token` are refreshed
once after a `401` response. Basic, static bearer, and custom authentication
providers retain application-owned credential lifecycles.

## Validation checklist

1. Regenerate and compare the analysis JSON Schema.
2. Validate alerting against representative historical DAG runs.
3. Confirm the configured Airflow identity can use batch endpoints or fallback
   endpoints.
4. Run `flowsense doctor` against the target Airflow environment.
5. Perform a canary analysis before replacing a pinned 0.4 installation.
