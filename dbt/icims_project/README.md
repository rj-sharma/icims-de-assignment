# dbt Project Notes

This dbt project builds the local DuckDB transformation layer for the iCIMS assignment.

Start with the repository-level [../../README.md](../../README.md) for setup and run commands.

## Layers

| Layer | Path | Purpose |
| --- | --- | --- |
| Sources | `models/sources.yml` | Raw DuckDB tables and source-level freshness/volume checks |
| Staging | `models/staging/core/` | Clean, type, normalize, and deduplicate source data |
| Intermediate | `models/intermediate/` | Reusable workflow event enrichment |
| Marts | `models/marts/` | Star-schema facts, dimensions, and Time to Hire aggregate |
| Quality | `models/quality/` | Persisted anomaly audit models |
| Tests | `tests/` and model YAML files | Data quality and business-rule checks |

## Common Commands

```bash
dbt run --project-dir dbt/icims_project --vars "{run_date: '2026-05-07'}" --full-refresh
dbt source freshness --project-dir dbt/icims_project --vars "{run_date: '2026-05-07'}"
dbt test --project-dir dbt/icims_project --vars "{run_date: '2026-05-07'}"
```

Run one layer:

```bash
dbt run --project-dir dbt/icims_project --vars "{run_date: '2026-05-07'}" --select tag:stg
dbt run --project-dir dbt/icims_project --vars "{run_date: '2026-05-07'}" --select tag:fact
dbt run --project-dir dbt/icims_project --vars "{run_date: '2026-05-07'}" --select tag:dq
```

## Key Models

- `int_workflow_events_enriched`: adds application apply date, anomaly flag, and event sequence.
- `fct_applications`: one row per application with current status, hired date, and Time to Hire.
- `fct_workflow_events`: one row per workflow transition event.
- `dim_job`: current-state job dimension.
- `dim_candidate`: current-state candidate dimension enriched with education.
- `agg_time_to_hire_by_job_department`: simple reporting aggregate for the assignment metric.
- `dq_hired_before_applied_anomalies`: audit table for known hired-before-applied issues.
