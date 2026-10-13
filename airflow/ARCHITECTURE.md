# Airflow Development Foundation

This directory contains the local orchestration runtime. It does not replace
Kafka or Spark Structured Streaming and does not run any production data jobs.

## Runtime

- Apache Airflow `3.1.8` on Python `3.12`
- `LocalExecutor` for finite local tasks
- PostgreSQL 16 metadata database
- Airflow API server, scheduler, and DAG processor
- Simple Auth Manager in local-development all-admin mode

Airflow is isolated in containers, so its dependencies are not installed into
the project virtual environment. Runtime API signing material is generated into
the `airflow-config` Docker volume during initialization and is not stored in
the repository.

## Commands

```bash
make airflow-init
make airflow-up
make airflow-test
make airflow-logs
make airflow-down
```

The UI is available at <http://localhost:8081>. Both
`airflow_smoke_test` and `daily_warehouse_pipeline` are manual
(`schedule=None`) and have catchup disabled.

## Warehouse runtime

The Airflow image contains a separate `/opt/warehouse-venv` with only dbt and
the Snowflake connector. Project modules, the dbt project, and local credential
directories are mounted at runtime; credentials are never copied into the
image. RAW loads use the reusable `warehouse.load_raw` module with
`BLOCKCHAIN_PIPELINE_ROLE`, while dbt keeps its existing profile and
`BLOCKCHAIN_DBT_ROLE`. Both paths use key-pair authentication.

The warehouse DAG validates inputs, runs independent RAW loads, executes dbt
seed/staging/core/marts boundaries, and finishes with the existing singular
reconciliation tests. Snowflake tasks receive one retry for transient failures;
deterministic dbt test failures are not retried.

## Job boundaries

Continuous services remain independently supervised outside Airflow:

- `make producer`
- `make market-data`
- `make bronze`
- `make market-data-bronze`

Finite Silver/Gold builders and bounded Aave or Chainlink backfills remain
candidates for separate future DAGs. Warehouse SQL and transformation logic
stay in their tested Python/dbt entrypoints rather than in DAG files.

Future Snowflake orchestration should retain separation of duties:

- RAW landing uses `BLOCKCHAIN_PIPELINE_ROLE`.
- dbt transformations use `BLOCKCHAIN_DBT_ROLE`.

Credentials must be supplied at runtime through mounted AWS shared-credential
files, environment variables, or an Airflow secrets backend. Snowflake private
keys and passwords must never be copied into images or committed.
