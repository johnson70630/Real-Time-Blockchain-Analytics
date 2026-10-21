# Pipeline observability foundation

`CONTROL.PIPELINE_RUNS` is the provider-neutral execution history for finite
production pipelines. Airflow remains the detailed task history; this table
stores one aggregate record per logical pipeline run.

## Terminology

- **Status** records whether execution is running, succeeded, failed, skipped,
  or completed with no work.
- **Progress** is derived from `watermark_after > watermark_before` and is not a
  synonym for success.
- **Freshness** is derived from the source/safe head and the committed source
  position. Milestone 11.3A does not define an SLA.
- **Data quality** is represented by quarantine counts and compact test/result
  counters. Detailed dbt and Airflow results stay in their native systems.
- **Performance** includes duration, RPC requests, records, and warehouse work.

## Existing signal inventory

Before 11.3A, Aave capture summaries exposed run ID, observed/safe head, source
range, logical and completed ranges, RPC attempts, adaptive splits, event-family
counts, and quarantine count. Chainlink exposed global block positions, planned
rounds, per-feed observations, batches, replayed batches, and RPC requests.
Uniswap exposed source range, source head, pool/source-contract identity,
logical/completed ranges, RPC requests, splits, Swap/Mint/Burn counts, and
quarantine count. The RAW loader already returned COPY files/rows, MERGE
inserts/updates, final rows, and duplicate keys. Airflow and dbt retained task
duration, retries, model results, and test results in their own histories.

The missing capability was a durable cross-stage lifecycle: no single record
proved a run started, finished, advanced its source position, or failed at a
high-level boundary. Existing log metrics were also not queryable as run
history.

## Run lifecycle

Every instrumented execution inserts `RUNNING` at operational start and makes
one guarded transition to `SUCCEEDED`, `NO_OP`, `SKIPPED`, or `FAILED`.
`NO_OP` means the job ran correctly but found no eligible work. A process crash
leaves `RUNNING`, making incomplete/stale executions visible. Automated stale
run cleanup is intentionally out of scope.

The natural identity is `pipeline_name + run_id`. Runtime code guards insert
and finish transitions because Snowflake primary-key constraints are metadata,
not enforcement. Stable names include `aave_forward`, `chainlink_forward`,
`uniswap_forward`, and `daily_warehouse_pipeline`; RPC providers are not part
of pipeline identity.

Generic scalar columns hold positions, aggregate row counts, duration, and
sanitized failure classification. `METRIC_DETAILS` is restricted by the Python
contract to non-negative integer counters such as event-family, per-feed,
RPC-range, model, and test counts. It cannot carry endpoint strings, exception
messages, event payloads, stack traces, credentials, or key material.

## Instrumentation

- Aave, Chainlink, and Uniswap use their existing capture summaries; no source
  reads are repeated for metrics.
- RAW child processes atomically add COPY/MERGE counters to their parent run.
- The daily warehouse DAG creates one pipeline-level run while Airflow retains
  task-level detail. Its terminal record includes RAW totals plus dbt manifest
  model/test counts.
- Correction replay uses source-specific logical names, records immutable
  watermark before/after values, and reports candidate counts without payloads.

## Failure boundary

Observability writes are best-effort. Connection, start, child-metric, or
finish failures log only the operation and exception class. They do not roll
back source, Bronze, Silver, Snowflake RAW, dbt, or watermark work. If pipeline
work succeeds and the final metrics update fails, pipeline data remains
committed and the observability failure is handled separately. Conversely,
handled pipeline failures record only a high-level stage and exception class;
the exception message is never stored.

The runtime role needs only `USAGE` on the existing database and CONTROL schema
plus `SELECT`, `INSERT`, and `UPDATE` on `PIPELINE_RUNS`. It receives no CREATE,
DELETE, TRUNCATE, or ownership privilege. Provisioning is an explicit owner
action using `warehouse/provision_pipeline_runs.sql`.
