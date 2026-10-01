# Durable S3 data lake and Snowflake RAW landing

## Architecture

```text
WebSocket producer / market-data poller
                 |
               Kafka
                 |
               Spark
        +--------+---------+
        | S3 Bronze        | raw, append-oriented, replayable
        | S3 Silver        | validated, typed, deduplicated
        | S3 Silver/enriched | point-in-time price enrichment
        | S3 quarantine    | rejected records and reasons
        +--------+---------+
                 |
       Snowflake storage integration
                 |
          external Parquet stage
                 |
       COPY landing + keyed MERGE
                 |
      BLOCKCHAIN_ANALYTICS.RAW
```

Spark remains responsible for streaming ingestion, validation, normalization,
deduplication, and point-in-time enrichment. S3 is the durable replay and
lineage boundary. Snowflake receives canonical datasets for analytical access;
it does not replace ingestion or transformation. dbt facts, dimensions, and
marts are deferred until the warehouse landing contract is stable.

## Storage configuration

`config/storage.py` owns the logical dataset registry. The default
`STORAGE_MODE=local` resolves it below the repository's `data/` directory and
requires neither AWS nor Snowflake. `STORAGE_MODE=s3` requires:

```shell
export STORAGE_MODE=s3
export DATA_LAKE_BUCKET=your-private-data-lake
export DATA_LAKE_PREFIX=blockchain-analytics/production
export AWS_REGION=us-west-2
```

The cloud layout is:

```text
s3://<bucket>/<prefix>/bronze/swaps/...
s3://<bucket>/<prefix>/bronze/market_data/...
s3://<bucket>/<prefix>/silver/swaps/...
s3://<bucket>/<prefix>/silver/aave_v3/...
s3://<bucket>/<prefix>/silver/market_prices/...
s3://<bucket>/<prefix>/silver/enriched/...
s3://<bucket>/<prefix>/quarantine/...
s3://<bucket>/<prefix>/checkpoints/...
```

Existing Hive partitions are preserved. Local writers retain atomic filesystem
replacement. S3 Spark batch writers use Hadoop's object-store writer with the
existing overwrite semantics; streaming Bronze remains append-oriented and
checkpointed.

## AWS authentication and IAM

No AWS keys are read into application settings. S3A explicitly extends
Hadoop's standard provider list with the AWS SDK v2
`ProfileCredentialsProvider`. For local development, the Spark JVM inherits
`AWS_PROFILE`, and the AWS SDK reads the selected profile from the standard
`~/.aws/credentials` and `~/.aws/config` files. Environment credentials and
AWS web-identity credentials plus Hadoop's IAM instance/container provider
remain available for production workload roles.
Production should use an IAM role with only the required bucket-prefix actions:

- Spark jobs: list the configured prefix and read/write/delete objects only
  within the dataset and checkpoint prefixes they own.
- Snowflake integration role: list the Silver prefix and read Silver objects.
- The bucket should block public access and use encryption and versioning.

Do not commit access keys, secret keys, session tokens, Snowflake passwords, or
private keys. `.env.example` contains names and non-secret defaults only.

## Snowflake landing design

The first warehouse milestone intentionally supports only Uniswap V3 swaps.
The database, schemas, storage integration, file format, and external stage are
already provisioned and are not recreated by the generator. The existing
objects used by the generated SQL are:

```text
BLOCKCHAIN_ANALYTICS.RAW.SILVER_S3_STAGE
BLOCKCHAIN_ANALYTICS.RAW.PARQUET_FORMAT
```

The generator creates only two persistent tables:

```text
BLOCKCHAIN_ANALYTICS.RAW.UNISWAP_SWAPS_LANDING
BLOCKCHAIN_ANALYTICS.RAW.UNISWAP_SWAPS
```

`COPY INTO` reads `@SILVER_S3_STAGE/swaps/` and applies
`PATTERN='.*[.]parquet$'` at file selection time. This prevents Spark control
objects such as `_SUCCESS` from reaching the Parquet decoder. The landing table
retains the complete source record, filename, file row number, load timestamp,
and merge timestamp.

Snowflake COPY history skips an identical object on a normal rerun. A
deterministic `MERGE` adds logical protection by matching `event_id` and keeping
the latest row by load time, source filename, and source-file row number. RAW
retains the complete record as `VARIANT` plus event ID, chain, protocol, block
timestamp, version fields, `loaded_at`, and `source_file`. Generated SQL never
sets `FORCE=TRUE`.

## Local validation and operation

```shell
make kafka-up
make producer
make market-data
make bronze
make market-data-bronze
make market-data-silver
make silver
make aave-silver
make price-enrichment
make quality
make check
```

These commands use local Parquet unless `STORAGE_MODE=s3` is set. Unit tests do
not contact AWS or Snowflake.

## Snowflake SQL review and loading

The storage integration and stage already use the Snowflake-managed AWS IAM
identity. Local AWS credentials are not used by Snowflake. Generate table DDL
for review without connecting:

```shell
uv run python -m warehouse.generate_sql --setup-only
```

Generate the Uniswap COPY and MERGE SQL separately:

```shell
uv run python -m warehouse.generate_sql \
  --loads-only \
  --dataset silver_uniswap_swaps
```

After review, run the DDL first and the load SQL second using a Snowflake role
with warehouse usage, stage/file-format usage, and table privileges in `RAW`.
Re-running the load SQL is safe and incremental.

Snowflake client authentication is intentionally external to generated SQL.
Use SSO/external-browser authentication for operators or a protected key-pair
for automation. Keep private keys in a secrets manager, never in this repo.
