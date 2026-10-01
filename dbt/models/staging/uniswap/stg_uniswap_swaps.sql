with source as (
    select *
    from {{ source('blockchain_raw', 'uniswap_swaps') }}
)

select
    event_id::varchar as event_id,
    chain::varchar as chain,
    protocol::varchar as protocol,
    block_timestamp::timestamp_tz as block_timestamp,
    producer_version::varchar as producer_version,
    schema_version::varchar as schema_version,
    silver_job_version::varchar as silver_job_version,
    source_file::varchar as source_file,
    source_file_row_number::number(38, 0) as source_file_row_number,
    loaded_at::timestamp_tz as loaded_at,
    record:event_type::varchar as event_type,
    try_to_number(record:block_number::varchar)::number(38, 0) as block_number,
    record:transaction_hash::varchar as transaction_hash,
    try_to_number(record:log_index::varchar)::number(38, 0) as log_index,
    record:pool_address::varchar as pool_address,
    try_to_date(record:event_date::varchar) as event_date,
    record:amount0_raw::varchar as amount0_raw,
    record:amount1_raw::varchar as amount1_raw,
    try_to_timestamp_tz(record:ingested_at::varchar) as ingested_at,
    try_to_timestamp_tz(record:kafka_timestamp::varchar) as kafka_timestamp,
    record:bronze_file::varchar as bronze_file,
    try_to_timestamp_tz(record:bronze_processed_at::varchar) as bronze_processed_at,
    try_to_timestamp_tz(record:silver_processed_at::varchar) as silver_processed_at
from source
