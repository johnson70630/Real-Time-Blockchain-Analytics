with ranked as (
    select
        chain::varchar as chain,
        protocol::varchar as protocol,
        pool_address::varchar as pool_address,
        token0_address::varchar as token0_address,
        token1_address::varchar as token1_address,
        fee_tier::number(38, 0) as fee_tier,
        tick_spacing::number(38, 0) as tick_spacing,
        factory_address::varchar as factory_address,
        metadata_source::varchar as metadata_source,
        factory_verified::boolean as factory_verified,
        created_block::number(38, 0) as created_block,
        created_transaction_hash::varchar as created_transaction_hash,
        created_log_index::number(38, 0) as created_log_index,
        created_block_timestamp::timestamp_tz as created_block_timestamp,
        ingested_at::timestamp_tz as ingested_at,
        producer_version::varchar as producer_version,
        schema_version::varchar as schema_version,
        processed_at::timestamp_tz as processed_at,
        source_file::varchar as source_file,
        source_file_row_number::number(38, 0) as source_file_row_number,
        loaded_at::timestamp_tz as loaded_at,
        row_number() over (
            partition by chain, protocol, pool_address
            order by loaded_at desc, source_file desc, source_file_row_number desc
        ) as row_rank
    from {{ source('blockchain_raw', 'uniswap_v3_pools') }}
)

select
    chain,
    protocol,
    pool_address,
    token0_address,
    token1_address,
    fee_tier,
    tick_spacing,
    factory_address,
    metadata_source,
    factory_verified,
    created_block,
    created_transaction_hash,
    created_log_index,
    created_block_timestamp,
    ingested_at,
    producer_version,
    schema_version,
    processed_at,
    source_file,
    source_file_row_number,
    loaded_at
from ranked
where row_rank = 1
