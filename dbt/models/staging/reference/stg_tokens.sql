with ranked as (
    select
        chain::varchar as chain,
        token_address::varchar as token_address,
        symbol::varchar as symbol,
        name::varchar as name,
        decimals::number(38, 0) as decimals,
        metadata_source::varchar as metadata_source,
        metadata_block_number::number(38, 0) as metadata_block_number,
        ingested_at::timestamp_tz as ingested_at,
        processed_at::timestamp_tz as processed_at,
        source_file::varchar as source_file,
        source_file_row_number::number(38, 0) as source_file_row_number,
        loaded_at::timestamp_tz as loaded_at,
        row_number() over (
            partition by chain, token_address
            order by loaded_at desc, source_file desc, source_file_row_number desc
        ) as row_rank
    from {{ source('blockchain_raw', 'tokens') }}
)

select
    chain,
    token_address,
    symbol,
    name,
    decimals,
    metadata_source,
    metadata_block_number,
    ingested_at,
    processed_at,
    source_file,
    source_file_row_number,
    loaded_at
from ranked
where row_rank = 1
