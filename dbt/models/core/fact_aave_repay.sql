select
    repays.event_id,
    dates.date_key,
    chains.chain_key,
    protocols.protocol_key,
    tokens.token_key as reserve_token_key,
    repays.block_timestamp,
    repays.block_number,
    repays.transaction_hash,
    repays.log_index,
    repays.reserve,
    repays.user,
    repays.repayer,
    repays.amount_raw,
    {{ normalize_token_amount('repays.amount_raw', 'tokens.decimals') }} as repay_amount,
    repays.use_atokens,
    repays.ingested_at,
    repays.silver_processed_at,
    repays.producer_version,
    repays.schema_version,
    repays.silver_job_version
from {{ ref('stg_aave_repays') }} as repays
left join {{ ref('dim_date') }} as dates
    on repays.block_timestamp::date = dates.date
left join {{ ref('dim_chain') }} as chains
    on repays.chain = chains.chain
left join {{ ref('dim_protocol') }} as protocols
    on repays.protocol = protocols.protocol
left join {{ ref('dim_token') }} as tokens
    on repays.chain = tokens.chain
    and repays.reserve = tokens.token_address
