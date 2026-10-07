select
    borrows.event_id,
    dates.date_key,
    chains.chain_key,
    protocols.protocol_key,
    tokens.token_key as reserve_token_key,
    borrows.block_timestamp,
    borrows.block_number,
    borrows.transaction_hash,
    borrows.log_index,
    borrows.reserve,
    borrows.user,
    borrows.on_behalf_of,
    borrows.amount_raw,
    {{ normalize_token_amount('borrows.amount_raw', 'tokens.decimals') }} as borrow_amount,
    borrows.interest_rate_mode,
    borrows.borrow_rate_raw,
    borrows.referral_code,
    borrows.ingested_at,
    borrows.silver_processed_at,
    borrows.producer_version,
    borrows.schema_version,
    borrows.silver_job_version
from {{ ref('stg_aave_borrows') }} as borrows
left join {{ ref('dim_date') }} as dates
    on borrows.block_timestamp::date = dates.date
left join {{ ref('dim_chain') }} as chains
    on borrows.chain = chains.chain
left join {{ ref('dim_protocol') }} as protocols
    on borrows.protocol = protocols.protocol
left join {{ ref('dim_token') }} as tokens
    on borrows.chain = tokens.chain
    and borrows.reserve = tokens.token_address
