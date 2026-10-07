select
    liquidations.event_id,
    dates.date_key,
    chains.chain_key,
    protocols.protocol_key,
    collateral.token_key as collateral_token_key,
    debt.token_key as debt_token_key,
    liquidations.block_timestamp,
    liquidations.block_number,
    liquidations.transaction_hash,
    liquidations.log_index,
    liquidations.collateral_asset,
    liquidations.debt_asset,
    liquidations.user,
    liquidations.liquidator,
    liquidations.debt_to_cover_raw,
    {{ normalize_token_amount('liquidations.debt_to_cover_raw', 'debt.decimals') }} as debt_to_cover,
    liquidations.liquidated_collateral_amount_raw,
    {{ normalize_token_amount(
        'liquidations.liquidated_collateral_amount_raw',
        'collateral.decimals'
    ) }} as liquidated_collateral_amount,
    liquidations.receive_atoken,
    liquidations.ingested_at,
    liquidations.silver_processed_at,
    liquidations.producer_version,
    liquidations.schema_version,
    liquidations.silver_job_version
from {{ ref('stg_aave_liquidations') }} as liquidations
left join {{ ref('dim_date') }} as dates
    on liquidations.block_timestamp::date = dates.date
left join {{ ref('dim_chain') }} as chains
    on liquidations.chain = chains.chain
left join {{ ref('dim_protocol') }} as protocols
    on liquidations.protocol = protocols.protocol
left join {{ ref('dim_token') }} as collateral
    on liquidations.chain = collateral.chain
    and liquidations.collateral_asset = collateral.token_address
left join {{ ref('dim_token') }} as debt
    on liquidations.chain = debt.chain
    and liquidations.debt_asset = debt.token_address
