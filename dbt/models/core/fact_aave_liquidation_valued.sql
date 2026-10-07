select
    liquidations.*,
    debt.price_status as debt_price_status,
    debt.price_status_reason as debt_price_status_reason,
    debt.mapped_feed_address as debt_mapped_feed_address,
    debt.matched_observation_id as debt_matched_observation_id,
    debt.matched_feed_address as debt_matched_feed_address,
    debt.price_usd as debt_price_usd,
    debt.price_feed_updated_at as debt_price_feed_updated_at,
    debt.price_age_seconds as debt_price_age_seconds,
    debt.amount_usd as debt_to_cover_usd,
    collateral.price_status as collateral_price_status,
    collateral.price_status_reason as collateral_price_status_reason,
    collateral.mapped_feed_address as collateral_mapped_feed_address,
    collateral.matched_observation_id as collateral_matched_observation_id,
    collateral.matched_feed_address as collateral_matched_feed_address,
    collateral.price_usd as collateral_price_usd,
    collateral.price_feed_updated_at as collateral_price_feed_updated_at,
    collateral.price_age_seconds as collateral_price_age_seconds,
    collateral.amount_usd as liquidated_collateral_amount_usd
from {{ ref('fact_aave_liquidation') }} as liquidations
left join {{ ref('int_defi_event_token_prices') }} as debt
    on liquidations.event_id = debt.event_id
    and debt.event_domain = 'aave_liquidation'
    and debt.token_side = 'debt'
left join {{ ref('int_defi_event_token_prices') }} as collateral
    on liquidations.event_id = collateral.event_id
    and collateral.event_domain = 'aave_liquidation'
    and collateral.token_side = 'collateral'
