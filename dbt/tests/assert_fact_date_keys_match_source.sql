select 'uniswap_swap' as fact_name, event_id as source_id
from {{ ref('fact_uniswap_swap') }}
where date_key != to_number(to_char(block_timestamp::date, 'YYYYMMDD'))

union all

select 'aave_borrow', event_id from {{ ref('fact_aave_borrow') }}
where date_key != to_number(to_char(block_timestamp::date, 'YYYYMMDD'))

union all

select 'aave_repay', event_id from {{ ref('fact_aave_repay') }}
where date_key != to_number(to_char(block_timestamp::date, 'YYYYMMDD'))

union all

select 'aave_liquidation', event_id from {{ ref('fact_aave_liquidation') }}
where date_key != to_number(to_char(block_timestamp::date, 'YYYYMMDD'))

union all

select 'market_price', observation_id from {{ ref('fact_market_price') }}
where date_key != to_number(to_char(feed_updated_at::date, 'YYYYMMDD'))
