select mart.event_id
from {{ ref('mart_aave_lending_tutorial') }} as mart
left join {{ ref('dim_token') }} as tokens
    on mart.token_key = tokens.token_key
where tokens.token_key is null
    or mart.token_address != tokens.token_address
    or mart.token_symbol != tokens.symbol
    or mart.token_name != tokens.name
    or mart.token_decimals != tokens.decimals
    or (
        mart.activity_type = 'BORROW'
        and (mart.repayer is not null or mart.use_atokens is not null)
    )
    or (
        mart.activity_type = 'REPAY'
        and (
            mart.on_behalf_of is not null
            or mart.interest_rate_mode is not null
            or mart.borrow_rate_raw is not null
            or mart.referral_code is not null
        )
    )
