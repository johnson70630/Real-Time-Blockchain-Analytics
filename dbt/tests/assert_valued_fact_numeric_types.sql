with expected as (
    select 'FACT_UNISWAP_SWAP_VALUED' as table_name, 'TOKEN0_PRICE_USD' as column_name, 38 as precision, 18 as scale
    union all select 'FACT_UNISWAP_SWAP_VALUED', 'TOKEN1_PRICE_USD', 38, 18
    union all select 'FACT_UNISWAP_SWAP_VALUED', 'AMOUNT0_USD', 38, 8
    union all select 'FACT_UNISWAP_SWAP_VALUED', 'AMOUNT1_USD', 38, 8
    union all select 'FACT_AAVE_BORROW_VALUED', 'PRICE_USD', 38, 18
    union all select 'FACT_AAVE_BORROW_VALUED', 'AMOUNT_USD', 38, 8
    union all select 'FACT_AAVE_REPAY_VALUED', 'PRICE_USD', 38, 18
    union all select 'FACT_AAVE_REPAY_VALUED', 'AMOUNT_USD', 38, 8
    union all select 'FACT_AAVE_LIQUIDATION_VALUED', 'DEBT_PRICE_USD', 38, 18
    union all select 'FACT_AAVE_LIQUIDATION_VALUED', 'DEBT_TO_COVER_USD', 38, 8
    union all select 'FACT_AAVE_LIQUIDATION_VALUED', 'COLLATERAL_PRICE_USD', 38, 18
    union all select 'FACT_AAVE_LIQUIDATION_VALUED', 'LIQUIDATED_COLLATERAL_AMOUNT_USD', 38, 8
),
actual as (
    select
        table_name,
        column_name,
        data_type,
        numeric_precision,
        numeric_scale
    from {{ target.database }}.information_schema.columns
    where table_schema = 'CORE'
)
select expected.*, actual.data_type, actual.numeric_precision, actual.numeric_scale
from expected
left join actual
    on expected.table_name = actual.table_name
    and expected.column_name = actual.column_name
where actual.column_name is null
    or actual.data_type != 'NUMBER'
    or expected.precision != actual.numeric_precision
    or expected.scale != actual.numeric_scale
