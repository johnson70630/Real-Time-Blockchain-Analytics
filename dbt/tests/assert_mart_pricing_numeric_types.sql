with expected as (
    select 'MART_CHAINLINK_ORACLE_TUTORIAL' as table_name, 'PRICE' as column_name, 38 as precision, 18 as scale
    union all select 'MART_CHAINLINK_ORACLE_TUTORIAL', 'PREVIOUS_PRICE', 38, 18
    union all select 'MART_CHAINLINK_ORACLE_TUTORIAL', 'PRICE_CHANGE', 38, 18
    union all select 'MART_CHAINLINK_ORACLE_TUTORIAL', 'PRICE_CHANGE_PCT', 38, 18
    union all select 'MART_POINT_IN_TIME_PRICING_TUTORIAL', 'NORMALIZED_AMOUNT', 38, 18
    union all select 'MART_POINT_IN_TIME_PRICING_TUTORIAL', 'PRICE', 38, 18
    union all select 'MART_POINT_IN_TIME_PRICING_TUTORIAL', 'VALUATION_PRICE_USD', 38, 18
    union all select 'MART_POINT_IN_TIME_PRICING_TUTORIAL', 'AMOUNT_USD', 38, 8
    union all select 'MART_POINT_IN_TIME_PRICING_TUTORIAL', 'NEXT_PRICE', 38, 18
),

actual as (
    select
        table_name,
        column_name,
        data_type,
        numeric_precision,
        numeric_scale
    from {{ target.database }}.information_schema.columns
    where table_schema = 'MARTS'
)

select
    expected.*,
    actual.data_type,
    actual.numeric_precision,
    actual.numeric_scale
from expected
left join actual
    on expected.table_name = actual.table_name
    and expected.column_name = actual.column_name
where actual.column_name is null
    or actual.data_type != 'NUMBER'
    or expected.precision != actual.numeric_precision
    or expected.scale != actual.numeric_scale
