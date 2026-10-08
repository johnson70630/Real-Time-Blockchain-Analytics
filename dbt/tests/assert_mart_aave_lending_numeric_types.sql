with expected as (
    select 'AMOUNT' as column_name, 38 as precision, 18 as scale
    union all select 'PRICE_USD', 38, 18
    union all select 'AMOUNT_USD', 38, 8
),

actual as (
    select
        column_name,
        data_type,
        numeric_precision,
        numeric_scale
    from {{ target.database }}.information_schema.columns
    where table_schema = 'MARTS'
        and table_name = 'MART_AAVE_LENDING_TUTORIAL'
)

select
    expected.*,
    actual.data_type,
    actual.numeric_precision,
    actual.numeric_scale
from expected
left join actual
    on expected.column_name = actual.column_name
where actual.column_name is null
    or actual.data_type != 'NUMBER'
    or expected.precision != actual.numeric_precision
    or expected.scale != actual.numeric_scale
