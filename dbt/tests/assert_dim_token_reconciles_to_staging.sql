with source_count as (
    select count(*) as row_count from {{ ref('stg_tokens') }}
),
dimension_count as (
    select count(*) as row_count from {{ ref('dim_token') }}
)

select source_count.row_count as source_rows, dimension_count.row_count as dimension_rows
from source_count
cross join dimension_count
where source_count.row_count != dimension_count.row_count
