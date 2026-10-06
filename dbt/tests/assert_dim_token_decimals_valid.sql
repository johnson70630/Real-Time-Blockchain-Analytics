select token_key, decimals
from {{ ref('dim_token') }}
where decimals < 0 or decimals > 255
