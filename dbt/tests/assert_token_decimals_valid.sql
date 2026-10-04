select chain, token_address, decimals
from {{ ref('stg_tokens') }}
where decimals < 0 or decimals > 255
