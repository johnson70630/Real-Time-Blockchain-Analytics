select
    chain,
    feed_address,
    round_id,
    count(*) as row_count
from {{ ref('mart_chainlink_oracle_tutorial') }}
group by chain, feed_address, round_id
having count(*) != 1
