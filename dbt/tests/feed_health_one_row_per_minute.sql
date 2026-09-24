-- Grain: exactly one row per minute. Returns the duplicated minutes.
select minute_utc, count(*) as rows
from {{ ref('mart_feed_health') }}
group by 1
having count(*) > 1
