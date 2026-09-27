-- The stall that motivated this mart must appear in it.
--
-- Measured from raw.enriched_vehicle_positions: on 2026-09-23 no position
-- arrived from 03:00 to 04:21 Pacific, 82 consecutive minutes, while Metro's
-- upstream objects stopped changing (producer log: stale peaked at 3998s).
-- The minute 03:30 local sits inside it, so its row must be silent and belong
-- to a run of at least 80 minutes.
--
-- Conditional on that night still being in the warehouse: once partition
-- retention drops it, there is nothing left to assert and the
-- test passes vacuously rather than failing forever.
with night_is_loaded as (
    select 1
    from {{ ref('stg_vehicle_positions') }}
    where position_local between '2026-09-23 02:00' and '2026-09-23 05:00'
    limit 1
),
the_minute as (
    select is_silent, stall_minutes
    from {{ ref('mart_feed_health') }}
    where minute_local = '2026-09-23 03:30'
)
select 'stall at 2026-09-23 03:30 Pacific not detected' as failure,
       (select is_silent from the_minute) as is_silent,
       (select stall_minutes from the_minute) as stall_minutes
from night_is_loaded
where not coalesce((select is_silent and stall_minutes >= 80 from the_minute), false)
