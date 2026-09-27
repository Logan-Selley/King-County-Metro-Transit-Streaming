-- An alert raised while the feed was live has a location, and the location is
-- in King County. Returns the alerts that break either rule.
--
-- WHY "WHILE THE FEED WAS LIVE" AND NOT "EVERY ALERT". An alert whose positions
-- are not in the warehouse cannot be placed, and that is not the mart's fault:
-- CI's fixture holds all 1,222 alerts but positions only for 2026-09-23
-- 00:00-05:30 Pacific, so all but 2 of its alerts have nothing to be placed
-- with. The obvious bound, "alerts inside the positions' time span", does not
-- work either: the fixture's ten stale 2026-09-04 records stretch that span
-- back seventeen days and would demand locations for every alert of 09-21 and
-- 09-22. mart_feed_health already knows which minutes had data, so the test
-- asks it. On the live warehouse that is nearly every minute; in CI it is the
-- 2 alerts that can be checked, which is thin but real.
--
-- MEASURED, 2026-09-27: 198 of 198 live alerts over two hours had both
-- vehicles' positions in the window. Positions can be lost (1,138 were at
-- 17:55 on 09-24), so an alert in a live minute with no location is a real
-- failure, not a reason to loosen this to a percentage.
--
-- The bounding box is producer/errors.py's KING_COUNTY_BBOX. Every position
-- was checked against it before it could be enriched, and the midpoint of two
-- points inside a box is inside it too, so a location outside it can only mean
-- the columns are wrong: latitude and longitude swapped, or a value that did
-- not come from the two positions at all. It cannot catch the wrong vehicle;
-- the one-row-per-alert test and the trip match are what guard that.
with live_minutes as (

    select minute_utc
    from {{ ref('mart_feed_health') }}
    where not is_silent

),

alerts as (

    select *
    from {{ ref('mart_bunching_alerts') }}

)

select a.vehicle_id_a, a.vehicle_id_b, a.window_at, a.latitude, a.longitude,
       case when a.latitude is null then 'not located' else 'outside King County' end as failure
from alerts a
join live_minutes m
  on m.minute_utc = date_trunc('minute', a.window_at) - interval '1 minute'
where a.latitude is null
   or a.longitude is null
   or a.longitude not between -122.60 and -121.05
   or a.latitude  not between   47.10 and   47.85
