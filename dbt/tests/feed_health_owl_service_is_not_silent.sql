-- The guard against the opposite mistake: thin is not silent.
--
-- Measured: from 00:00 to 02:59 Pacific on 2026-09-23 the thinnest minute
-- still carried 30 positions (average 198). Owl service is sparse but never
-- empty, so any silent minute in that window is a bug in the mart, not a
-- quiet feed. Returns the offending minutes.
select minute_local, positions
from {{ ref('mart_feed_health') }}
where minute_local between '2026-09-23 00:00' and '2026-09-23 02:59'
  and is_silent
