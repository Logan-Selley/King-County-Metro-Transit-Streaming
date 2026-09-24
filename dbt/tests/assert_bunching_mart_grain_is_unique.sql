{{ config(severity='error') }}

{#
  (route_short_name, direction_id, local_hour) is the bunching mart's grain, and
  no single column of it is unique, so this cannot be a column test. A duplicate
  row means the group by is wrong -- most likely a column added to the select
  without being added to the group by, which would also silently deflate every
  count in the mart.
#}

select
    route_short_name,
    direction_id,
    local_hour,
    count(*) as mart_rows

from {{ ref('mart_bunching_by_route_hour') }}

group by 1, 2, 3
having count(*) > 1
