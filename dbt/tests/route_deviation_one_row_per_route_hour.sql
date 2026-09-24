-- Grain: one row per (route_short_name, service_date, local_hour).
select route_short_name, service_date, local_hour, count(*) as rows
from {{ ref('mart_route_deviation_hourly') }}
group by 1, 2, 3
having count(*) > 1
