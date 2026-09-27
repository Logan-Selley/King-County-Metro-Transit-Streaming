{#
  Bunching alerts per route per direction per local hour.

  Answers the proposal's own example question: which segments bunch worst during
  the PM peak. A spot-check put 40% of alerts in 16:00-18:00 and G Line, E Line
  and route 7 on top; this mart is what that check gets re-derived from.

  ROUTE AND DIRECTION, NOT ROUTE ALONE. A segment is one direction of one route:
  a pair of buses bunched northbound on Aurora is not evidence about the
  southbound side, and the proposal's question is about segments.

  LOCAL HOUR from the staging model, which converts the timezone once. The hour
  is the hour the window CLOSED, which is the alert's event time.

  THE MEDIANS, NOT AVERAGES. gap_ft and the deviations are heavy-tailed: a
  single pair separated by a missed trip moves the mean enough to change which
  segment looks worst, and the question is about the typical case. Counts are
  alongside so a median over four alerts is visibly a median over four alerts.
#}

with alerts as (

    select * from {{ ref('stg_bunching_alerts') }}

),

by_segment_hour as (

    select
        route_short_name,
        direction_id,
        local_hour,

        count(*)::integer                                               as alerts,

        round(
            percentile_cont(0.5) within group (order by gap_ft)::numeric, 1
        )                                                               as median_gap_ft,

        -- NULL when Metro had no schedule estimate for either bus, which is
        -- not zero. percentile_cont skips NULLs, so the median is over the
        -- buses that had an estimate; the counts above say how many there were.
        round(
            percentile_cont(0.5) within group (order by deviation_a)::numeric
        )::integer                                                      as median_deviation_a_s,
        round(
            percentile_cont(0.5) within group (order by deviation_b)::numeric
        )::integer                                                      as median_deviation_b_s

    from alerts
    group by 1, 2, 3

)

select * from by_segment_hour

