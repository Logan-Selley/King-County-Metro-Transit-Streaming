#!/usr/bin/env bash
# Export the real-data slice CI's dbt build runs against.
#
#     make ci-fixture      # re-export from the running warehouse
#
# WHY REAL DATA. CI's first rule (ci.yml's header) is that a green check that
# means nothing is a liability. `dbt build` against an EMPTY warehouse passes
# almost every test in this project, because the singular tests return
# failing rows and an empty table has none. The one test built to catch
# exactly that, feed_health_detects_overnight_stall, is conditional on the
# night being loaded, so on an empty warehouse it passes silently too. So CI
# builds against a small slice of what the pipeline actually wrote, chosen so
# every spec test has something to be wrong about:
#
#   enriched_vehicle_positions   2026-09-23 00:00-05:30 Pacific
#       Owl service (never silent, thinnest minute 30 positions), the upstream
#       stall (03:00-04:21, 82 silent minutes), the flapping recovery and the
#       first hour of normal service. Every feed-health test and the
#       route-deviation reconciliation have real rows to disagree with.
#     + the ten 2026-09-04 records, the stale block. Without them the stale
#       filter in stg_vehicle_positions is never exercised, and removing it
#       would still pass: those ten are what stretch the spine seventeen days
#       back when the filter is missing.
#
#   prediction_accuracy          observed 2026-09-23 07:00-07:10 Pacific
#       The AM peak, ten minutes: ~35k pairs with every lead bucket populated,
#       45-60m the thinnest at ~1,100. Enough for the curve test to see the
#       monotonic rise, small enough to commit.
#
#   bunching_alerts              all of them (~1,200 rows, ~120 kB)
#
# FILES are gzipped CSV with a header, columns in table order, so the CI job
# loads each with one `\copy raw.<table> FROM PROGRAM 'gunzip -c ...' CSV
# HEADER`. MANIFEST records the row count of each file; the CI job should
# check its loaded counts against it, so a truncated load fails there instead
# of producing a smaller, still-green build.
#
# The slice is FIXED by date, not relative to today, so re-running this
# reproduces the same files for as long as the warehouse keeps 2026-09-23
# (90-day retention, 04-retention.sql: until 2026-12-22). After that, the
# committed files are the only copy, which is the point of committing them.

set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
: "${POSTGRES_USER:?run via make ci-fixture, which loads .env}"
: "${POSTGRES_DB:?}"

psql_out() {
    docker exec -i transit_warehouse psql -X -q -v ON_ERROR_STOP=1 \
        -U "$POSTGRES_USER" -d "$POSTGRES_DB" "$@"
}

export_table() {
    local name="$1" order="$2" where="$3"
    local out="$HERE/$name.csv.gz"
    psql_out -c "\\copy (select * from raw.$name where $where order by $order) to stdout csv header" \
        | gzip -n -9 > "$out"
    # Ordered by the PRIMARY KEY, and -n keeps the name and timestamp out of
    # the gzip header: together they make an unchanged slice re-export
    # byte-identical, so git sees no change. The first version ordered by the
    # first two columns, which tie in prediction_accuracy, and that file came
    # out different on every run.
    local rows
    rows=$(( $(gunzip -c "$out" | wc -l) - 1 ))
    printf '%-28s %8d rows  %s\n' "$name" "$rows" "$(du -h "$out" | cut -f1)"
    echo "$name $rows" >> "$HERE/MANIFEST.tmp"
}

rm -f "$HERE/MANIFEST.tmp"

export_table enriched_vehicle_positions "vehicle_id, position_timestamp" \
    "(position_timestamp >= '2026-09-23 00:00-07' and position_timestamp < '2026-09-23 05:30-07')
     or position_timestamp < '2026-09-05'"

export_table prediction_accuracy \
    "start_date, trip_id, stop_id, issued_at, observed_at" \
    "observed_at >= '2026-09-23 07:00-07' and observed_at < '2026-09-23 07:10-07'"

export_table bunching_alerts "vehicle_id_a, vehicle_id_b, window_end" "true"

mv "$HERE/MANIFEST.tmp" "$HERE/MANIFEST"
echo "wrote $HERE/MANIFEST"
