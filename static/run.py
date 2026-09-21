"""CLI for the static reference loader.

    python -m static.run --status          # what versions exist
    python -m static.run --load            # load if the feed changed
    python -m static.run --load --force    # load even if the ETag matches
    python -m static.run --neighborhoods   # reload the spatial layer only
    python -m static.run --retire 3        # retire a superseded version

Airflow wraps this with a BashOperator in Phase 4; it
never imports the package, same separation the producer has.

Deliberately NOT scheduled here. This is the batch side of the streaming/batch
boundary: the static feed changes on service-change dates, and a cron that
runs it hourly would mostly be a no-op that occasionally rewrites 1.1M rows
during peak service.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

import psycopg
from dotenv import load_dotenv

from static import load as loader
from static.feed import SOURCE

log = logging.getLogger("static")


def dsn() -> str:
    return (
        f"host={os.environ.get('WAREHOUSE_HOST', 'localhost')} "
        f"port={os.environ.get('WAREHOUSE_PORT', '5434')} "
        f"dbname={os.environ['POSTGRES_DB']} "
        f"user={os.environ['POSTGRES_USER']} "
        f"password={os.environ['POSTGRES_PASSWORD']}"
    )


def show_status(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT version_id, coalesce(feed_version, left(feed_etag, 14)), loaded_at, ready, retired_at,
                   coalesce(row_counts->>'trips', '?'),
                   coalesce(row_counts->>'stop_times', '?')
            FROM static.feed_version ORDER BY loaded_at DESC LIMIT 10
            """
        )
        rows = cur.fetchall()
        cur.execute("SELECT static.current_version()")
        current = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM static.neighborhoods")
        hoods = cur.fetchone()[0]

    if not rows:
        print("no static versions loaded -- run `make static-load`")
    else:
        print(f"{'ver':>4} {'feed_version':16} {'loaded':20} {'ready':6} {'trips':>8} {'stop_times':>11}")
        for vid, etag, loaded, ready, retired, trips, st in rows:
            mark = " <- current" if vid == current else (" (retired)" if retired else "")
            print(f"{vid:>4} {etag:16} {loaded:%Y-%m-%d %H:%M:%S}  {str(ready):6} "
                  f"{trips:>8} {st:>11}{mark}")
    print(f"\nneighborhoods: {hoods} polygons")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="static.run", description=__doc__)
    action = ap.add_mutually_exclusive_group(required=True)
    action.add_argument("--status", action="store_true", help="show loaded versions")
    action.add_argument("--load", action="store_true", help="load the GTFS feed")
    action.add_argument("--neighborhoods", action="store_true",
                        help="reload the neighborhood layer only")
    action.add_argument("--retire", type=int, metavar="VERSION",
                        help="mark a superseded version retired")
    ap.add_argument("--force", action="store_true",
                    help="load even when the ETag is unchanged")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s", datefmt="%H:%M:%S",
    )
    load_dotenv()

    with psycopg.connect(dsn()) as conn:
        if args.status:
            show_status(conn)
            return 0

        if args.retire:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE static.feed_version SET retired_at = now() "
                    "WHERE version_id = %s AND retired_at IS NULL RETURNING version_id",
                    (args.retire,),
                )
                hit = cur.fetchone()
            conn.commit()
            print(f"retired version {args.retire}" if hit else f"version {args.retire} not found or already retired")
            return 0 if hit else 1

        if args.neighborhoods:
            n = loader.load_neighborhoods(conn)
            conn.commit()
            print(f"loaded {n} neighborhood polygons")
            return 0

        # --load
        with conn.cursor() as cur:
            cur.execute(
                "SELECT feed_etag FROM static.feed_version WHERE ready "
                "ORDER BY loaded_at DESC LIMIT 1"
            )
            row = cur.fetchone()
        known_etag = None if args.force else (row[0] if row else None)

        payload, etag, last_modified = loader.fetch_zip(known_etag)
        if payload is None:
            print(f"static feed unchanged (etag {etag[:14]}) -- nothing to do")
            return 0

        log.info("loading %s bytes, etag %s", f"{len(payload):,}", etag[:14])
        # feed_info.txt is parsed from the payload and handed to
        # begin_version, so the agency label lands on the version row.
        info = loader.read_feed_info(payload)
        log.info('feed_version %s (valid %s..%s)', info.feed_version or '(absent)',
                 info.feed_start_date or '?', info.feed_end_date or '?')
        version_id = loader.begin_version(conn, etag, last_modified, info)
        counts = loader.load_gtfs(conn, payload, version_id)
        counts["shape_lines"] = loader.build_shape_lines(conn, version_id)

        # ready LAST, in the same transaction that finished the load. See the
        # module docstring in static/load.py.
        loader.mark_ready(conn, version_id, counts)
        conn.commit()

        print(loader.LoadResult(version_id, etag, counts, 0))
        print("\nreminder: the enrichment consumer pins a version at startup, "
              "so it will not pick this up until restarted.")
        return 0


if __name__ == "__main__":
    sys.exit(main())
