"""Load the GTFS zip and the neighborhood layer into PostGIS.

--- the ordering that matters ---

A version is written, THEN marked ready. Never the other way round.

`static.feed_version.ready` is what `static.current_version()` filters on, and
the enrichment consumer resolves through that function. If a version were
marked ready before stop_times finished loading, the consumer would start
joining against a version whose trips exist and whose schedule does not: every
trip would resolve, every deviation would be null, and nothing would look
broken. Mark it ready last, in the same transaction that finishes the load.

--- why versions accumulate instead of replacing ---

Realtime trip_ids reference whichever static version was current when the trip
was scheduled. Truncate-and-reload mid-service-day and every in-flight trip
stops resolving at once, so the DLQ rate jumps from its ~2.6% baseline to
~100%. Retiring an old version is a separate, deliberate step.
"""

from __future__ import annotations

import csv
import io
import json
import logging
import zipfile
from dataclasses import dataclass

import psycopg
import requests

from producer.errors import FeedError
from static.feed import SOURCE, GtfsTable, FEED_INFO, parse_feed_info, FeedInfo

log = logging.getLogger("static.load")


@dataclass
class LoadResult:
    version_id: int
    etag: str
    row_counts: dict[str, int]
    neighborhoods: int

    def __str__(self) -> str:
        rows = ", ".join(f"{k}={v:,}" for k, v in sorted(self.row_counts.items()))
        return f"version {self.version_id} [{self.etag[:12]}]: {rows}, neighborhoods={self.neighborhoods:,}"


# --- COPY and version plumbing -----------------------------------------------


def copy_rows(conn: psycopg.Connection, table: str, columns: list[str], rows) -> int:
    """Stream rows into a table with COPY.

    COPY rather than executemany because stop_times is 1.1M rows per version
    and this runs on every service change. The difference is seconds against
    minutes, and a load that takes minutes is one people skip.

    `rows` is an iterable of sequences, consumed lazily -- do not materialise
    the CSV into a list before calling this or the memory saving is lost.
    """
    cols = ", ".join(columns)
    n = 0
    with conn.cursor() as cur:
        with cur.copy(f"COPY {table} ({cols}) FROM STDIN") as copy:
            for row in rows:
                copy.write_row(row)
                n += 1
    return n


def begin_version(conn: psycopg.Connection, etag: str, last_modified: str | None, info: FeedInfo) -> int:
    """Insert a not-ready version row and return its id.

    ON CONFLICT (feed_etag) DO UPDATE rather than a second row, because one
    etag is one version. Re-running a load for an unchanged feed therefore
    returns the existing id, which lets the caller decide whether to redo the
    work or skip.

    The feed_info columns merge with COALESCE rather than taking EXCLUDED
    flat. Same etag means same bytes, so the label already recorded is right
    by construction -- but a parse that yields NULL must not be able to erase
    it, and a NULL label written by a run that could not parse feed_info gets
    repaired by a later one that can. COALESCE does both; plain EXCLUDED does
    neither.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO static.feed_version AS fv
                (feed_etag, last_modified, feed_version, feed_start_date, feed_end_date)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (feed_etag) DO UPDATE SET
                feed_etag = EXCLUDED.feed_etag,
                feed_version = COALESCE(EXCLUDED.feed_version, fv.feed_version),
                feed_start_date = COALESCE(EXCLUDED.feed_start_date, fv.feed_start_date),
                feed_end_date = COALESCE(EXCLUDED.feed_end_date, fv.feed_end_date)
            RETURNING version_id, ready
            """,
            (etag, last_modified, info.feed_version, info.feed_start_date,
             info.feed_end_date),
        )
        version_id, ready = cur.fetchone()
    if ready:
        log.info("version %s already loaded and ready for etag %s", version_id, etag[:12])
    return version_id


def mark_ready(conn: psycopg.Connection, version_id: int, row_counts: dict) -> None:
    """Flip a fully-loaded version to ready. Call LAST."""
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE static.feed_version SET ready = true, row_counts = %s WHERE version_id = %s",
            (json.dumps(row_counts), version_id),
        )


def open_member(zf: zipfile.ZipFile, name: str) -> csv.DictReader:
    """CSV reader over one zip member.

    utf-8-sig, not utf-8: GTFS files routinely carry a BOM, and without this
    the first column of the header becomes '﻿route_id' and every lookup
    of 'route_id' silently misses.
    """
    raw = io.TextIOWrapper(zf.open(name), encoding="utf-8-sig", newline="")
    return csv.DictReader(raw)


# --- fetching ------------------------------------------------------------

def _session() -> requests.Session:
    """A session with the project's User-Agent. Both fetches in this module
    use it, so the UA cannot drift between the zip and the ArcGIS layer."""
    s = requests.Session()
    s.headers["User-Agent"] = SOURCE.user_agent
    return s


def fetch_zip(etag: str | None = None) -> tuple[bytes | None, str, str | None]:
    """Conditionally GET the GTFS zip.

    Returns (payload_or_None, etag, last_modified). A 304 returns None for the
    payload, meaning the caller should skip the load entirely.

    Same conditional-GET pattern as producer/fetch.py -- If-None-Match when an
    ETag is known, omitted otherwise. Reuse the reasoning, not the code: this
    polls on a service-change cadence rather than every 10 seconds, so it does
    not need FeedState or staleness tracking.

    The zip is ~11 MB. Set a generous timeout and do not stream it to disk;
    zipfile.ZipFile reads happily from io.BytesIO and the whole thing fits in
    memory many times over.
    """
    session = _session()
    headers = {"If-None-Match": etag} if etag else {}
    try:
        r = session.get(SOURCE.gtfs_zip, headers=headers, timeout=200)
    except requests.RequestException as exc:
        raise FeedError(f"GTFS zip fetch failed: {exc}") from exc

    if r.status_code == 304:
        return None, r.headers.get("ETag") or etag, None
    if r.status_code != 200:
        raise RuntimeError(f"GTFS zip fetch failed: {r.status_code}")

    etag_header = r.headers.get("ETag")
    if not etag_header:
        raise RuntimeError("GTFS zip served without an ETag - cannot version this load")

    return r.content, etag_header, r.headers.get("Last-Modified")



def _rows(reader: csv.DictReader, columns: tuple[str, ...], version_id:int):
    for row in reader:
        yield version_id, *(row.get(c) or None for c in columns)


def load_gtfs(conn: psycopg.Connection, payload: bytes, version_id: int) -> dict[str, int]:
    """Load every table in SOURCE.tables for one version.

    Contract:

      * Iterate SOURCE.tables. For each, read the named columns from the zip
        member and COPY them into static.<table>, prefixing every row with
        version_id.

      * The manifest's `columns` is a SUBSET of what the file publishes. Pull
        by name from the DictReader, never by position -- column order in GTFS
        is not guaranteed and Metro has no obligation to keep it stable.

      * Empty strings become NULL. GTFS represents "no value" as an empty
        field, and '' in a numeric column is a COPY error while '' in a text
        column is a value that breaks `is null`. This is the single most
        common way a GTFS loader half-works.

      * Compare the loaded count against GtfsTable.expect_rows and log a
        warning on an order-of-magnitude miss. Do not fail on a mismatch --
        a service change legitimately moves these numbers -- but 300 trips
        where 31,688 were expected means a truncated download, not a quiet
        Tuesday.

      * Return {table: rowcount}. The caller passes it to mark_ready, so it
        lands in feed_version.row_counts as a permanent record of what that
        version contained.

    Do NOT mark the version ready here. That is the caller's last act.
    """
    zf = zipfile.ZipFile(io.BytesIO(payload))
    names = set(zf.namelist())
    loaded: dict[str, int] = {}
    for table in SOURCE.tables:
        if table.filename not in names:
            if table.required:
                raise ValueError(f'Table {table.filename} not readable from payload')
            else:
                log.warning(f"Table {table.filename} not readable from payload")
                continue
        with conn.cursor() as cursor:
            cursor.execute(f"DELETE FROM static.{table.table} WHERE version_id = %s", (version_id,))
        reader = open_member(zf,table.filename)
        rows = _rows(reader,columns=table.columns,version_id=version_id)
        inserted_count = copy_rows(conn=conn,table=f'static.{table.table}',columns=["version_id",*table.columns],rows=rows)
        count_comparison = inserted_count / table.expect_rows
        if count_comparison <= .1 or count_comparison > 2:
            log.warning(f"Table {table.table} ingested {inserted_count} rows instead of the expected {table.expect_rows} rows")
        loaded[table.table] = inserted_count
    return loaded


def read_feed_info(payload: bytes) -> FeedInfo:
    """feed_info.txt -> FeedInfo. Bytes in, because the CLI hands the payload
    straight through; an absent file is normal, not an error."""
    zf = zipfile.ZipFile(io.BytesIO(payload))
    if FEED_INFO not in zf.namelist():
        return parse_feed_info([])
    return parse_feed_info(open_member(zf, FEED_INFO))


def build_shape_lines(conn: psycopg.Connection, version_id: int) -> int:
    """Assemble static.shapes points into one LINESTRING per shape_id.

    431 shapes from 172,617 points. Pure SQL is the right tool --
    ST_MakeLine over points ordered by shape_pt_sequence, grouped by shape_id.
    Pulling 172k points into Python to build geometries is slower and gains
    nothing.

    Two things to get right:

      * ORDER BY shape_pt_sequence INSIDE the aggregate, not on the outer
        query. `ST_MakeLine(ST_MakePoint(...) ORDER BY shape_pt_sequence)` --
        without the ordered-set syntax the points assemble in whatever order
        the scan returned them, which produces a linestring that zigzags
        across the county and a linear-referencing result that is nonsense
        rather than an error.

      * Record n_points and max_dist(shape_dist_traveled) per shape. The
        enrichment needs max_dist to convert a normalised
        ST_LineLocatePoint result (0..1) back into feed units, and n_points
        is the cheap sanity check that a shape assembled fully.

    Returns the number of shapes built.
    """
    with conn.cursor() as cursor:
        cursor.execute("DELETE FROM static.shape_lines WHERE version_id = %s", (version_id,))
        cursor.execute(
            """
            INSERT INTO static.shape_lines (version_id, shape_id, n_points, max_dist, geom)
            SELECT %s, shape_id,
                count(*)::int,
                max(shape_dist_traveled),
                ST_SetSRID(ST_MakeLine(ST_MakePoint(shape_pt_lon, shape_pt_lat) ORDER BY shape_pt_sequence), 4326)
            FROM static.shapes
            WHERE version_id = %s
            GROUP BY shape_id
            """,
            (version_id,version_id),
        )
        return cursor.rowcount


def load_neighborhoods(conn: psycopg.Connection) -> int:
    """Load the King County neighborhood polygons.

    NOT tied to version_id -- this layer changes on its own rare schedule and
    has nothing to do with GTFS service-change dates. Truncate and replace.

    Contract:

      * GET SOURCE.neighborhoods_count first and assert the GeoJSON returns
        that many features. The service caps at maxRecordCount=1000 and 350
        features fit, but a silent truncation is exactly the failure that
        looks like "some buses have no neighborhood" months later.

      * ST_Multi every geometry. 345 of 350 are Polygon and 5 are
        MultiPolygon; the column is MultiPolygon because PostGIS cannot hold
        both, so normalise on the way in.

      * SRID 4326. The service is asked for outSR=4326, so the coordinates
        arrive in WGS84 -- but ST_GeomFromGeoJSON does not set an SRID, and
        a geometry with SRID 0 will not join against one with 4326. Set it
        explicitly with ST_SetSRID.

    Returns the number of polygons loaded.
    """
    session = _session()
    try:
        r = session.get(SOURCE.neighborhoods_count,timeout=60)
        rf = session.get(SOURCE.neighborhoods_geojson,timeout=90)
    except requests.RequestException as exc:
        raise FeedError(f"neighborhood fetch failed: {exc}") from exc
    if r.status_code != 200:
        raise FeedError(f"neighborhood fetch failed: {r.status_code}")
    if rf.status_code != 200:
        raise FeedError(f"neighborhood fetch failed: {rf.status_code}")
    count = int(r.json()["count"])
    features = rf.json()["features"]

    if len(features) != count:
        raise RuntimeError(f"neighborhood layer truncated: {len(features)} of {count} features")

    with conn.cursor() as cursor:
        cursor.execute("TRUNCATE static.neighborhoods")
        cursor.executemany(
            """
            INSERT INTO static.neighborhoods (objectid, neigh_num, neighborhood_name, geom)
            VALUES (%s, %s, %s, ST_Multi(ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326)))
            """,
            [
                (f["properties"]["OBJECTID"], f["properties"]["NEIGH_NUM"],
                f["properties"]["NEIGHBORHOOD_NAME"], json.dumps(f["geometry"]))
                for f in features
            ],
        )
        return cursor.rowcount