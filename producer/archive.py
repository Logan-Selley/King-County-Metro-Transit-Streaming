"""Raw payload archive in MinIO.

The archive stores the bytes exactly as fetched, before any parsing. That
ordering is the whole point and it is easy to get backwards.

Store decoded records and you have a log of what your current decoder
believed. Store raw bytes and you can re-run a *changed* decoder over real
history -- which is the Phase 6 replay demo, and the difference between "I ran
Kafka" and "I designed around Kafka". Every bug you fix in decode.py is
retroactively fixable against the archive; none of them are if you archive
after decoding.

It is also the insurance policy. The Terms of Use explicitly reserve King
County's right to modify or discontinue access without notice. If the feed
disappears mid-project, the archive is the project.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from io import BytesIO

from minio import Minio
from minio.error import S3Error

from producer.feeds import FeedSpec

log = logging.getLogger("producer.archive")


@dataclass(frozen=True)
class ArchivedObject:
    bucket: str
    key: str
    size: int

    def __str__(self) -> str:
        return f"s3://{self.bucket}/{self.key} ({self.size:,} b)"


class RawArchive:
    """Writes fetched payloads to object storage, unmodified.

    Used as a context manager for symmetry with the rest of the pipeline, but
    it holds no resources that need releasing -- the MinIO client is stateless
    HTTP. The __exit__ exists so callers do not have to remember which
    components need closing and which do not.
    """

    def __init__(
        self,
        endpoint: str | None = None,
        access_key: str | None = None,
        secret_key: str | None = None,
        bucket: str | None = None,
        secure: bool = False,
    ) -> None:
        self.bucket = bucket or os.environ.get("RAW_BUCKET", "transit-raw")
        self._client = Minio(
            endpoint or os.environ.get("MINIO_ENDPOINT", "localhost:9000"),
            # archive_writer's credentials (build step 5C), not the MinIO root.
            # Its policy allows ListBucket on the bucket and PutObject under
            # raw/, so the producer cannot read an object back, let alone touch
            # flink-checkpoints/.
            access_key=access_key or os.environ["MINIO_ACCESS_KEY"],
            secret_key=secret_key or os.environ["MINIO_SECRET_KEY"],
            # Plain HTTP: MinIO here is a local stand-in for S3 on a private
            # Docker network. Anything reachable from outside the host needs
            # secure=True and a real certificate.
            secure=secure,
        )
        self.written = 0
        self.bytes_written = 0

    def __enter__(self) -> RawArchive:
        self.ensure_bucket()
        return self

    def __exit__(self, *exc_info) -> None:
        if self.written:
            log.info(
                "archive: %d object(s), %s",
                self.written,
                _human(self.bytes_written),
            )

    def ensure_bucket(self) -> None:
        """Create the bucket if absent.

        Normally a no-op -- the minio-init container in docker-compose.yml
        creates it at stack start. Kept because a producer run against a fresh
        volume otherwise fails on the first put with a NoSuchBucket that reads
        like a credentials problem.
        """
        if not self._client.bucket_exists(self.bucket):
            self._client.make_bucket(self.bucket)
            log.info("created bucket %s", self.bucket)

    def key_for(self, spec: FeedSpec, fetched_at: datetime, etag: str | None) -> str:
        """Object key for one payload.

        Hour-level prefixes: a day of vehicle positions at a 20s publish rate
        is ~4,300 objects, and a flat prefix makes listing unusable long
        before that. Hour buckets also make "replay 08:00-09:00 on the 4th" a
        prefix scan instead of a full listing.

        The ETag goes in the name because it is the feed's own content
        identity -- two objects with the same ETag are the same bytes, which
        makes the archive self-verifying and makes an accidental double-write
        idempotent rather than a duplicate.
        """
        stamp = fetched_at.astimezone(timezone.utc)
        # ETags arrive quoted and may be weak (W/"abc"); both forms break
        # nothing in a key but make prefixes ugly and grep unreliable.
        tag = (etag or "no-etag").strip('"').removeprefix("W/").strip('"')
        return (
            f"{spec.archive_prefix}/"
            f"{stamp:%Y/%m/%d/%H}/"
            f"{int(stamp.timestamp())}-{tag}.{spec.extension}"
        )

    def put(
        self,
        spec: FeedSpec,
        payload: bytes,
        fetched_at: datetime,
        etag: str | None = None,
        last_modified: str | None = None,
    ) -> ArchivedObject:
        """Store one payload verbatim. Returns where it landed.

        Raises S3Error on failure, deliberately: a fetch that cannot be
        archived must not be silently published, because it would be a record
        that exists in Kafka with no replayable origin. The caller decides
        whether that is fatal for the tick.
        """
        key = self.key_for(spec, fetched_at, etag)

        # Round-trips the feed's own metadata so the archive is interpretable
        # without the database. x-amz-meta-* is the only place this survives.
        metadata = {
            "feed": spec.name,
            "fetched-at": fetched_at.astimezone(timezone.utc).isoformat(),
        }
        if etag:
            metadata["source-etag"] = etag.strip('"')
        if last_modified:
            metadata["source-last-modified"] = last_modified

        self._client.put_object(
            bucket_name=self.bucket,
            object_name=key,
            data=BytesIO(payload),
            length=len(payload),
            content_type=(
                "application/x-protobuf"
                if spec.wire_format == "protobuf"
                else "application/json"
            ),
            metadata=metadata,
        )

        self.written += 1
        self.bytes_written += len(payload)
        log.debug("archived %s", key)
        return ArchivedObject(self.bucket, key, len(payload))

    def iter_keys(self, spec: FeedSpec, prefix: str = ""):
        """List archived objects for a feed, oldest first.

        This is the read side of the replay demo: Phase 6 walks these keys and
        feeds the bytes back through a changed decoder. `prefix` narrows to a
        time range, e.g. "2026/09/04/08".
        """
        full = f"{spec.archive_prefix}/{prefix}" if prefix else spec.archive_prefix
        try:
            objects = self._client.list_objects(self.bucket, prefix=full, recursive=True)
            yield from sorted((o.object_name for o in objects))
        except S3Error as exc:
            raise RuntimeError(f"listing {full} failed: {exc}") from exc

    def get(self, key: str) -> bytes:
        """Fetch one archived payload back. The replay read path."""
        response = None
        try:
            response = self._client.get_object(self.bucket, key)
            return response.read()
        finally:
            if response is not None:
                response.close()
                response.release_conn()


def _human(n: int) -> str:
    for unit in ("b", "KB", "MB", "GB"):
        if n < 1024 or unit == "GB":
            return f"{n:,.0f} {unit}" if unit == "b" else f"{n:.1f} {unit}"
        n /= 1024.0
    return f"{n:.1f} GB"
