"""Kafka production.

Records are published as JSON initially. That is a deliberate Phase 1 choice,
not the end state: Phase 2 registers protobuf schemas against the Schema
Registry and swaps the serializer, and doing that swap against a topic that
already has history is precisely the schema-evolution exercise the project
exists to demonstrate. Starting with protobuf would skip the migration.

Keys are always plain UTF-8 strings, and stay that way after the Phase 2 swap
-- Kafka partitioning hashes the key bytes, so changing key serialization
would reshuffle every existing key to a different partition and break the
per-vehicle ordering guarantee for the whole history.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, is_dataclass
from datetime import date, datetime
import base64

from confluent_kafka import Producer

from producer.feeds import FeedSpec

log = logging.getLogger("producer.publish")


def _json_default(value):
    """datetime/date -> ISO 8601, for json.dumps."""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    raise TypeError(f"not JSON serialisable: {type(value).__name__}")


def serialize(record) -> bytes:
    """Record -> JSON bytes.

    Phase 2 replaces the body of this function with a ProtobufSerializer and
    a registered schema. It is a separate function for exactly that reason --
    the swap should touch one place.
    """
    payload = asdict(record) if is_dataclass(record) else dict(record)
    return json.dumps(payload, default=_json_default, separators=(",", ":")).encode()


class TopicPublisher:
    """Produces records to their feed's topic, keyed per ADR 0002."""

    def __init__(self, bootstrap: str, flush_timeout_s: float = 30.0) -> None:
        self.flush_timeout_s = flush_timeout_s
        self.delivered = 0
        self.failed: list[str] = []

        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap,
                # Retries plus idempotence. At-least-once with idempotent
                # writes is the delivery model (ADR 0004): the sink's primary
                # key absorbs duplicates, so retrying is safe and dropping is
                # not.
                "enable.idempotence": True,
                "acks": "all",
                # Batch a little. At ~280 positions or ~18k trip updates per
                # tick, batching is the difference between one request and
                # thousands. 20ms is well under the 10s poll interval, so it
                # costs nothing in latency terms here.
                "linger.ms": 20,
                "compression.type": "snappy",
                # Fail loudly rather than blocking forever if the broker is
                # gone -- a producer wedged on a full queue looks identical to
                # a producer that is merely quiet.
                "queue.buffering.max.messages": 200_000,
                "message.timeout.ms": 60_000,
            }
        )

    def _on_delivery(self, err, msg) -> None:
        """Delivery callback.

        Fires on the producer's thread during poll()/flush(). Keep it cheap
        and do not raise from here -- an exception in a delivery callback is
        swallowed by librdkafka and you will never see it.
        """
        if err is not None:
            self.failed.append(str(err))
            log.error("delivery failed: %s", err)
        else:
            self.delivered += 1

    def publish(self, spec: FeedSpec, records: list) -> int:
        """Produce records to spec.topic. Returns the number enqueued.

        Keys are record.key, UTF-8 encoded, and never null: raw.service_alerts
        is log-compacted and a null key cannot be compacted, while the other
        topics depend on the key for per-key ordering.

        librdkafka services delivery callbacks only when the client is
        polled, and produce() alone does not service it -- hence poll(0) on
        every iteration. Omit it and callbacks queue up unbounded,
        `delivered` stays 0 until the next flush, and a BufferError arrives
        with no explanation.

        BufferError means the local queue is full: drain with poll(1.0),
        retry once, and on a second failure record the failure and give up
        on that record -- never a silent drop.

        The return is the count enqueued, NOT delivered. Delivery is
        asynchronous; `delivered` is only meaningful after flush().

        Topics are not created here -- `make topics` owns that, because
        auto-created topics get broker defaults and the alerts topic would
        silently lose its compaction policy.
        """
        enqueued = 0
        for record in records:
            try:
                self._producer.produce(spec.topic, key=record.key.encode("utf-8"),
                                       value=serialize(record), on_delivery=self._on_delivery)
                enqueued += 1
            except BufferError:
                # Local queue full: service the client so delivery callbacks
                # can free slots, then retry exactly once.
                self._producer.poll(1.0)
                try:
                    self._producer.produce(spec.topic, key=record.key.encode("utf-8"),
                                           value=serialize(record), on_delivery=self._on_delivery)
                    enqueued += 1
                except BufferError:
                    self.failed.append(f"[{spec.name}] queue full, dropped key={record.key}")
                    log.error("[%s] queue full, dropped key=%s", spec.name, record.key)
            self._producer.poll(0)
        return enqueued

    # ------------------------------------------------------------------------

    def flush(self) -> int:
        """Block until the queue drains.

        Returns the number of messages still undelivered -- nonzero means the
        broker did not accept everything within the timeout, which the caller
        should treat as a failed tick rather than a warning.
        """
        remaining = self._producer.flush(self.flush_timeout_s)
        if remaining:
            log.error("%d message(s) undelivered after flush", remaining)
        return remaining

    def publish_dlq(
        self,
        topic: str,
        reason: str,
        payload: bytes,
        detail: str = "",
        entity_key: str | None = None,
    ) -> None:
        """Route a failed payload to a DLQ topic with its reason.

        The DLQ wire format is ours to define until the Connect sink exists:
        a JSON envelope mirroring the raw.dlq columns -- reason, optional
        detail and entity key, and the original bytes base64. The bytes are
        kept verbatim: they are the evidence for the bug that caused the
        route, and the replay demo re-runs them through a fixed decoder.
        """
        envelope: dict = {"reason": str(reason)}
        if detail:
            envelope["detail"] = detail
        if entity_key is not None:
            envelope["entity_key"] = entity_key
        envelope["payload_b64"] = base64.b64encode(payload).decode("ascii")
        self._producer.produce(
            topic,
            key=(entity_key or reason).encode("utf-8"),
            value=json.dumps(envelope, separators=(",", ":")).encode(),
            on_delivery=self._on_delivery,
        )
        self._producer.poll(0)