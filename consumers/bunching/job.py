"""Flink job: windowed bunching detection over enriched.vehicle_positions.

>>> YOU IMPLEMENT: parse_record(), assign_route_key(), detect_in_window(),
                   BunchingState.emit()
    The environment setup, Kafka wiring and watermark strategy are given.

Submitted to the cluster, NOT run from the project venv:

    make flink-up            # build the image, start JobManager + TaskManager
    make bunching            # submit this job
    make flink-ui            # http://localhost:8086

ADR 0006 explains why this cannot share the project virtualenv: apache-flink
pins protobuf<6 through apache-beam, the project runs 7.36.1, and the
downgrade is silent -- `uv add apache-flink` resolves fine and then
recon/probe.py dies on `field.is_repeated`. Nothing here imports producer/ or
consumers.enrichment; the interface is Kafka on both sides.

--- why event time, and what the watermark is actually for ---

This is the first thing in the project that cannot be done correctly in
processing time.

Vehicles go through tunnels and dead zones and then report a burst of stale
positions -- the proposal flagged it in section 5, and Phase 1 measured gaps
up to 65s between archived payloads across 24 hours. If the detector windows
on arrival time, a bus that was quiet for two minutes and then dumps six
positions looks like six vehicles in one window, all bunched with each other.

Windowing on `position_timestamp` (when the GPS fix happened) rather than
arrival puts those six back where they belong. The watermark is what lets the
window know it can close: "no more records older than T are expected." Records
later than the allowed lateness are dropped from the window rather than
reopening it, which is a deliberate accuracy-for-latency trade and belongs in
the writeup.
"""

from __future__ import annotations

import json
import logging
import os

from pyflink.common import Duration, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)

from consumers.bunching.config import CONFIG, CONSUMER_GROUP, SINK_TOPIC, SOURCE_TOPIC

log = logging.getLogger("bunching")

# redpanda:9092, not localhost:19092 -- the job runs INSIDE the compose
# network, so it uses the internal listener. Using the external one here is
# the single most common way a Flink job hangs at startup with no error: it
# resolves, connects to nothing, and reports no partitions assigned.
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_INTERNAL", "redpanda:9092")


# --- given -------------------------------------------------------------------


def build_env() -> StreamExecutionEnvironment:
    """Execution environment with event time and checkpointing. GIVEN."""
    env = StreamExecutionEnvironment.get_execution_environment()
    # Checkpointing is what makes keyed state survive a TaskManager restart.
    # Without it a crash loses every vehicle's last-known position and the
    # detector silently under-reports until state refills.
    env.enable_checkpointing(30_000)
    env.set_parallelism(2)
    return env


def watermark_strategy() -> WatermarkStrategy:
    """Bounded out-of-orderness on event time. GIVEN.

    `allowed_lateness_s` (120s) is sized from measurement, not taste: Phase 1
    saw a 65s maximum inter-payload gap, and stale bursts after a tunnel run
    to minutes. Too tight and real positions get dropped; too loose and every
    window waits on a straggler that may never come.

    The timestamp assigner reads `position_timestamp` -- the GPS fix time --
    which is the whole point. See the module docstring.
    """
    return (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(CONFIG.allowed_lateness_s))
        .with_timestamp_assigner(_PositionTimestampAssigner())
    )


def kafka_source() -> KafkaSource:
    """Source over enriched.vehicle_positions. GIVEN.

    `earliest` rather than `latest`: a bunching job restarted mid-day should
    rebuild its picture from recent history rather than start blind. The
    consumer group means it resumes from committed offsets in practice, and
    only falls back to earliest on a genuinely new group.
    """
    return (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_topics(SOURCE_TOPIC)
        .set_group_id(CONSUMER_GROUP)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )


def kafka_sink() -> KafkaSink:
    """Sink to alerts.bunching. GIVEN.

    Keyed by route so alerts for one route stay ordered, and so a downstream
    compacted view could keep the latest state per route if one is ever
    wanted. `make topics` owns the topic's config; this only writes to it.
    """
    return (
        KafkaSink.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(SINK_TOPIC)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )


class _PositionTimestampAssigner:
    """Extracts event time from a record. GIVEN.

    Flink wants milliseconds since epoch. `position_timestamp` arrives as an
    ISO-8601 string after the JSON round-trip through publish.serialize(), so
    it is parsed rather than cast -- the same trap enrich_v2 hits, and the
    same fix.
    """

    def extract_timestamp(self, value, record_timestamp: int) -> int:
        from datetime import datetime

        try:
            rec = json.loads(value)
            ts = rec.get("position_timestamp")
            if isinstance(ts, (int, float)):
                return int(ts * 1000)
            return int(datetime.fromisoformat(ts).timestamp() * 1000)
        except Exception:  # noqa: BLE001 -- a bad record must not stall the watermark
            return record_timestamp


# --- stubs ------------------------------------------------------------- >>> TODO


def parse_record(value: str) -> dict | None:
    """Enriched JSON -> the fields the detector needs.

    >>> IMPLEMENT THIS.

    Keep only what the detector uses: vehicle_id, route_id, direction_id,
    trip_id, shape_dist_traveled, position_timestamp, route_short_name,
    schedule_deviation_seconds. Dropping the rest early matters more here than
    in the enrichment consumer, because everything retained crosses the
    Python/JVM boundary on every record.

    Return None for a record the detector cannot use, and let the caller
    filter: no shape_dist_traveled (the vehicle could not be located on its
    shape) or no route_id. Do not raise -- one malformed record must not fail
    the job, and an exception here takes down the whole TaskManager slot.
    """
    raise NotImplementedError("consumers.bunching.job.parse_record")


def assign_route_key(rec: dict) -> str:
    """Partition key for the detector.

    >>> IMPLEMENT THIS.

    (route_id, direction_id) -- NOT route_id alone. The two directions of a
    route run on different shapes, so their shape_dist_traveled values are not
    comparable, and a northbound bus at 12,000 ft is not "near" a southbound
    one at 12,100 ft. Keying on route alone produces confident nonsense at
    every terminus.

    Note this is the re-keying that ADR 0002 predicted would be necessary:
    the topic is partitioned by vehicle_id for per-vehicle ordering, so
    route-level analysis has to shuffle. That cost was accepted knowingly and
    this is where it lands.
    """
    raise NotImplementedError("consumers.bunching.job.assign_route_key")


def detect_in_window(key: str, records: list[dict]) -> list[dict]:
    """Find bunched pairs among one route-direction's positions in one window.

    >>> IMPLEMENT THIS. This is the core of Phase 3's first deliverable.

    Given every position for one (route, direction) inside a 60s event-time
    window, emit an alert dict per bunched PAIR.

    Shape:

      * Reduce to one position per vehicle -- the latest by
        position_timestamp. A 60s window holds ~3 observations per vehicle at
        the measured 20s publish rate, and comparing all of them against each
        other would count the same pair three times.

      * Drop vehicles whose position is older than CONFIG.max_position_age_s
        relative to the window end. A stale-burst position measures where a
        bus WAS; pairing it against a fresh one invents a gap that closed
        minutes ago.

      * Sort by shape_dist_traveled and compare CONSECUTIVE vehicles only.
        All-pairs is O(n^2) and wrong besides: three buses in a row are two
        bunched pairs, not three.

      * A gap below CONFIG.gap_threshold_ft is a candidate.

      * Emit: route_id, direction_id, route_short_name, both vehicle_ids and
        trip_ids, gap_ft, window_end, and both schedule_deviation_seconds --
        the deviations are what make an alert interpretable, because bunching
        with one bus 8 minutes late is a different story from two buses both
        on time.

    UNITS: shape_dist_traveled is in feed units, which is FEET for 423 of 424
    shapes. Both vehicles in a pair share a shape, so the comparison is
    internally consistent regardless -- see the note in config.py. Do not
    convert to metres and do not compare across routes.

    Return [] rather than None when nothing is bunched.
    """
    raise NotImplementedError("consumers.bunching.job.detect_in_window")


# --- wiring -------------------------------------------------------- >>> TODO


def build_pipeline(env: StreamExecutionEnvironment) -> None:
    """Assemble source -> parse -> key -> window -> detect -> sink.

    >>> IMPLEMENT THIS once the functions above are done.

    Sketch, in PyFlink terms:

        stream = env.from_source(kafka_source(), watermark_strategy(),
                                 "enriched-positions")
        parsed = (stream.map(parse_record, output_type=Types.MAP(...))
                        .filter(lambda r: r is not None))
        alerts = (parsed.key_by(assign_route_key)
                        .window(TumblingEventTimeWindows.of(
                            Time.seconds(CONFIG.window_s)))
                        .process(BunchingWindowFunction()))
        alerts.sink_to(kafka_sink())

    Two things that will bite:

      * Types. PyFlink needs explicit output types on map/process; a Python
        dict crossing the boundary without one produces a pickled blob that
        the sink serialises as gibberish rather than failing.

      * CONFIG.min_consecutive_windows and CONFIG.cooldown_s are NOT window
        logic -- they are state that spans windows. They belong in a
        KeyedProcessFunction with a ValueState per pair, downstream of the
        window. Trying to express them inside a single window's process()
        cannot work, because one window has no memory of the last.
    """
    raise NotImplementedError("consumers.bunching.job.build_pipeline")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(message)s")
    env = build_env()
    build_pipeline(env)
    env.execute("bunching-detector")


if __name__ == "__main__":
    main()
