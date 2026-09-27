"""Flink job: windowed bunching detection over enriched.vehicle_positions.

WIRING ONLY. The detection logic lives in consumers/bunching/detect.py, which
imports no pyflink and is therefore testable from the project venv against
tests/test_bunching_contract.py. That includes BunchingState, the cross-window
cooldown: it is a dataclass in detect.py with no Flink in it. This file holds
the environment, the Kafka wiring, and the ValueState round-trip; the detection
logic is not here.

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

Processing time is not correct here.

Vehicles go through tunnels and dead zones and then report a burst of stale
positions -- the proposal flagged it in section 5, and the feed shows gaps
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

import logging
import os
from functools import partial

from pyflink.common import Configuration, Duration, Types, WatermarkStrategy
from pyflink.common.serialization import ByteArraySchema
from pyflink.common.time import Time
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetResetStrategy,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.functions import KeyedProcessFunction, ProcessWindowFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.window import TumblingEventTimeWindows

from consumers import framing
from consumers.checkpointing import configure_checkpoints
from consumers.bunching.config import CONFIG, run_settings
from consumers.bunching.decode import decode
from consumers.bunching.detect import (
    BunchingState,
    assign_route_key,
    detect_in_window,
    pair_key,
    parse_record,
)

log = logging.getLogger("bunching")

# redpanda:9092, not localhost:19092 -- the job runs INSIDE the compose
# network, so it uses the internal listener. Using the external one here is
# the single most common way a Flink job hangs at startup with no error: it
# resolves, connects to nothing, and reports no partitions assigned.
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_INTERNAL", "redpanda:9092")

# WHERE THIS RUN READS AND WRITES, resolved once at startup. Every topic, group
# and gate below comes from here rather than from the constants, so a replay
# cannot half-apply: reading the replay topic while writing the live one is the
# failure this indirection exists to make impossible. With no replay variables
# set, run_settings returns exactly the live wiring, which is the case that must
# not change (tests/test_replay_contract.py pins both).
SETTINGS = run_settings(os.environ)
log.info("run %s: %s -> %s (group %s, bounded=%s, gate=%s)",
         SETTINGS.job_name, SETTINGS.source_topic, SETTINGS.sink_topic,
         SETTINGS.group, SETTINGS.bounded, SETTINGS.min_stop_sequence)

# Where the job reads its sink schema's id from, at submit time. Defaults to the
# compose service name for the same reason BOOTSTRAP does: .env carries the
# HOST-facing address (localhost:18081) and a container cannot reach that.
SCHEMA_REGISTRY = os.environ.get("SCHEMA_REGISTRY_INTERNAL", "http://redpanda:8081")

RECORD_TYPE = Types.MAP(Types.STRING(), Types.PICKLED_BYTE_ARRAY())


# --- environment, sources and sink -------------------------------------------


def build_env() -> StreamExecutionEnvironment:
    """Execution environment: checkpointed for the live job, fail-fast for a replay."""
    if SETTINGS.bounded:
        # A REPLAY FAILS FAST: no checkpoints, no restarts. Measured
        # 2026-09-27: the baseline replay of 09-24 Pacific died 7 minutes in,
        # when the Python worker raised "A serializer has already been
        # registered for the state; re-registration is not allowed" from
        # StateSerializerProvider.registerNewSerializerForRestoredState, which
        # is the restore path. Under Flink's default restart-on-failure the job
        # neither recovered nor failed, and the container sat for 5 h 22 min
        # (the same job on the UTC day finished in 12 minutes). A bounded
        # replay has nothing worth restoring, since rerunning it from the start
        # gives the same output, so a failure now ends the job, execute()
        # raises, and `make replay-detect` exits nonzero.
        conf = Configuration()
        conf.set_string("restart-strategy.type", "none")
        env = StreamExecutionEnvironment.get_execution_environment(conf)
    else:
        env = StreamExecutionEnvironment.get_execution_environment()
        # Checkpointing is what makes keyed state survive a TaskManager
        # restart. Without it a crash loses every vehicle's last-known position
        # and the detector silently under-reports until state refills.
        # Interval, tolerated failures and retention: consumers/checkpointing.py,
        # which also records why none of them can live in the cluster config.
        configure_checkpoints(env)
    # 3, matching the partition count of enriched.vehicle_positions, and this
    # is a CORRECTNESS setting rather than a throughput one.
    #
    # PyFlink cannot assign watermarks at the source (see watermark_strategy),
    # so each subtask computes one watermark over every partition it was
    # given. At parallelism 2 one subtask reads two partitions, interleaves
    # them, and cannot tell a partition lagging from a record arriving late:
    # measured, 29.75% of records dropped as late over an evening. Give each
    # subtask exactly one partition and its single watermark IS a per-
    # partition watermark, which is the property the source placement would
    # have provided.
    #
    # map and filter preserve partitioning, so the assigner downstream of
    # them still sees one partition per subtask. A key_by after that point
    # shuffles, which is fine -- the watermark is already correct by then.
    #
    # Raising the partition count means raising this to match.
    env.set_parallelism(3)
    return env


def watermark_strategy() -> WatermarkStrategy:
    """Bounded out-of-orderness on event time.

    `allowed_lateness_s` is sized from measurement, not taste: stale bursts
    after a tunnel run to minutes, and the event-time skew of reading several
    partitions through one watermark dominates it (CONFIG.allowed_lateness_s
    carries the distribution). Too tight and real positions get dropped; too
    loose and every window waits on a straggler that may never come.

    The timestamp assigner reads `position_timestamp` -- the GPS fix time --
    which is the whole point. See the module docstring.

    APPLIED DOWNSTREAM OF THE PYTHON MAPS, and not as a preference.

    The textbook placement is on the source, where Flink keeps a watermark
    PER KAFKA PARTITION and emits the minimum, so a partition being consumed
    slowly holds time back instead of having its records declared late.
    That placement does not work in PyFlink, and the failure is silent.

    Measured with the strategy passed to `from_source`:

        rec_ts=1789960793771  wm=1789960673416  pos=1789959734
        rec_ts=1789960793771  wm=1789960673416  pos=1789959748
        rec_ts=1789960793771  wm=1789960673416  pos=1789959741

    `pos` is each record's own position_timestamp and varies; `rec_ts` is
    what Flink windows on and is IDENTICAL for every record. A Python map
    operator does not carry the input record's timestamp to its output, so a
    timestamp assigned before `.map(decode)` is gone by the time the window
    sees it. Every record then lands in one window whose end sits just past
    the final watermark, and it never fires: the job runs, reads all 46,286
    records, drops nothing, and emits nothing. Zero late drops is what it
    looks like when no record has a real timestamp at all.

    So the assigner has to go after the last Python operator, which means
    giving up per-split watermarking. That cost is permanent.

    Measured over an evening of steady state, the late-drop rate is not a
    catch-up artifact:

        649,949 records into the window
        193,364 dropped as late                     29.75%

    Three partitions do not advance in lockstep just because the job has
    caught up, because the enrichment consumer fills them in bursts. The
    disorder is still not in the DATA -- within any single partition the
    same records never exceed 111s -- but the interleaving the job sees is
    real and permanent, so the bound has to cover it.

    CONFIG.allowed_lateness_s carries the distribution it was sized from.
    A watermark bound has to be measured where the watermark is computed,
    not where the data is produced.
    """
    return (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(CONFIG.allowed_lateness_s))
        .with_timestamp_assigner(_PositionTimestampAssigner())
    )


def kafka_source() -> KafkaSource:
    """Source over enriched.vehicle_positions.

    COMMITTED OFFSETS, falling back to earliest only on a genuinely new group.
    The call is deliberately not `.earliest()`, which IGNORES the group's
    committed offsets: every boot would replay the topic's whole 7-day
    retention into alerts.bunching (measured: four replays in a single day).
    The warehouse's upsert collapses the duplicates that reach it, but the
    topic keeps every one of them, and each boot pays for a full replay.

    The restart trade is unchanged and still deliberate: a group with no commits
    starts at earliest, and the per-pair window state refills within minutes, so
    a cold start is a few minutes of extra alerts rather than a gap. See
    docker/flink-submit.sh, which explains why checkpoint restore is left manual.

    BYTES, not SimpleStringSchema. The topic carries Confluent-framed
    protobuf. SimpleStringSchema does not fail on protobuf -- Java's
    `new String(bytes, charset)` substitutes U+FFFD for anything undecodable
    -- so the wrong deserializer produces a healthy job emitting garbage.
    decode.py turns the bytes into a dict; see its docstring for why it does
    not use ProtobufDeserializer.
    """
    builder = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_topics(SETTINGS.source_topic)
        .set_group_id(SETTINGS.group)
        .set_starting_offsets(
            KafkaOffsetsInitializer.committed_offsets(
                KafkaOffsetResetStrategy.EARLIEST))
        # Offsets are committed only for the live job, which resumes from them.
        # A replay has nothing to resume, and its group would be one more
        # abandoned group in `rpk group list` (the lag-check finding).
        .set_property("commit.offsets.on.checkpoint",
                      "true" if SETTINGS.commit_offsets else "false")
        .set_value_only_deserializer(ByteArraySchema())
    )
    # BOUNDED IS AN OFFSETS INITIALIZER, NOT A FLAG. set_bounded takes a
    # KafkaOffsetsInitializer, the offsets to STOP at, and latest() means "stop
    # at the offsets current when the job started". A replay reads to there and
    # ENDS, which is what makes the final watermark fire and the last event-time
    # windows close. The live job must never finish, so it stays
    # CONTINUOUS_UNBOUNDED and nothing is set for it.
    #
    # Passing a bool here raises AttributeError on `_j_initializer`, and the
    # live job passing False would stop bunching alerts on the next JobManager
    # restart, `make resume` or reboot. The contract suite cannot catch it:
    # PyFlink is not in the venv the tests run in, so nothing there ever builds
    # a source. `make bunching-source-check` builds both in the image that runs
    # them.
    if SETTINGS.bounded:
        builder = builder.set_bounded(KafkaOffsetsInitializer.latest())
    return builder.build()


def kafka_sink() -> KafkaSink:
    """Sink to alerts.bunching.

    Keyed by route so alerts for one route stay ordered, and so a downstream
    compacted view could keep the latest state per route if one is ever
    wanted. `make topics` owns the topic's config; this only writes to it.

    BYTES, not SimpleStringSchema. The records carry Confluent framing
    (consumers/framing.py) so the JDBC sink can read them, and a
    framed record is bytes rather than text: the first five are a magic byte and
    a schema id, which Java's String(bytes, charset) would replace with U+FFFD.
    The same trap the SOURCE docstring describes, pointing the other way.
    """
    return (
        KafkaSink.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(SETTINGS.sink_topic)
            .set_value_serialization_schema(ByteArraySchema())
            .build()
        )
        .build()
    )


class _PositionTimestampAssigner(TimestampAssigner):
    """Extracts event time from a record.

    Flink wants milliseconds since epoch. `position_timestamp` is `int64` in
    the .proto and decode.to_dict() keeps it an int, so this multiplies
    rather than parses.

    That is worth stating because the obvious alternative is wrong in a quiet
    way: json_format.MessageToDict renders int64 as a STRING (JSON numbers
    cannot hold the range), so a decoder built on it would hand this method
    "1789935300" and `* 1000` would produce a thousand-fold repeated string.
    decode.to_dict() uses explicit field access for exactly this reason.

    Falls back to the Kafka record timestamp rather than raising. A record
    that cannot yield an event time must not stall the watermark, and the
    broker's own timestamp is close enough for one record.
    """

    def extract_timestamp(self, value, record_timestamp: int) -> int:
        # A dict: this runs after decode and parse_record, never on raw bytes.
        # The isinstance guard is the cheap way to fail soft rather than
        # AttributeError inside an operator.
        rec = value if isinstance(value, dict) else None
        ts = rec.get("position_timestamp") if rec else None
        if isinstance(ts, (int, float)):
            return int(ts * 1000)
        return record_timestamp


# --- wiring ------------------------------------------------------------------


class BunchingWindowFunction(ProcessWindowFunction):
    """One window of one (route, direction) -> that window's candidate alerts.

    Thin on purpose. The pairing logic is detect_in_window, which is pure and
    tested; the only thing this owns is the unit conversion.
    """

    def process(self, key, context, elements):
        # context.window().end is epoch MILLISECONDS while position_timestamp
        # and window_end_s are seconds. Passed unconverted, every record looks
        # ancient to the stale gate and the window emits nothing.
        # int() of the millisecond window end. Epoch seconds as an INTEGER,
        # because the sink's TimestampConverter refuses FLOAT64 (measured: the
        # JDBC task died on "Schema Schema{FLOAT64} does not correspond to a
        # known timestamp type format" without writing a row). This value is
        # also what every alert carries as window_end, so the coercion is
        # stated once, here.
        # The gate is the RUN's, not the module's: with no replay variables set
        # this is the live MIN_STOP_SEQUENCE, and a variant passes its own, so
        # the two replayed detectors differ in that one value and nothing else
        # (consumers/bunching/config.py's run_settings).
        yield from detect_in_window(key, list(elements),
                                    int(context.window().end / 1000),
                                    min_stop_sequence=SETTINGS.min_stop_sequence)


class BunchingCooldown(KeyedProcessFunction):
    """CONFIG.cooldown_s and CONFIG.min_consecutive_windows, across windows.

    Keyed by vehicle PAIR, so one state entry per pair rather than per route.
    The decision is detect.BunchingState.emit; this class only carries the state
    in and out of Flink, which is the part that needs a cluster.

    A second re-keying: the window stream is keyed by (route, direction) and
    this splits it by pair, so alerts for one route no longer share a subtask.
    alerts.bunching has one partition, so ordering holds regardless -- the
    shuffle is the cost, not the correctness.
    """

    def open(self, ctx):
        self.state = ctx.get_state(
            ValueStateDescriptor("bunching", Types.PICKLED_BYTE_ARRAY()))

    def process_element(self, alert, ctx):
        # alert["window_end"] rather than ctx.timestamp(): the same epoch
        # seconds detect_in_window used, so no second unit conversion here.
        emit, updated = BunchingState.from_dict(self.state.value()).emit(alert["window_end"])
        self.state.update(updated.to_dict())
        if emit:
            yield alert


def build_pipeline(env: StreamExecutionEnvironment) -> None:
    """Assemble source -> decode -> parse -> key -> window -> detect -> cooldown -> sink.

    Six stages in two groups. The first is stateless and event-time: decode the
    topic's protobuf bytes, narrow to the detector's ten fields, stamp event
    time from position_timestamp, and window each (route, direction) into
    CONFIG.window_s buckets. The second is stateful: BunchingWindowFunction
    turns one window into that window's candidate alerts, and BunchingCooldown
    holds the per-pair memory that decides which of them are worth sending.

    Three things that bite here, all of them silently:

      * Types. PyFlink needs an explicit output type wherever a Python object
        crosses the boundary; without one it pickles the object, which works
        between Python operators and then fails at the sink, because
        SimpleStringSchema is a Java serializer expecting a string. Hence
        RECORD_TYPE on every map/process and the framing stage at the end.

      * The watermark goes on the DECODED stream, because a Python map does
        not carry a record's timestamp to its output. Assigned on the source
        it is lost before the window, every record gets one identical
        timestamp, and the job reads everything and emits nothing. The cost
        of the working placement is late drops during replay; both numbers
        are in watermark_strategy's docstring.

      * context.window().end is epoch MILLISECONDS while position_timestamp and
        window_end_s are seconds. BunchingWindowFunction divides by 1000; get it
        wrong and the stale gate compares 1.79e12 against 1.79e9, every record
        looks ancient, and a healthy-looking job emits nothing.

    CONFIG.min_consecutive_windows and CONFIG.cooldown_s are NOT window logic.
    One window has no memory of the last, so they cannot be expressed inside
    process() at all: they live in BunchingCooldown's ValueState, one entry per
    vehicle pair, with the decision itself in detect.BunchingState.emit.
    """
    records = (
        env.from_source(kafka_source(), WatermarkStrategy.no_watermarks(),
                        "enriched-positions")
        .map(decode, output_type=RECORD_TYPE)
        .filter(lambda rec: rec is not None)
        .map(parse_record, output_type=RECORD_TYPE)
        .filter(lambda rec: rec is not None)
        # AFTER the maps, not on the source. A Python map drops the record
        # timestamp, so assigning it earlier gives every record the same one
        # and no window ever fires. See watermark_strategy.
        .assign_timestamps_and_watermarks(watermark_strategy())
    )
    alerts = (
        records.key_by(assign_route_key)
        .window(TumblingEventTimeWindows.of(Time.seconds(CONFIG.window_s)))
        .process(BunchingWindowFunction(), output_type=RECORD_TYPE)
        .key_by(pair_key)
        .process(BunchingCooldown(), output_type=RECORD_TYPE)
    )
    # The sink schema's id, resolved ONCE here rather than per record. See
    # consumers/framing.py for why, and for what a re-registration later does
    # to a job that is already running.
    sid = framing.schema_id(SCHEMA_REGISTRY, SETTINGS.sink_subject)
    log.info("sink schema %s -> id %s", SETTINGS.sink_subject, sid)

    alerts.map(partial(framing.frame, sid=sid),
               output_type=Types.PRIMITIVE_ARRAY(Types.BYTE())).sink_to(kafka_sink())


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(message)s")
    env = build_env()
    build_pipeline(env)
    env.execute(SETTINGS.job_name)


if __name__ == "__main__":
    main()
