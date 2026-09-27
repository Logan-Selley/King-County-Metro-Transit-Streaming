"""Flink job: prediction accuracy, joining trip updates to observed arrivals.

WIRING ONLY. The measurement lives in consumers/prediction/accuracy.py, which
imports no pyflink and is testable from the project venv against
tests/test_prediction_contract.py. This file holds the two sources, the
ValueState round-trip and the TTL, which are the parts that need a cluster.

    make flink-up
    make prediction
    make flink-ui            # http://localhost:8086

accuracy.py holds the measurement and its 48-case spec runs in CI; this file
holds only what needs a cluster. Every trap in the wiring here had already
been paid for by the bunching job, and the list below is that debt.

--- what this inherits from bunching, unchanged ---

Four things were learned the hard way in consumers/bunching/job.py and are
applied here without rediscovering them:

  1. BYTES from Kafka, never SimpleStringSchema on the protobuf topic. Java's
     String(bytes, charset) substitutes U+FFFD rather than failing, so the
     wrong deserializer yields a healthy job emitting garbage.

  2. The watermark goes AFTER the Python maps. A Python map does not carry a
     record's timestamp to its output, so assigning on the source gives every
     record one identical timestamp and no window or timer ever fires.

  3. Parallelism MATCHES THE PARTITION COUNT, so each subtask reads one
     partition and its single watermark is effectively a per-partition
     watermark. At parallelism 2 over 3 partitions the bunching job dropped
     29.75% of records as late, silently.

  4. --pyFiles /opt/jobs on submit, or `from consumers...` fails with
     ModuleNotFoundError despite the code being mounted.

The new problem here is (3) applied to TWO topics with DIFFERENT partition
counts. See build_pipeline.
"""

from __future__ import annotations

import json
import logging
import os
from functools import partial

from pyflink.common import Duration, Types, WatermarkStrategy
from pyflink.common.time import Time
from pyflink.common.serialization import ByteArraySchema
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.functions import KeyedCoProcessFunction
from pyflink.datastream.state import StateTtlConfig, ValueStateDescriptor

from consumers import framing
from consumers.checkpointing import configure_checkpoints
from consumers.bunching.decode import decode
from consumers.prediction.accuracy import (
    PredictionBuffer,
    accuracy_record,
    join_key,
    parse_observation,
    parse_prediction,
)
from consumers.prediction.config import (
    CONFIG,
    CONSUMER_GROUP,
    OBSERVATION_TOPIC,
    PREDICTION_TOPIC,
    SINK_TOPIC,
)

log = logging.getLogger("prediction")

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_INTERNAL", "redpanda:9092")

# Where the job reads its sink schema's id from, at submit time. The compose
# service name, not .env's host-facing localhost:18081, for the same reason
# BOOTSTRAP defaults to redpanda:9092.
SCHEMA_REGISTRY = os.environ.get("SCHEMA_REGISTRY_INTERNAL", "http://redpanda:8081")
RECORD_TYPE = Types.MAP(Types.STRING(), Types.PICKLED_BYTE_ARRAY())


# --- environment, sources and sink -------------------------------------------


def build_env() -> StreamExecutionEnvironment:
    """Execution environment.

    Parallelism 6, matching raw.trip_updates -- the WIDER of the two topics.

        raw.trip_updates            6 partitions
        enriched.vehicle_positions  3 partitions

    A single env parallelism has to serve both, and going wider than a topic
    is harmless (three subtasks simply get no split and sit idle) while going
    narrower is the bug that cost the bunching job 29.75% of its records. So
    take the maximum, not the minimum.

    The idle subtasks are not free, though: a source split that never
    produces holds the watermark back forever unless it is marked idle, which
    is what `with_idleness` in the strategies below is for. Without it this
    job starts, reads both topics, and never fires a timer.
    """
    env = StreamExecutionEnvironment.get_execution_environment()
    # Interval, tolerated failures and retention: consumers/checkpointing.py,
    # which also records why none of them can live in the cluster config.
    configure_checkpoints(env)
    env.set_parallelism(6)
    return env


def _watermarks(assigner: TimestampAssigner) -> WatermarkStrategy:
    """Bounded out-of-orderness plus idleness.

    The 360s bound is bunching's measured value (see its config.py): it
    covers the event-time skew of reading several Kafka partitions through
    one watermark, which is a property of the reader rather than the feed.

    `with_idleness` is required here and was not in bunching. Two topics of
    different widths means some subtasks get a split from one topic and none
    from the other, and an idle split holds the watermark at its last value.
    The join's timers would then never fire.
    """
    return (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(360))
        .with_idleness(Duration.of_seconds(60))
        .with_timestamp_assigner(assigner)
    )


class _PredictionTimestamps(TimestampAssigner):
    """Event time for a prediction is when it was ISSUED.

    Not the predicted arrival. A prediction is an observation of Metro's
    opinion at `trip_timestamp`; using the forecast time as event time would
    place records in the future and stall the watermark behind them.
    """

    def extract_timestamp(self, value, record_timestamp: int) -> int:
        ts = value.get("issued_at") if isinstance(value, dict) else None
        return int(ts * 1000) if isinstance(ts, (int, float)) else record_timestamp


class _ObservationTimestamps(TimestampAssigner):
    """Event time for an arrival is the GPS fix."""

    def extract_timestamp(self, value, record_timestamp: int) -> int:
        ts = value.get("observed_at") if isinstance(value, dict) else None
        return int(ts * 1000) if isinstance(ts, (int, float)) else record_timestamp


def _source(topic: str, group_suffix: str) -> KafkaSource:
    """Kafka source over one topic.

    Separate consumer groups per stream so their offsets are independent;
    one stream being replayed must not move the other's position.

    UNBOUNDED only. An earlier draft carried a `bounded` flag that nothing
    passed, which is worse than not having one: it reads as a supported local
    replay path that was never wired. A bounded variant for spot-checking
    belongs next to consumers/bunching/smoke.py when someone needs it.

    LATEST, not earliest, and unlike bunching this is about memory rather
    than taste. StateTtlConfig only supports processing time (PyFlink 2.2
    exposes TtlTimeCharacteristic.ProcessingTime and nothing else), so the
    90-minute TTL measures wall-clock time since a key was last written. A
    replay of raw.trip_updates -- 31.9M retained records, about three days --
    compresses days of event time into however long the replay takes, TTL
    expires nothing during it, and the hashmap backend holds every key on
    the TaskManager heap. Only 27.5% of predictions found an arrival in the
    offline run, so most keys never drain and each keeps up to
    max_predictions_per_key predictions. That is an OutOfMemoryError with
    extra steps.

    History is answered offline instead: the curve in findings.md came from
    running accuracy.py over both topics directly. This job measures from
    the moment it starts, which is what a live accuracy monitor is for.
    """
    builder = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_topics(topic)
        .set_group_id(f"{CONSUMER_GROUP}-{group_suffix}")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(ByteArraySchema())
    )
    return builder.build()


def kafka_sink() -> KafkaSink:
    """Sink to analytics.prediction_accuracy.

    Still JSON, and still unregistered at the serializer level, for ADR 0007's
    reason: a protobuf serializer inside the Flink image means the protobuf<6
    conflict that put this job in its own image in the first place. The schema
    this one carries is a registered JSON Schema, so nothing about that changes.

    BYTES since 4B, with Confluent framing in front of the JSON
    (consumers/framing.py) so the JDBC sink can read it.
    """
    return (
        KafkaSink.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(SINK_TOPIC)
            .set_value_serialization_schema(ByteArraySchema())
            .build()
        )
        .build()
    )


class AccuracyJoin(KeyedCoProcessFunction):
    """The two-stream join, and the only stateful operator here.

    PyFlink 2.2 has no interval_join (verified: DataStream, KeyedStream and
    ConnectedStreams expose only `connect` and `process`), so the buffering
    is explicit. That is a better demonstration anyway: the state, its TTL
    and its expiry policy are all visible rather than implied by an operator.

    The decision logic is PredictionBuffer in accuracy.py. This class only
    moves state in and out of Flink, which is the part a test cannot reach.
    """

    def open(self, ctx):
        # TTL is what makes this bounded. Without it, every (start_date,
        # trip_id, stop_id) that never gets an observation -- a cancelled
        # trip, a bus that skipped a stop, a trip whose last stop is never
        # reported STOPPED_AT -- stays in state forever, and the job's
        # memory grows monotonically until it dies days later.
        #
        # Time, NOT Duration, and the difference is where this code runs.
        # open() executes inside the Python UDF worker, which has no py4j
        # gateway to the JVM. Duration wraps a java.time.Duration, so
        # Duration.of_seconds() tries to launch a gateway there and dies:
        #
        #   Exception: It's launching the PythonGatewayServer during Python
        #   UDF execution which is unexpected.
        #
        # pyflink.common.time.Time is a plain Python object holding
        # milliseconds, which is what StateTtlConfig.new_builder is typed to
        # take anyway. The watermark code above can use Duration because it
        # runs in the client while the job graph is built, where the gateway
        # exists. Same class, two processes, two answers.
        ttl = (
            StateTtlConfig
            .new_builder(Time.seconds(CONFIG.state_ttl_s))
            # The prediction stream keeps WRITING to a key as the feed
            # restates it, so refreshing on write keeps a live trip alive
            # and lets only genuinely abandoned keys expire.
            .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build()
        )
        desc = ValueStateDescriptor("accuracy-buffer", Types.PICKLED_BYTE_ARRAY())
        desc.enable_time_to_live(ttl)
        self.buffer = ctx.get_state(desc)

    def process_element1(self, value, ctx):
        """Stream 1: a prediction."""
        emit, updated = PredictionBuffer.from_dict(self.buffer.value()).on_prediction(value)
        self.buffer.update(updated.to_dict())
        yield from emit

    def process_element2(self, value, ctx):
        """Stream 2: an observed arrival, which drains the buffer."""
        emit, updated = PredictionBuffer.from_dict(self.buffer.value()).on_observation(value)
        self.buffer.update(updated.to_dict())
        yield from emit


def build_pipeline(env: StreamExecutionEnvironment) -> None:
    """Assemble both streams -> key -> connect -> join -> sink.

    The two streams are decoded differently and that is the point of the
    shape below:

        raw.trip_updates            JSON       -> json.loads
        enriched.vehicle_positions  protobuf   -> bunching.decode.decode

    Reusing bunching's decoder rather than writing a second one keeps the
    descriptor-loading trick (ADR 0006's revision) in exactly one place.

    Note the observation side filters to STOPPED_AT inside parse_observation,
    so roughly 77% of that stream is discarded before the join. Filtering
    early matters more here than usual: every surviving record becomes keyed
    state.
    """
    predictions = (
        env.from_source(_source(PREDICTION_TOPIC, "pred"),
                        WatermarkStrategy.no_watermarks(), "trip-updates")
        .map(lambda b: json.loads(b.decode("utf-8")), output_type=RECORD_TYPE)
        .map(parse_prediction, output_type=RECORD_TYPE)
        .filter(lambda r: r is not None)
        .assign_timestamps_and_watermarks(_watermarks(_PredictionTimestamps()))
        .key_by(join_key)
    )
    observations = (
        env.from_source(_source(OBSERVATION_TOPIC, "obs"),
                        WatermarkStrategy.no_watermarks(), "arrivals")
        .map(decode, output_type=RECORD_TYPE)
        .filter(lambda r: r is not None)
        .map(parse_observation, output_type=RECORD_TYPE)
        .filter(lambda r: r is not None)
        .assign_timestamps_and_watermarks(_watermarks(_ObservationTimestamps()))
        .key_by(join_key)
    )

    joined = predictions.connect(observations).process(
        AccuracyJoin(), output_type=RECORD_TYPE)

    # The sink schema's id, resolved ONCE at submit rather than per record.
    sid = framing.schema_id(SCHEMA_REGISTRY, f"{SINK_TOPIC}-value")
    log.info("sink schema %s-value -> id %s", SINK_TOPIC, sid)
    joined.map(partial(framing.frame, sid=sid),
               output_type=Types.PRIMITIVE_ARRAY(Types.BYTE())).sink_to(kafka_sink())


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(message)s")
    env = build_env()
    build_pipeline(env)
    env.execute("prediction-accuracy")


if __name__ == "__main__":
    main()
