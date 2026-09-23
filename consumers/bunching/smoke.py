"""Prove the Flink -> Kafka path before any detection logic depends on it.

    make flink-smoke

The Phase 3 analogue of tests/smoke_roundtrip.py. It reads a bounded slice of
enriched.vehicle_positions, counts what it got, and exits.

--- what this is actually testing ---

Not the logic. There is none. It tests the four things that fail in ways that
look like Python errors and are not:

  1. The Kafka connector JAR loads. KafkaSource is a thin wrapper over the
     Java connector, so a missing or mismatched JAR surfaces as
     ClassNotFoundException at submit -- after the Python imported cleanly.

  2. The connector's Flink version matches the runtime. The image pins
     flink-sql-connector-kafka-5.0.0-2.2 against a 2.2.0 runtime; connector
     JARs are compiled per Flink minor. Pairing 2.3.0 with a 1.20-built
     connector imported fine, submitted fine, and crash-looped in RESTARTING
     with a NoSuchMethodError -- see docker/Dockerfile.flink.

  3. The job reaches the broker on the INTERNAL listener. redpanda:9092, not
     localhost:19092 -- the job runs inside the compose network. Getting this
     wrong does not error; the job starts, assigns no partitions, and waits
     forever looking healthy.

  4. Records cross the Python/JVM boundary intact AND decode. This one was
     added late, and the reason is the point: the first version mapped every
     record to the constant 1. It passed for a week while the real job's
     deserializer was wrong for the topic's format, because a count proves
     arrival and says nothing about content. A smoke test that cannot fail
     for the reason you are worried about is decoration.

A bounded source on purpose: `set_bounded(latest())` makes this terminate
instead of streaming forever, so it is usable as a check rather than something
you have to remember to kill.
"""

from __future__ import annotations

import os

from pyflink.common import Types, WatermarkStrategy
from pyflink.common.serialization import ByteArraySchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource

from consumers.bunching.decode import decode

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_INTERNAL", "redpanda:9092")
TOPIC = "enriched.vehicle_positions"


def _probe(raw: bytes) -> str:
    """One record -> a line saying whether it decoded, and to what.

    The assertion this job exists to make, now that there IS a decode path.
    An earlier version mapped every record to the constant 1, which proved
    bytes arrived and nothing else -- and it kept passing while the job's
    deserializer was wrong for the topic's format. A count is not a decode.
    """
    rec = decode(raw)
    if rec is None:
        return f"UNDECODED {len(raw)}B {raw[:8].hex(' ')}"
    return (f"ok vehicle={rec.get('vehicle_id')} "
            f"ts={rec.get('position_timestamp')} "
            f"dist={rec.get('shape_dist_traveled')}")


def main() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_topics(TOPIC)
        .set_group_id("flink-smoke")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        # Bounded: stop at whatever the end of the log is when the job starts.
        .set_bounded(KafkaOffsetsInitializer.latest())
        # BYTES, not SimpleStringSchema. The topic is Confluent-framed
        # protobuf, and Java's String(bytes, charset) SUBSTITUTES U+FFFD for
        # undecodable input rather than raising -- so a string deserializer
        # here silently corrupts every record and still reports success.
        .set_value_only_deserializer(ByteArraySchema())
        .build()
    )

    stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "enriched")
    stream.map(_probe, output_type=Types.STRING()).print()

    env.execute("flink-smoke")


if __name__ == "__main__":
    main()
