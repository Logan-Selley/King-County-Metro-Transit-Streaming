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

  4. Records cross the Python/JVM boundary intact.

A bounded source on purpose: `set_bounded(latest())` makes this terminate
instead of streaming forever, so it is usable as a check rather than something
you have to remember to kill.
"""

from __future__ import annotations

import os

from pyflink.common import Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_INTERNAL", "redpanda:9092")
TOPIC = "enriched.vehicle_positions"


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
        # Raw bytes as a string. The records are protobuf with a 5-byte
        # Confluent prefix, so this is deliberately NOT a meaningful decode --
        # it only proves bytes arrive. Decoding is 3B's problem.
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "enriched")

    # map to a constant and count: cheapest possible proof that records made
    # it through the boundary without depending on their content.
    counted = stream.map(lambda _: 1, output_type=Types.INT())
    counted.print()

    env.execute("flink-smoke")


if __name__ == "__main__":
    main()
