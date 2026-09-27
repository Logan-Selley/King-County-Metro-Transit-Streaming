"""Bunching detection parameters, and what each one is measured against.

Declarative, same shape as producer/feeds.py and static/feed.py.

This is the file to change when the detector emits nonsense -- every number
here is a threshold, and thresholds are where a detector is right or wrong.

--- what bunching is, and why it is measured in DISTANCE not stops ---

The proposal defines it as "two vehicles on the same route within N seconds of
each other at the same stop". Detecting it that way needs stop ARRIVAL events,
and those are sparse: only ~27% of position records carry STOPPED_AT.

`enriched.vehicle_positions` carries `shape_dist_traveled` on 100% of records
(measured), which is each vehicle's distance along its route geometry. Two
vehicles on the same (route, direction) whose shape distances are close are
physically close, continuously, without waiting for either to reach a stop.

So the detector keys on (route_id, direction_id) and looks at gaps in
shape_dist_traveled between consecutive vehicles. That is strictly more data
than stop-based detection and it is available every 20 seconds per vehicle.

A (route, direction) pair does not imply one shape. Measured: 101 of 280
(route, direction) pairs carry more than one shape, covering 44.4% of trips,
and 92 shape pairs start over 500 m apart. The key stays (route_id,
direction_id), and the gap comparison is confirmed against straight-line
distance before it can alert -- see ADR 0007 and consumers/bunching/detect.py.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

SOURCE_TOPIC = "enriched.vehicle_positions"
SINK_TOPIC = "alerts.bunching"
CONSUMER_GROUP = "bunching"


# --- how a run is wired: live, or a replay ------------------------------------
#
# The live job reads the constants above. A replay runs the SAME job.py with
# different wiring, chosen by environment variable, so the replayed detector is
# the live detector and not a copy of it. What changes is only where it reads,
# where it writes, whether its input ends, and (for the variant run) the gate.
#
# The resolution lives HERE rather than in job.py because job.py imports
# pyflink, which the project venv does not carry (ADR 0006). As a pure function
# over a Mapping, tests/test_replay_contract.py can pin it: in particular, that
# an environment with no replay variables resolves to exactly the live wiring,
# since this function sits on the live job's startup path.

REPLAY_ENV = "BUNCHING_REPLAY"            # unset, "baseline" or "variant"
REPLAY_GATE_ENV = "BUNCHING_REPLAY_MIN_STOP_SEQUENCE"

# The two runs, and the namespace prefix they write under.
#
# THE PREFIX IS A DELIBERATE SECOND COPY of producer.replay.REPLAY_PREFIX, and
# not an import: this module runs inside the Flink image, which mounts
# consumers/ and schemas/ and NOT producer/ (docker-compose.yml's replay-detect
# target, and ADR 0006 on why the job image carries no Kafka client). Importing
# it would fail in the replay container, which is the one place it is needed.
# The two cannot drift silently: run_settings below refuses to return a replay
# whose names are not under this prefix, and test_replay_contract.py pins both
# this module's strings and the producer's.
REPLAY_RUNS = ("baseline", "variant")
REPLAY_PREFIX = "replay."


@dataclass(frozen=True)
class RunSettings:
    """Everything job.py needs to know about where it is running."""

    source_topic: str
    sink_topic: str
    # The registry subject whose schema id frames the output. A replay keeps
    # the LIVE subject: the records it writes have exactly that schema, and
    # resolving the id from a replay subject would mean registering subjects
    # nobody owns (ADR 0010).
    sink_subject: str
    group: str
    # Bounded: read from the earliest offset to the offsets current at start,
    # then finish. A bounded source's end emits the final watermark, which is
    # what closes the last event-time windows, so a replay ends with every
    # window fired rather than waiting for data that will never come.
    bounded: bool
    # Offsets are committed only for the live job, which resumes from them. A
    # replay has nothing to resume, and a committed group would be one more
    # abandoned group in `rpk group list` (the lag-check finding).
    commit_offsets: bool
    min_stop_sequence: int
    job_name: str


def run_settings(env: Mapping[str, str]) -> RunSettings:
    """Resolve the run's wiring from the environment.

    CONTRACT
      * REPLAY_ENV unset or empty -> the live wiring EXACTLY: SOURCE_TOPIC,
        SINK_TOPIC, f"{SINK_TOPIC}-value", CONSUMER_GROUP, unbounded, committing
        offsets, detect.MIN_STOP_SEQUENCE, job name "bunching-detector". The
        live job's startup path runs through this function, so this case is
        the one that must not change.
      * REPLAY_ENV = "baseline" or "variant":
          source  replay.enriched.vehicle_positions
          sink    replay.alerts.bunching.<run>
          subject the LIVE subject, f"{SINK_TOPIC}-value"
          group   f"replay-bunching-<run>"
          bounded, NOT committing offsets
          job     f"bunching-replay-<run>"
      * The gate: baseline uses the live detect.MIN_STOP_SEQUENCE and REFUSES a
        REPLAY_GATE_ENV value (a baseline with a different gate is not a
        baseline). variant REQUIRES REPLAY_GATE_ENV, an int >= 0; no default,
        because a variant that silently ran the live gate would report "no
        difference" and be believed.
      * Any other REPLAY_ENV value -> ValueError naming the allowed values.
      * INVARIANT, checked before returning: in a replay, neither topic nor the
        group is a live name. A resolution that fails it raises rather than
        returning, because the caller is about to produce.

    detect.py imports this module, so import MIN_STOP_SEQUENCE inside the
    function, not at the top of the file.
    """
    # Inside the function, because detect.py imports this module at module
    # scope: a top-level import here would be circular.
    from consumers.bunching.detect import MIN_STOP_SEQUENCE

    run = env.get(REPLAY_ENV, "").strip()

    if not run:
        # The live job's startup path. Every value is the constant above, so
        # nothing in this function can change how the live detector runs.
        return RunSettings(
            source_topic=SOURCE_TOPIC,
            sink_topic=SINK_TOPIC,
            sink_subject=f"{SINK_TOPIC}-value",
            group=CONSUMER_GROUP,
            bounded=False,
            commit_offsets=True,
            min_stop_sequence=MIN_STOP_SEQUENCE,
            job_name="bunching-detector",
        )

    if run not in REPLAY_RUNS:
        raise ValueError(
            f"{REPLAY_ENV}={run!r} is not a run; use one of "
            f"{', '.join(REPLAY_RUNS)}, or leave {REPLAY_ENV} unset to run live")

    settings = RunSettings(
        source_topic=f"{REPLAY_PREFIX}enriched.vehicle_positions",
        sink_topic=f"{REPLAY_PREFIX}alerts.bunching.{run}",
        # The LIVE subject, deliberately: these records have exactly the live
        # schema, so framing them with the live id is correct, and a
        # replay.<topic>-value subject would be one nobody registered (ADR 0010
        # and ADR 0005's auto-registration rule).
        sink_subject=f"{SINK_TOPIC}-value",
        # Hyphens, unlike the topics: a consumer group is named for humans in
        # `rpk group list`, and the Makefile's REPLAY_GROUPS uses the same form.
        group=f"replay-bunching-{run}",
        # Bounded, so the job ends when the replayed stream does and the final
        # watermark fires the last windows. Not committing offsets, because
        # there is nothing to resume and an abandoned group is clutter (the
        # lag-check finding).
        bounded=True,
        commit_offsets=False,
        min_stop_sequence=_replay_gate(run, env.get(REPLAY_GATE_ENV),
                                       MIN_STOP_SEQUENCE),
        job_name=f"bunching-replay-{run}",
    )

    # INVARIANT, checked before returning anything a caller could produce with.
    # A replay that resolved to a live name would write history into the live
    # topics, where the sinks would upsert it into the warehouse and the live
    # detector would alert on it. Raising rather than returning, because the
    # caller's next step is to produce.
    live_names = {SOURCE_TOPIC, SINK_TOPIC, CONSUMER_GROUP}
    namespace = REPLAY_PREFIX.rstrip(".")
    for label, value in (("source_topic", settings.source_topic),
                         ("sink_topic", settings.sink_topic),
                         ("group", settings.group)):
        if value in live_names or not value.startswith(namespace):
            raise ValueError(
                f"{label} resolved to {value!r}, which is not in the replay "
                f"namespace; refusing to run")

    return settings


def _replay_gate(run: str, raw: str | None, live_gate: int) -> int:
    """The terminal-stop gate for a replay run.

    The variant's gate is REQUIRED rather than defaulted: a variant that
    silently ran the live gate would report "no difference" and be believed,
    which is the one failure mode this whole experiment cannot detect later.
    The baseline's is FORBIDDEN for the mirror reason: a baseline with a
    different gate is not the live logic, so comparing it against the live
    output would measure the gate rather than the replay.
    """
    value = (raw or "").strip()

    if run == "baseline":
        if value:
            raise ValueError(
                f"{REPLAY_GATE_ENV}={value!r} is set for the baseline run, "
                f"which must use the live gate ({live_gate}); the baseline "
                "exists to reproduce the live logic")
        return live_gate

    if not value:
        raise ValueError(
            f"the variant run requires {REPLAY_GATE_ENV} (an integer >= 0); "
            "without it the variant would run the live gate and report no "
            "difference")

    try:
        gate = int(value)
    except ValueError:
        raise ValueError(
            f"{REPLAY_GATE_ENV}={value!r} is not an integer") from None

    if gate < 0:
        raise ValueError(
            f"{REPLAY_GATE_ENV}={gate} is negative; the gate is a stop "
            "sequence, so 0 is the lowest meaningful value")
    return gate


@dataclass(frozen=True)
class BunchingConfig:
    """Thresholds for the detector."""

    # Vehicles closer together than this along the route are candidates.
    #
    # UNITS ARE FEET, and that is a measured fact rather than an assumption:
    # ST_Length(geom::geography) / max_dist came out at 0.3048 across 423 of
    # 424 shapes, which is the foot-to-metre constant falling out of the data.
    # (One shape publishes metres; see the note below.)
    #
    # 1,000 ft is roughly 2-3 city blocks. Two buses on the same route that
    # close are visibly bunched to a rider at a stop between them.
    gap_threshold_ft: float = 1_000.0

    # Ignore pairs where either vehicle's position is older than this. A
    # vehicle emerging from a tunnel reports a burst of stale positions
    # (proposal section 5), and pairing a fresh position against a 4-minute-old
    # one measures where a bus WAS, not where it is.
    max_position_age_s: int = 90

    # Event-time window the detector aggregates over. Positions publish every
    # 20s, so 60s holds ~3 observations per vehicle: enough to be robust to a
    # single dropped poll, short enough that a bus moves under 1 km within it.
    window_s: int = 60

    # Watermark lateness.
    #
    # Sized from the event-time skew the JOB sees, which is dominated by
    # reading three Kafka partitions through one watermark (see
    # watermark_strategy in job.py -- PyFlink cannot do per-partition
    # watermarks here). The gap between archived payloads is a property of
    # the FEED and is not what this bound has to cover.
    #
    # Measured on 60,000 steady-state records, interleaved as the job reads
    # them:
    #
    #     p50   65s     p95  253s
    #     p75  150s     p99  297s
    #     p90  227s     max  342s
    #
    #     bound 120s -> keeps 67.7%
    #     bound 240s -> keeps 93.9%
    #     bound 360s -> keeps 100.0%
    #
    # Within a single partition the same data never exceeds 111s, so the
    # disorder is an artifact of the interleaving rather than the feed.
    #
    # 120s silently discards 29.75% of records as late: 193,364 of
    # 649,949 over one evening, with no error anywhere. The cost of 360s is
    # that a window closes six minutes after its event time rather than two,
    # which is the honest price of the watermark placement.
    allowed_lateness_s: int = 360

    # Suppress repeat alerts for the same vehicle pair. Without this a pair
    # that stays bunched for ten minutes emits an alert every window, and the
    # alert topic becomes a position feed with extra steps.
    cooldown_s: int = 600

    # A pair must be under the gap threshold in at least this many consecutive
    # windows before alerting. One window is a GPS jitter artifact or two buses
    # legitimately passing at a layover; two is a pattern.
    min_consecutive_windows: int = 2

    @property
    def gap_threshold_m(self) -> float:
        """The threshold in metres, for comparing against geography lengths."""
        return self.gap_threshold_ft * 0.3048


CONFIG = BunchingConfig()


# --- the unit trap, measured -------------------------------------------------
#
# shape_dist_traveled is in FEET for 423 of 424 shapes and METRES for exactly
# one (shape_id 63424). Harmless for this detector, but only because of how
# the comparison is framed:
#
#   SAFE    comparing two vehicles on the SAME shape -- both distances come
#           from the same scale, so the gap is meaningful whatever the unit
#   UNSAFE  any threshold applied across routes, or any aggregate that sums
#           or averages distances from different shapes
#
# The detector keys on (route_id, direction_id), which does NOT guarantee one
# shape, so this anomaly is handled only where it is provably safe: shape
# 63424 is the only shape on its (route 7994, direction 1), so no pair can
# ever straddle the unit boundary. Measured, not assumed.
#
# A future mart that ranks routes by distance MUST normalise first.
# ref.locate_on_shape() already returns feed units per shape rather than
# metres, for the same reason.
UNIT_ANOMALY_SHAPE = "63424"


# Routes worth spot-checking a detector against. RapidRide lines run at high
# frequency, which is where bunching actually happens -- a route with a
# 30-minute headway cannot bunch in any interesting way.
#
# The proposal's own example is "which RapidRide segments bunch worst during
# PM peak", so these are the routes the output has to be credible on.
HIGH_FREQUENCY_ROUTES = ("A Line", "B Line", "C Line", "D Line", "E Line",
                         "F Line", "G Line", "H Line")
