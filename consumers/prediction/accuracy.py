"""Prediction-accuracy logic, with no Flink in it.

tests/test_prediction_contract.py is the spec. The three helpers most callers
lean on are lead_bucket, epoch_s and service_day, three because the two topics
publish start_date in different formats. See service_day.

Same split as consumers/bunching/detect.py, for the same reason: `job.py`
holds the Flink wiring and imports these, so everything that can be wrong
about the *measurement* is reachable from the project venv and from
tests/test_prediction_contract.py with no cluster at all.

--- why this is a hand-rolled join ---

The textbook answer is an interval join: key both streams, and ask Flink for
every prediction within N minutes before its observation. PyFlink 2.2 does not
have one. Measured:

    DataStream        ['connect', 'process']
    KeyedStream       ['connect', 'process']
    ConnectedStreams  ['process']

No `interval_join` anywhere. The only two-stream primitive is `connect()`
feeding a KeyedCoProcessFunction, so the buffering, the match and the state
expiry are all written out by hand. `PredictionBuffer` below is that logic,
kept as a plain dataclass so it can be tested without a cluster, exactly like
bunching's `BunchingState`.

--- the asymmetry that shapes everything ---

The two streams are not symmetric and the join must not treat them as such.

    predictions   MANY per key (mean 18, max 78), arriving over an hour,
                  each one a separate data point on the error curve
    observation   ONE per key, arriving last, and it is what resolves every
                  buffered prediction at once

So a prediction is buffered and an observation drains the buffer, emitting one
accuracy record per prediction held. The reverse case (a prediction arriving
after the observation) is real but rare, and is the 2.8% of records with a
negative lead: the feed restating an arrival already in the past.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from consumers.prediction.config import CONFIG, LEAD_BUCKET_LABELS, LEAD_BUCKETS_S


# --- helpers -----------------------------------------------------------------


def lead_bucket(lead_s: float) -> str:
    """Lead time in seconds -> the bucket label for the error curve.

    Negative lead is its own bucket rather than being clamped into "0-2m".
    "The sign said the bus left three minutes ago and it has not arrived" is
    a different rider experience from "the sign said two minutes", and
    folding them together would flatter the short end of the curve, which is
    the end the whole chart is judged on.
    """
    if lead_s < 0:
        return LEAD_BUCKET_LABELS[0]
    # LABELS[0] is "past", so bound i maps to label i+1. Getting this offset
    # wrong relabels every bucket without failing anything.
    for i in range(len(LEAD_BUCKETS_S) - 1, -1, -1):
        if lead_s >= LEAD_BUCKETS_S[i]:
            return LEAD_BUCKET_LABELS[i + 1]
    return LEAD_BUCKET_LABELS[1]


def epoch_s(value) -> float | None:
    """ISO-8601 string or epoch number -> epoch seconds.

    The two streams disagree about time format and always will:

        raw.trip_updates            ISO strings  "2026-09-22T06:25:25+00:00"
        enriched.vehicle_positions  int64 epoch  1789959734

    raw is JSON straight from the producer's decode; enriched is protobuf,
    where decode.to_dict() keeps int64 an int. Normalising in one place is
    what stops the join comparing a string to a float and silently matching
    nothing -- the same class of bug as the ISO-vs-GTFS date mismatch that
    makes `schedule_deviation` return None for every record.

    Returns None rather than raising. A record whose time cannot be read is
    dropped by the caller, not allowed to kill a TaskManager slot.

    THE ALL-DIGITS GUARD IS LOAD-BEARING. Since Python 3.11,
    `datetime.fromisoformat` accepts basic ISO-8601, so a GTFS service date
    like "20260922" parses happily as 2026-09-22T00:00:00 instead of
    failing. Both formats are live in this project: `start_date` is GTFS
    "YYYYMMDD" in some paths and ISO "YYYY-MM-DD" in others, the exact
    mismatch that makes `schedule_deviation` return None for every record.
    Here it would be worse than None: midnight of the service day is a
    plausible-looking timestamp, so a date fed in by mistake would produce
    errors of several hours and look like a timezone bug.
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if not isinstance(value, str) or value.isdigit():
        return None
    try:
        from datetime import datetime

        return datetime.fromisoformat(value).timestamp()
    except (ValueError, TypeError):
        return None


def service_day(value) -> str | None:
    """start_date from either stream -> one canonical GTFS YYYYMMDD.

    THE TWO TOPICS PUBLISH THIS FIELD IN DIFFERENT FORMATS, on purpose, and
    the join does not work until one of them is normalised. Measured on the
    live wire:

        raw.trip_updates            start_date = "2026-09-22"   ISO
        enriched.vehicle_positions  start_date = "20260922"     GTFS

    Neither side is wrong. The producer decodes the realtime field to a
    `date` and serialises it with `.isoformat()`, so raw carries ISO;
    `consumers/enrichment/schema.py` then does `.replace("-", "")` on the way
    into protobuf, deliberately, with a docstring saying "as the agency
    publishes it". The realtime wire form really is GTFS, and the dashes
    were an artifact of the producer's decode.

    Left uncorrected this is the worst kind of bug: `join_key` would build
    `2026-09-22:675379571:11140` against `20260922:675379571:11140`, no key
    would ever match, every prediction would sit in state until its TTL
    expired, and the job would run clean with an empty sink. The same shape
    as `schedule_deviation` returning None for every record.

    NOT imported from consumers.enrichment. That module reaches `psycopg`,
    which the Flink image does not have, and TestImportIsolation fails the
    build if anything here tries. A five-line local helper is the price of
    ADR 0006's isolation.
    """
    if value is None:
        return None
    if hasattr(value, "strftime"):          # date / datetime
        return value.strftime("%Y%m%d")
    if not isinstance(value, str):
        return None
    compact = value.replace("-", "")
    return compact if len(compact) == 8 and compact.isdigit() else None


# --- parsing, measurement, and the join --------------------------------------


def parse_prediction(rec: dict) -> dict | None:
    """One raw.trip_updates record -> the fields the join needs.

    Input is already a dict: the topic is JSON, so job.py does json.loads
    before this runs. Fields available are in config.py's module docstring;
    the ones this needs are trip_id, stop_id, start_date, route_id,
    stop_sequence, arrival_time (the PREDICTION) and trip_timestamp (when it
    was issued).

    Normalise both times through epoch_s. Compute nothing else here -- the
    lead time belongs in accuracy_record, where the observation is also
    available, so that there is exactly one place that does arithmetic on
    these two clocks.

    Return None when trip_id, stop_id, start_date or arrival_time is missing,
    since none of those can be reconstructed. `arrival_delay` is NOT required:
    it is Metro's own claim about lateness and this module exists to check
    that claim, not to depend on it.

    Never raise.
    """
    prediction_keys = [
        "trip_id", "stop_id", "start_date", "route_id", "stop_sequence",
    ]
    required_keys = ["trip_id", "stop_id", "start_date"]
    if not isinstance(rec, dict):
        return None
    if service_day(rec.get("start_date")) is None:
        return None
    prediction_record = {k: rec.get(k) for k in prediction_keys}
    if any(prediction_record[k] is None for k in required_keys):
        return None
    arrival_time_epoch_s = epoch_s(rec.get("arrival_time"))
    if arrival_time_epoch_s is None:
        return None
    # int(), not float. The sink's TimestampConverter reads these as unix
    # seconds and refuses FLOAT64: with floats the JDBC task died on "Schema
    # Schema{FLOAT64} does not correspond to a known timestamp type format"
    # having written nothing. The protobuf path never met this because int64
    # arrives as an integer.
    prediction_record["predicted_arrival"] = int(arrival_time_epoch_s)
    issued_at = epoch_s(rec.get("trip_timestamp"))
    prediction_record["issued_at"] = int(issued_at) if issued_at is not None else None
    return prediction_record


def parse_observation(rec: dict) -> dict | None:
    """One enriched.vehicle_positions record -> an observed arrival, or None.

    **Return None unless current_status == "STOPPED_AT".** That is the filter
    that turns a position feed into an arrival feed, and it is why this
    function returns None far more often than parse_prediction: measured,
    23.1% of positions are STOPPED_AT, and 100% of those carry both stop_id
    and current_stop_sequence.

    Careful with absence here. An ABSENT current_status means IN_TRANSIT_TO
    rather than unknown, because the proto2 field carries
    `[default = IN_TRANSIT_TO]`. decode.to_dict()
    reads through the attribute accessor so the default is already applied,
    and a missing value is therefore genuinely missing rather than
    IN_TRANSIT_TO. Test the value, not presence.

    The observed arrival time is `position_timestamp`, which is the GPS fix,
    not the enrichment time. Keep trip_id, stop_id, start_date, route_id,
    route_short_name and that timestamp.

    Never raise.
    """
    if not isinstance(rec, dict):
        return None
    if service_day(rec.get("start_date")) is None:
        return None
    if rec.get("current_status") != "STOPPED_AT":
        return None
    observation_keys = [
        "trip_id", "stop_id", "start_date", "route_id", "route_short_name",
    ]
    required_keys = ["trip_id", "stop_id"]
    observation_record = {k: rec.get(k) for k in observation_keys}
    if any(observation_record[k] is None for k in required_keys):
        return None
    observed_at_epoch_s = epoch_s(rec.get("position_timestamp"))
    if observed_at_epoch_s is None:
        return None
    # int(), for the reason parse_prediction records, and this is the field the
    # connector PARTITIONS on, so a FLOAT64 here fails twice over.
    observation_record["observed_at"] = int(observed_at_epoch_s)
    return observation_record


def join_key(rec: dict) -> str:
    """Join key for both streams.

    **(service_day(start_date), trip_id, stop_id)** -- in that order, and
    start_date is neither optional nor usable raw.

    `trip_id` repeats every service day: the same scheduled trip runs again
    tomorrow with the same id. Keying on (trip_id, stop_id) alone lets a
    prediction issued today match an observation from tomorrow's run of the
    same trip, which would produce an error of roughly 24 hours and look like
    a unit bug rather than a join bug. `service_date_origin` exists for the
    same reason.

    NORMALISE THROUGH service_day() HERE, not in the parsers. The two topics
    publish this field in different formats (ISO vs GTFS -- see that
    function), and this is the one place both streams pass through, so it is
    the one place the formats have to be reconciled. The parsers keep the raw
    value, which is what `test_keeps_the_join_identity` asserts.

    service_day first, so the key sorts by service day. That is not cosmetic:
    it is what makes the key column readable in the Flink UI during a
    spot-check.

    A string, not a tuple, for the same reason as bunching's assign_route_key:
    a tuple crossing the Python/JVM boundary without an explicit type gets
    pickled rather than rejected.
    """
    return f"{service_day(rec['start_date'])}:{rec['trip_id']}:{rec['stop_id']}"


def accuracy_record(prediction: dict, observation: dict) -> dict | None:
    """One prediction + its observed arrival -> one point on the error curve.

    Two quantities, and getting their signs the right way round is the whole
    measurement:

        lead_time_s = predicted_arrival - issued_at
            How far ahead the estimate was made. Always from the
            PREDICTION's own two timestamps, never from the observation --
            a prediction's lead time is a property of when it was issued,
            and must not change depending on when the bus actually turned up.

        error_s = predicted_arrival - actual_arrival
            POSITIVE means the prediction was LATE relative to reality: it
            said the bus would arrive later than it did, so the rider missed
            it. Negative means the bus was later than promised. State the
            convention in the output and keep it, because a sign flip here
            inverts every conclusion and nothing downstream can detect it.

    Emit both, plus abs_error_s, lead_bucket(lead_time_s), and enough
    identity to group by later: trip_id, stop_id, route_id, route_short_name,
    start_date, and both raw timestamps.

    Drop the pair (return None) when the lead exceeds CONFIG.join_window_s:
    the buffer may hold a prediction slightly past the window, and a 24-hour
    lead is a next-service-day artifact rather than a data point.

    CONFIG.keep_negative_lead is True, so a negative lead is kept and
    bucketed as "past" rather than dropped.
    """
    issued_at = prediction.get('issued_at')
    observed_at = observation.get('observed_at')
    if issued_at is None or observed_at is None:
        return None
    if issued_at >= observed_at:
        # Issued after the bus had already arrived, so it is not a forecast.
        # 97% of trip updates carry arrival_time == departure_time, and Metro
        # keeps restating that one time while the bus sits at the stop.
        # Measured over 702,069 joined pairs, 13.4% were this, concentrated
        # where they do the most damage: 74.6% of the 0-2m bucket and 99.5%
        # of "past". Kept, they doubled the 0-2m median error (45s -> 85s)
        # and bent the curve so the shortest lead looked worse than 2-5m.
        return None
    lead_time_s = prediction['predicted_arrival'] - issued_at
    if lead_time_s > CONFIG.join_window_s:
        return None
    if lead_time_s < 0 and not CONFIG.keep_negative_lead:
        return None
    error_s = prediction['predicted_arrival'] - observed_at
    return {
        'lead_time_s': lead_time_s,
        'error_s': error_s,
        'abs_error_s': abs(error_s),
        'lead_bucket': lead_bucket(lead_time_s),
        'trip_id': prediction['trip_id'],
        'stop_id': prediction['stop_id'],
        'route_id': prediction['route_id'],
        'route_short_name': observation.get('route_short_name'),
        'start_date': prediction['start_date'],
        'issued_at': prediction['issued_at'],
        'predicted_arrival': prediction['predicted_arrival'],
        'observed_at': observation['observed_at'],
    }


@dataclass
class PredictionBuffer:
    """Per-key state: the predictions waiting for their observation.

    This is the join. It lives here rather than in job.py because it is the
    measurement's logic, not Flink's, and because state reachable only
    through a running cluster is state that never gets tested.

    Immutable in the same style as BunchingState: the methods return a new
    buffer rather than mutating, so a caller that dies between the decision
    and the ValueState write cannot leave a half-updated buffer behind.

    ONE observation, MANY predictions. `observed_at` is a single value
    because a (start_date, trip_id, stop_id) is arrived at once;
    CONFIG.first_observation_wins says which STOPPED_AT counts, since a bus
    dwelling at a stop reports several.
    """

    predictions: list[dict] = field(default_factory=list)
    observed_at: float | None = None

    def on_prediction(self, prediction: dict) -> tuple[list[dict], PredictionBuffer]:
        """Fold in a prediction. Returns (records to emit, new buffer).

        Two cases:

          * No observation yet -- the normal case. Buffer it and emit
            nothing. Respect CONFIG.max_predictions_per_key; dropping the
            OLDEST is correct if the cap is hit, because the newest
            prediction is the one closest to the truth.

          * Observation already seen -- the 2.8% negative-lead case, where
            the feed restates an arrival that has already happened. Emit
            immediately and do not buffer.
        """
        if self.observed_at is not None:
            # The arrival is already in, so this is the restatement case.
            # Resolve against the stored timestamp and buffer nothing --
            # waiting for a second observation would wait forever.
            record = accuracy_record(prediction, {"observed_at": self.observed_at})
            return ([record] if record is not None else [], self)
        buffered = self.predictions + [prediction]
        if len(buffered) > CONFIG.max_predictions_per_key:
            # Drop from the FRONT, not the back. The newest estimate is the one
            # closest to the truth about a bus that is about to arrive.
            buffered = buffered[-CONFIG.max_predictions_per_key:]
        return ([], PredictionBuffer(predictions=buffered,
                                     observed_at=self.observed_at))

    def on_observation(self, observation: dict) -> tuple[list[dict], PredictionBuffer]:
        """Fold in an observed arrival. Returns (records to emit, new buffer).

        This drains the buffer: one accuracy_record per buffered prediction,
        which is what produces a whole error curve for a single stop from a
        single arrival.

        If an observation was already recorded and
        CONFIG.first_observation_wins is set, ignore this one and emit
        nothing. A bus sitting at a stop reports STOPPED_AT on every poll,
        and taking the last would measure its DEPARTURE while making the
        error depend on dwell time.

        Keep `observed_at` in the returned buffer, so a late prediction
        arriving afterwards can still be resolved by on_prediction.
        """
        if self.observed_at is not None and CONFIG.first_observation_wins:
            # A bus dwelling at a stop reports STOPPED_AT on every poll. The
            # first is the arrival; taking the last would measure departure and
            # make the error depend on how long it sat there.
            return ([], self)
        records = []
        for prediction in self.predictions:
            record = accuracy_record(prediction, observation)
            if record is not None:
                # Beyond CONFIG.join_window_s. The buffer can hold a prediction
                # past the window when an arrival never comes, so this is a real
                # case rather than a defensive one.
                records.append(record)
        # predictions is emptied because they are now resolved. observed_at is
        # kept so a prediction arriving after this one can still resolve.
        return (records, PredictionBuffer(
            predictions=[], observed_at=observation.get("observed_at")))

    def to_dict(self) -> dict:
        """Plain dict for ValueState, not the dataclass.

        Same reasoning as BunchingState.to_dict: PyFlink pickles either, but
        a pickled dataclass restores without calling __init__ and raises
        AttributeError on a field added later. A dict survives the change.
        """
        return {"predictions": self.predictions, "observed_at": self.observed_at}

    @classmethod
    def from_dict(cls, raw: dict | None) -> PredictionBuffer:
        """ValueState is None the first time a key is seen."""
        raw = raw or {}
        return cls(
            predictions=list(raw.get("predictions") or []),
            observed_at=raw.get("observed_at"),
        )
