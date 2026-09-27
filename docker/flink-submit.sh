#!/usr/bin/env bash
# Submit the Flink jobs once the JobManager is up, and re-submit them after a
# restart. Run by the `flink-submit` compose service on every boot.
#
#     make resume          # restart the submitter, i.e. run this again
#     make flink-up        # starts the submitter along with the cluster
#
# WHY THIS EXISTS. There is no JobManager HA, so a restarted JobManager logs
# "Successfully recovered 0 persisted job graphs" and the job is simply gone.
# docker-compose.yml records that decision (HA is more machinery than a
# single-node laptop stack is worth) and the consequence: before this script,
# a restart meant the detector was silently absent until somebody noticed the
# alert topic had stopped.
#
# IDEMPOTENT BY JOB NAME. `flink list -r` already reports what is running, so
# a re-run on a machine where the jobs survived does nothing. Without that
# check, every compose restart would stack another copy of the same job.
#
# RESUMING FROM A RETAINED CHECKPOINT IS STILL MANUAL, deliberately. Each job
# keeps its checkpoints across a cancellation (RETAIN_ON_CANCELLATION, set in
# consumers/checkpointing.py; until 2026-09-25 it sat in the JobManager's
# config, where it had no effect on submitted jobs), so the option exists:
#
#     flink run -s s3://transit-raw/flink-checkpoints/<job-id>/chk-N \
#       --pyFiles /opt/jobs -py /opt/jobs/consumers/bunching/job.py
#
# Automating it means finding the newest chk-N under a job id that changes on
# every submission, from a container that would then need MinIO credentials
# for read access. Restarting with empty state is the right trade here:
# bunching's per-pair state refills within minutes, and a job that is absent
# reports nothing at all.
set -euo pipefail

# Not localhost: this runs in its own container, where the CLI's default
# jobmanager.rpc.address resolves to nothing. -m below is what makes the
# submitter find the cluster.
JM="${FLINK_JM:-flink-jobmanager}"
REST="http://${JM}:8081"

# The REST API answers a few seconds after the container starts, and a submit
# against a half-started cluster fails outright rather than waiting.
for _ in $(seq 1 60); do
    curl -sf "${REST}/overview" >/dev/null 2>&1 && break
    sleep 2
done
if ! curl -sf "${REST}/overview" >/dev/null 2>&1; then
    echo "jobmanager never answered at ${REST}" >&2
    exit 1
fi
echo "jobmanager reachable at ${REST}"

# The registry has to answer too, because the jobs resolve their sink schema id
# at SUBMISSION time (consumers/framing.py), before the job graph is even built.
# A submit that beats Redpanda's readiness dies with "Connection refused" on
# that lookup. Measured: three failed attempts on one boot, recovering on the
# fourth only because the restart policy kept retrying, which dresses a slow
# boot up as a successful one and would look like a flaky submit script.
#
# Same shape as the JobManager wait above, and the same default URL the jobs
# themselves fall back to.
REGISTRY="${SCHEMA_REGISTRY_INTERNAL:-http://redpanda:8081}"
for _ in $(seq 1 60); do
    curl -sf "${REGISTRY}/subjects" >/dev/null 2>&1 && break
    sleep 2
done
if ! curl -sf "${REGISTRY}/subjects" >/dev/null 2>&1; then
    echo "schema registry never answered at ${REGISTRY}" >&2
    exit 1
fi
echo "schema registry reachable at ${REGISTRY}"

# -m ON EVERY CLI CALL, not just the submit. `flink run` is the one that has
# to reach the cluster, but `flink list` has the same problem and shows it as a
# HANG rather than an error: the image's conf points jobmanager.rpc.address at
# localhost, so from this container the list waits on a socket that will never
# answer. Measured: the script sat at the reachability check for a minute
# printing nothing.
#
# -d: detached, so the job outlives this container. Without it `flink run`
# attaches and this script would block forever holding the client.
submit() {
    local file="$1" name="$2"
    local listing
    # FAIL SAFE. If the listing cannot be read, this exits rather than
    # submitting blind: a duplicate job would write the same sink twice, and
    # the restart policy retries this on the next pass anyway.
    if ! listing=$(timeout 30 flink list -r -m "${JM}:8081" 2>/dev/null); then
        echo "flink list failed or timed out; not submitting ${name}" >&2
        exit 1
    fi
    if printf '%s' "${listing}" | grep -q "${name}"; then
        echo "already running, skipping: ${name}"
        return 0
    fi
    echo "submitting ${name}"
    flink run -d -m "${JM}:8081" --pyFiles /opt/jobs -py "/opt/jobs/${file}"
}

submit consumers/bunching/job.py bunching-detector

# ON BY DEFAULT, and it used to be the opposite for a reason that was wrong.
# The gate existed because "the prediction join reads raw.trip_updates from
# earliest, which is ~47M records, so auto-submitting would kick that replay off
# on every boot". It does not: that source starts at latest(), a change made
# during the Phase 3 close-out, so a fresh submission begins at the end of the
# topic and costs nothing to replay.
#
# What leaving it off cost is measured rather than argued. After a crash at
# 13:11 Pacific only the detector came back; the prediction job stayed absent
# for hours and nothing said so, because sources.yml carried no freshness check
# on its output table. It has one now, so the next absence fails a task at
# the next hourly :15 run rather than being a discovery: 30 to 90 minutes after
# the job stops writing, not within half an hour.
#
# FLINK_AUTOSUBMIT_PREDICTION=0 opts out, for a machine that wants only the
# detector.
if [ "${FLINK_AUTOSUBMIT_PREDICTION:-1}" = "1" ]; then
    submit consumers/prediction/job.py prediction-accuracy
else
    echo "prediction job not submitted (FLINK_AUTOSUBMIT_PREDICTION=0)"
fi

echo "submit pass complete"
