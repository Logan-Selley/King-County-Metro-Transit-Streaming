"""Checkpoint settings for every Flink job, set in the job rather than the cluster.

WHY IN THE JOB. A job's checkpoint settings are assembled by the CLIENT that
submits it (from its own config and the job's code) and fixed in the job graph;
the JobManager's properties do not reach them. So docker-compose.yml's
FLINK_PROPERTIES for the JobManager, which flink-submit.sh relied on for
hand-resuming a cancelled job, cannot set them. Measured on 2026-09-25 through
the REST API, for both running jobs:

    externalization: {'enabled': False, 'delete_on_cancellation': True}
    tolerable_failed_checkpoints: 0

Setting them here puts them in the job graph whoever submits it: the
flink-submit container, `make bunching` from the JobManager, or a replay's
local run.

WHY TOLERATE FAILED CHECKPOINTS. The default is zero, so one checkpoint that
cannot be written fails the whole job. On 2026-09-25 that is what happened to
prediction-accuracy: the host was short on memory (15G in swap), MinIO on the
spinning /mnt/F disk slowed down, and its checkpoint uploads timed out ("Unable
to complete multi-part upload ... Read timed out"). Every timeout restarted the
job: 279 failed checkpoints and 162 restores, and each restart left its Python
workers behind as zombies in the TaskManager, over 1,100 of them.

A failed checkpoint loses no data. Processing carries on, and the next
checkpoint tries again. The only cost is that the latest RECOVERABLE point gets
older while storage is slow. Ten failures at a 30s interval is five minutes of
slow storage before Flink gives up, and a restart from a five-minute-old
checkpoint re-reads five minutes from Kafka, which both topics hold for days.
The limit still exists because storage that is really broken should fail the
job visibly rather than let it run for hours with nothing recoverable.
"""

from __future__ import annotations

CHECKPOINT_INTERVAL_MS = 30_000
TOLERABLE_FAILED_CHECKPOINTS = 10

# THE PREDICTION JOIN CHECKPOINTS EVERY 5 MINUTES, not 30 s. Its state is the
# trip-update predictions waiting for their arrivals: 203 MB on 2026-09-29
# (average 148 MB, peak 368 MB), and the hashmap backend uploads ALL of it on
# every checkpoint. At 30 s that was MinIO writing 15.75 MB/s, nearly every
# write the spinning disk took (91% busy), and MinIO was OOM-killed mid-upload
# at 1.2 GB resident. The bunching detector's state is 3 MB and keeps 30 s.
# The cost: a restore now re-reads up to 5 minutes of Kafka instead of 30 s.
PREDICTION_CHECKPOINT_INTERVAL_MS = 300_000

# What TOLERABLE_FAILED_CHECKPOINTS means is a span of time: about five minutes
# of storage failing before the job gives up (see the module docstring). At a
# longer interval the same span is fewer checkpoints.
TOLERATED_FAILURE_SPAN_MS = TOLERABLE_FAILED_CHECKPOINTS * CHECKPOINT_INTERVAL_MS


def configure_checkpoints(env, interval_ms: int = CHECKPOINT_INTERVAL_MS) -> None:
    """Enable checkpointing on `env` with this project's settings."""
    # Imported here so the constants above stay importable without pyflink
    # (the project venv does not carry it; ADR 0006).
    from pyflink.datastream.checkpoint_config import ExternalizedCheckpointRetention

    env.enable_checkpointing(interval_ms)
    config = env.get_checkpoint_config()
    config.set_tolerable_checkpoint_failure_number(
        max(1, TOLERATED_FAILURE_SPAN_MS // interval_ms))
    # Retained on cancellation, so a cancelled job can be resumed by hand from
    # its last checkpoint (flink-submit.sh shows how). terraform/core/storage.tf
    # expires abandoned checkpoint directories after 7 days.
    config.set_externalized_checkpoint_retention(
        ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION)
