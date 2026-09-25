"""A liveness file for the supervised consumers, and the healthcheck that reads it.

WHY A FILE. Compose's restart policy already answers "is the process running": a
container whose main process exits is restarted, and `docker ps` shows it. It does
not answer "is the process looping". A consumer wedged on a stuck socket, a
deadlock, or a retry loop that never returns is Up and green while moving nothing.

So each supervised consumer touches one file per loop iteration, and
docker-compose.yml declares a healthcheck that stats its age. Two things it does
NOT tell you, both worth stating before somebody trusts it too far:

  * Whether data is arriving. During the 2026-09-23 upstream stall this loop
    ticked normally for 82 minutes while Metro published nothing. That question
    belongs to dbt source freshness and mart_feed_health, which measure the data
    rather than the process.
  * Anything at all after a restart. The file lives in the container's /tmp, so a
    fresh container is unhealthy until its first iteration, which is what
    `start_period` exists to cover.

Shared from producer/ rather than duplicated, because
consumers/enrichment/run.py already imports producer.errors and producer.publish.
The cross-package dependency is pre-existing, so this adds no new coupling.
"""

from __future__ import annotations

import os
from pathlib import Path

# Overridable so a test can point it at a temporary directory, and so the writer
# and the compose healthcheck cannot disagree by accident.
HEARTBEAT_PATH = os.environ.get("HEARTBEAT_PATH", "/tmp/transit-heartbeat")


def beat() -> None:
    """Touch the liveness file. Never raises, because this must not stop a stream.

    Called once per loop iteration by producer/run.py and by
    consumers/enrichment/run.py, both of which iterate at least once a second.
    """
    try:
        Path(HEARTBEAT_PATH).touch()
    except OSError:
        # A missing or read-only /tmp is not a reason to stop moving data. The
        # healthcheck reports unhealthy, which is the visible half of the same
        # problem, and a log line here would repeat once a second.
        pass