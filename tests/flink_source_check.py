#!/usr/bin/env python
"""Build the detector's Kafka source in the PyFlink image, both ways, and assert
each one's boundedness.

NOT a pytest test, for the same reason tests/sink_roundtrip.py is not one: it
needs PyFlink, which is not installed in the project venv. It is
`make bunching-source-check`.

WHY THIS EXISTS. KafkaSource.Builder.set_bounded takes a KafkaOffsetsInitializer,
the offsets to STOP at, not a bool. The job passed `SETTINGS.bounded`, so
set_bounded(False) raised AttributeError on `_j_initializer`, and it did so for
the LIVE run too: a JobManager restart, `make resume` or a reboot would have
resubmitted that code and stopped bunching alerts. The contract suite cannot see
it, because nothing under tests/ builds a source. So this builds both, in the
image that runs them, and checks the two boundednesses the two runs need.

Exit status is the whole point: 0 only when both sources build and each has the
boundedness its run needs.
"""

from __future__ import annotations

import importlib
import os
import sys


def boundedness(replay: str | None) -> str:
    """kafka_source()'s Java boundedness with BUNCHING_REPLAY set to `replay`."""
    if replay is None:
        os.environ.pop("BUNCHING_REPLAY", None)
    else:
        os.environ["BUNCHING_REPLAY"] = replay
    # Imported and reloaded per run: the module resolves SETTINGS from the
    # environment at import time, which is exactly what makes one process able to
    # check both runs.
    import consumers.bunching.job as job

    importlib.reload(job)
    # getBoundedness() is the JAVA method: the Python wrapper does not re-export
    # it (dir(KafkaSource) is just builder and get_java_function), and py4j
    # renders the enum as CONTINUOUS_UNBOUNDED or BOUNDED.
    return str(job.kafka_source().get_java_function().getBoundedness())


def failure_handling(replay: str | None) -> str:
    """build_env()'s checkpointing and restart strategy, for the same run.

    Called after boundedness(), which set the environment and reloaded the job.
    The replay must FAIL FAST (no checkpoints, restart strategy none): on
    2026-09-27 a replay under the default restart-on-failure hung for 5 h 22 min
    after its Python worker died on the restore path. The live job must keep
    its checkpoints and the default restarts.
    """
    import consumers.bunching.job as job
    from pyflink.java_gateway import get_gateway

    env = job.build_env()
    option = get_gateway().jvm.org.apache.flink.configuration.RestartStrategyOptions.RESTART_STRATEGY
    restart = env._j_stream_execution_environment.getConfiguration().getOptional(option).orElse("default")
    checkpoints = "checkpoints" if env.get_checkpoint_config().is_checkpointing_enabled() else "no-checkpoints"
    return f"{checkpoints}/restart={restart}"


def main() -> int:
    wanted = ((None, "CONTINUOUS_UNBOUNDED", "checkpoints/restart=default"),
              ("baseline", "BOUNDED", "no-checkpoints/restart=none"))
    wrong = []
    for replay, expected, expected_env in wanted:
        label = "live" if replay is None else f"BUNCHING_REPLAY={replay}"
        got = boundedness(replay)
        got_env = failure_handling(replay)
        ok = got == expected and got_env == expected_env
        print(f"  {label:<24} {got:<20} {got_env:<28} "
              f"{'ok' if ok else 'EXPECTED ' + expected + ' ' + expected_env}")
        if not ok:
            wrong.append(label)
    if wrong:
        print(f"FAIL: wrong source for {', '.join(wrong)}", file=sys.stderr)
        return 1
    print("both build: live CONTINUOUS_UNBOUNDED with checkpoints, "
          "replay BOUNDED failing fast")
    return 0


if __name__ == "__main__":
    sys.exit(main())
