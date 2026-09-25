"""Phase 5D contract: health checks and consumer-lag monitoring, checked statically.

The runtime half is `make airflow-check` (transit_health must have a
consumer_lag task) and the task itself running green for a day. This half
pins the things that drift silently: which services report health, and which
consumer groups the lag check watches.

5D landed 2026-09-24, so this is no longer `wip`: four services declare
healthchecks, flink-submit waits on a healthy JobManager, and the DAG watches
exactly the groups the pipeline runs.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest
import yaml

from consumers.bunching import config as bunching_config
from consumers.prediction import config as prediction_config

pytestmark = [pytest.mark.contract]

ROOT = Path(__file__).resolve().parents[1]


def _compose() -> dict:
    return yaml.safe_load((ROOT / "docker-compose.yml").read_text())["services"]


def _module_constant(path: Path, name: str):
    """A module-level literal, read without importing the module. The DAG
    imports airflow and the enrichment consumer imports confluent_kafka's
    registry client; neither import is needed to read a constant."""
    for node in ast.parse(path.read_text()).body:
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            if any(isinstance(t, ast.Name) and t.id == name for t in targets):
                return ast.literal_eval(node.value)
    raise AssertionError(f"{name} not defined at module level in {path.name}")


# =============================================================================
# Health checks
# =============================================================================

# connect is absent on purpose: cp-kafka-connect's image carries its own
# HEALTHCHECK (/etc/confluent/docker/healthcheck.sh, measured with
# `docker inspect`), which compose inherits without declaring it.
NEEDS_HEALTHCHECK = ("producer", "enrichment", "flink-jobmanager", "flink-taskmanager")


class TestHealthchecks:
    @pytest.mark.parametrize("service", NEEDS_HEALTHCHECK)
    def test_long_running_service_reports_health(self, service):
        assert "healthcheck" in _compose()[service], (
            f"{service} has restart: unless-stopped and no healthcheck, so "
            "`docker ps` shows it Up while it does nothing")

    def test_submitter_waits_for_a_healthy_jobmanager(self):
        """flink-submit.sh polls the REST API itself today. With a
        jobmanager healthcheck, compose can do the waiting, and the script's
        loop becomes the fallback rather than the mechanism."""
        dep = _compose()["flink-submit"]["depends_on"]["flink-jobmanager"]
        assert dep["condition"] == "service_healthy"


# =============================================================================
# Consumer lag
# =============================================================================

def _production_groups() -> set[str]:
    """Every consumer group the pipeline actually runs, from the code's own
    names. NOT from `rpk group list`: that lists twelve abandoned diagnostic
    groups too (scan-*, probe-era2, strcheck, ...), each ~3.2M behind, which
    is exactly why the lag check needs an explicit list."""
    groups = {
        bunching_config.CONSUMER_GROUP,
        f"{prediction_config.CONSUMER_GROUP}-pred",
        f"{prediction_config.CONSUMER_GROUP}-obs",
        _module_constant(ROOT / "consumers" / "enrichment" / "run.py", "GROUP"),
    }
    # One group per sink, named connect-<connector> by the worker.
    groups |= {f"connect-{f.stem}" for f in (ROOT / "connect").glob("*.json")}
    return groups


class TestConsumerLag:
    DAG = ROOT / "airflow" / "dags" / "transit_health.py"

    def test_the_dag_watches_exactly_the_production_groups(self):
        """A connector added to connect/ without a threshold here would be
        the one sink nobody watches. A diagnostic group added here would be
        red from its first run."""
        watched = _module_constant(self.DAG, "LAG_THRESHOLDS")
        assert set(watched) == _production_groups()

    def test_every_threshold_is_a_positive_record_count(self):
        """Per group, because the groups do not lag alike: the Flink groups
        commit only on checkpoint (every 30s), so prediction-accuracy-pred
        sat at 1,189 while fully caught up, and connect-* sat at 0."""
        for group, limit in _module_constant(self.DAG, "LAG_THRESHOLDS").items():
            assert isinstance(limit, int) and limit > 0, group

    def test_dag_spec_requires_the_task(self):
        required = _module_constant(ROOT / "airflow" / "check_dags.py", "REQUIRED")
        assert "consumer_lag" in required["transit_health"]
