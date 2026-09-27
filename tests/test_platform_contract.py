"""Phase 5B/5C contract: what terraform/ must declare, checked statically.

The executable spec for 5B (topics, bucket) and the static half of 5C (no
credential in any .tf). Nothing here needs the stack: it parses the HCL
with python-hcl2 and compares it against values MEASURED on the live stack on
2026-09-24, against the Flink jobs' parallelism, and against the Makefile and
compose file. That is why it can run in CI, where there is no broker.

What it cannot check is that the declarations match the LIVE objects. That is
`make tf-drift`, which exits 0 only when a plan against the running stack is
empty, and it is the exit criterion terraform/core/topics.tf states.
"""

from __future__ import annotations

import ast
import json
import re
from pathlib import Path

import hcl2
import pytest

from consumers.bunching import config as bunching_config
from consumers.prediction import config as prediction_config
from producer import feeds

pytestmark = [pytest.mark.contract]

ROOT = Path(__file__).resolve().parents[1]
TF = ROOT / "terraform"

# Measured with `rpk topic describe <t> -c`, keeping DYNAMIC_TOPIC_CONFIG rows
# only, on 2026-09-24. Adoption has to match these exactly or the first plan
# after import shows a change.
LIVE_TOPICS: dict[str, tuple[int, dict[str, str]]] = {
    "raw.vehicle_positions": (3, {"cleanup.policy": "delete", "retention.ms": "604800000"}),
    "raw.trip_updates": (6, {"cleanup.policy": "delete", "retention.ms": "259200000"}),
    "raw.service_alerts": (1, {"cleanup.policy": "compact",
                               "min.cleanable.dirty.ratio": "0.1"}),
    "enriched.vehicle_positions": (3, {"retention.ms": "604800000"}),
    "alerts.bunching": (1, {"retention.ms": "2592000000"}),
    "analytics.prediction_accuracy": (3, {"retention.ms": "2592000000"}),
    "_connect_configs": (1, {"cleanup.policy": "compact"}),
    "_connect_offsets": (1, {"cleanup.policy": "compact"}),
    "_connect_status": (1, {"cleanup.policy": "compact"}),
    "dlq.vehicle_positions": (1, {}),
    "dlq.trip_updates": (1, {}),
    "dlq.service_alerts": (1, {}),
}


# --- parsing helpers ---------------------------------------------------------
#
# python-hcl2 8.x keeps the quotes on string values and keys ('"raw.x"'),
# returns expressions as "${...}" strings, and adds __is_block__ and
# __comments__ keys of its own. These undo the first, leave the second alone
# and skip the third, which is enough for everything asserted below.

def _unquote(v):
    if isinstance(v, str) and len(v) >= 2 and v[0] == v[-1] == '"':
        return v[1:-1]
    return v


def _load(root: str) -> dict[str, list]:
    """Every top-level block across one root's .tf files, merged by kind."""
    merged: dict[str, list] = {}
    for path in sorted((TF / root).glob("*.tf")):
        with path.open() as fh:
            for kind, blocks in hcl2.load(fh).items():
                merged.setdefault(kind, []).extend(blocks)
    return merged


def _resources(root: str, rtype: str) -> dict[str, dict]:
    """{resource_name: body} for one resource type."""
    out = {}
    for block in _load(root).get("resource", []):
        for t, named in block.items():
            if _unquote(t) != rtype:
                continue
            for name, body in named.items():
                out[_unquote(name)] = body
    return out


# The replay namespace (terraform/core/replay.tf) is one for_each resource named
# `replay`, whose `name` is an expression rather than a literal. It is the live
# platform's scratch space, not part of it, so it is excluded from the live-topic
# checks below and has its own class at the end of this file.
REPLAY_RESOURCE = "replay"


def _topics() -> dict[str, dict]:
    """{topic name: body} for the LIVE topics, keyed by the name attribute."""
    return {_unquote(body["name"]): body
            for name, body in _resources("core", "kafka_topic").items()
            if name != REPLAY_RESOURCE}


def _replay_topics() -> dict[str, dict]:
    """{replay topic: {partitions, mirrors}} from replay.tf's local map."""
    for block in _load("core").get("locals", []):
        if "replay_topics" in block:
            return {_unquote(k): {kk: _unquote(vv) for kk, vv in v.items()
                                  if not kk.startswith("__")}
                    for k, v in block["replay_topics"].items()
                    if not k.startswith("__")}
    return {}


def _prevents_destroy(body: dict) -> bool:
    return any(lc.get("prevent_destroy") is True for lc in body.get("lifecycle", []))


def _parallelism(job: Path) -> int:
    """The literal in env.set_parallelism(N), read from source.

    Not imported: the job modules import pyflink, which the project venv
    deliberately does not have (ADR 0006).
    """
    for node in ast.walk(ast.parse(job.read_text())):
        if (isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "set_parallelism"):
            return ast.literal_eval(node.args[0])
    raise AssertionError(f"no set_parallelism call in {job}")


def _resource_name(topic: str) -> str:
    return re.sub(r"[.]", "_", topic).lstrip("_")


# =============================================================================
# 1. Pinning
# =============================================================================

class TestPinning:
    @pytest.mark.parametrize("root", ["core", "connectors"])
    def test_terraform_version_matches_the_makefile_image(self, root):
        """The Makefile runs hashicorp/terraform:<tag>. A root that asks for
        a different version fails init with a message about the CLI, which is
        confusing on a machine where nobody installed a CLI."""
        tag = re.search(r"TF_IMAGE := hashicorp/terraform:(\S+)",
                        (ROOT / "Makefile").read_text()).group(1)
        versions = [b.get("required_version") for b in _load(root)["terraform"]]
        assert f'"= {tag}"' in versions or f"= {tag}" in map(_unquote, versions)

    @pytest.mark.parametrize("root", ["core", "connectors"])
    def test_providers_are_pinned_exactly(self, root):
        for block in _load(root)["terraform"]:
            for providers in block.get("required_providers", []):
                for name, spec in providers.items():
                    if name.startswith("__"):   # __is_block__, __comments__
                        continue
                    v = _unquote(spec["version"])
                    assert re.fullmatch(r"\d+\.\d+\.\d+", v), (
                        f"{root}: provider {name} pinned as {v!r}; an exact "
                        "version, so an import reads back the same way next month")

    @pytest.mark.parametrize("root", ["core", "connectors"])
    def test_lock_file_is_committed_next_to_the_root(self, root):
        assert (TF / root / ".terraform.lock.hcl").exists(), (
            f"terraform/{root}/.terraform.lock.hcl missing: run `make tf-init "
            f"R={root}` and commit it")


# =============================================================================
# 2. Topics (5B)
# =============================================================================

class TestTopics:
    def test_every_live_topic_is_declared(self):
        missing = sorted(set(LIVE_TOPICS) - set(_topics()))
        assert not missing, f"topics not in terraform/core: {missing}"

    def test_no_topic_outside_the_measured_set(self):
        """Including _schemas, which Redpanda's registry owns (topics.tf, 5)."""
        extra = sorted(set(_topics()) - set(LIVE_TOPICS))
        assert not extra, f"declared but not part of the platform: {extra}"

    @pytest.mark.parametrize("topic", sorted(LIVE_TOPICS))
    def test_partitions_and_config_match_what_is_live(self, topic):
        body = _topics().get(topic)
        if body is None:
            pytest.fail(f"{topic} not declared")
        partitions, config = LIVE_TOPICS[topic]
        assert body["partitions"] == partitions
        assert body["replication_factor"] == 1
        declared = {_unquote(k): _unquote(v)
                    for k, v in body.get("config", {}).items()}
        assert declared == config, (
            f"{topic}: declared {declared}, live {config}. Anything else is a "
            "change on the first plan after adoption")

    @pytest.mark.parametrize("topic", sorted(LIVE_TOPICS))
    def test_resource_is_named_after_the_topic(self, topic):
        names = {_unquote(b["name"]): n
                 for n, b in _resources("core", "kafka_topic").items()}
        if topic in names:
            assert names[topic] == _resource_name(topic)

    @pytest.mark.parametrize("topic", sorted(LIVE_TOPICS))
    def test_every_topic_refuses_destroy(self, topic):
        """A partition decrease plans as `forces replacement`, measured, which
        is delete-and-recreate. This is what turns that plan into an error."""
        body = _topics().get(topic)
        if body is None:
            pytest.fail(f"{topic} not declared")
        assert _prevents_destroy(body)

    def test_every_topic_can_be_adopted_and_adoption_is_gated(self):
        """Each topic resource is the target of an import block, and every
        import block is conditional on adopt_existing. Ungated, an import of
        an object a fresh stack does not have is a hard error (measured:
        `Cannot import non-existent remote object`)."""
        imports = _load("core").get("import", [])
        targets = set()
        for block in imports:
            assert "adopt_existing" in str(block.get("for_each", "")), (
                f"import to {block.get('to')} is not gated on var.adopt_existing")
            targets.add(str(block["to"]))
        for name in _resources("core", "kafka_topic"):
            if name == REPLAY_RESOURCE:
                # Nothing to adopt: the replay namespace was created by
                # Terraform, never by hand.
                continue
            assert any(f"kafka_topic.{name}" in t for t in targets), (
                f"kafka_topic.{name} has no import block")

    def test_bunching_parallelism_equals_its_source_partitions(self):
        """The Phase 3 late-drop bug: one subtask over two interleaved
        partitions shares a watermark and drops 29.75% as late. Raising the
        partition count is an in-place update in Terraform (measured), so
        nothing else would catch the job being left behind."""
        partitions = _topics()[bunching_config.SOURCE_TOPIC]["partitions"]
        assert _parallelism(ROOT / "consumers/bunching/job.py") == partitions

    def test_prediction_parallelism_equals_its_wider_source(self):
        """prediction/job.py takes the MAX of its two sources' partition
        counts: wider than a topic is harmless, narrower is the bug above."""
        t = _topics()
        wider = max(t[prediction_config.PREDICTION_TOPIC]["partitions"],
                    t[prediction_config.OBSERVATION_TOPIC]["partitions"])
        assert _parallelism(ROOT / "consumers/prediction/job.py") == wider

    def test_every_topic_the_code_names_is_declared(self):
        """The topics the code writes and reads, from the code's own constants.
        A feed added to producer/feeds.py without a DLQ topic here would have
        its first DLQ record auto-create one with broker defaults."""
        named = {
            bunching_config.SOURCE_TOPIC, bunching_config.SINK_TOPIC,
            prediction_config.PREDICTION_TOPIC, prediction_config.OBSERVATION_TOPIC,
            prediction_config.SINK_TOPIC,
        }
        for name, spec in feeds.FEEDS.items():
            named |= {spec.topic, f"dlq.{name}"}
        missing = sorted(named - set(_topics()))
        assert not missing, f"used by the code, not declared: {missing}"

    def test_make_no_longer_creates_topics(self):
        """Two places that set topic config is how a topic loses its retention
        without anything noticing."""
        assert "rpk topic create" not in (ROOT / "Makefile").read_text()


# =============================================================================
# 3. Storage (5B)
# =============================================================================

class TestStorage:
    def test_bucket_refuses_destroy(self):
        buckets = _resources("core", "minio_s3_bucket")
        assert buckets, "no minio_s3_bucket"
        assert all(_prevents_destroy(b) for b in buckets.values())

    def test_abandoned_checkpoints_expire(self):
        policies = _resources("core", "minio_ilm_policy")
        assert policies, "no minio_ilm_policy: flink-checkpoints/ grows forever"
        text = json.dumps(policies)
        assert "flink-checkpoints/" in text

    def test_nothing_expires_the_raw_archive(self):
        """raw/ is Phase 6's replay source (storage.tf, 3)."""
        text = json.dumps(_resources("core", "minio_ilm_policy"))
        assert '"raw/' not in text and "'raw/" not in text

    def test_compose_no_longer_creates_the_bucket(self):
        assert "mc mb" not in (ROOT / "docker-compose.yml").read_text()


# =============================================================================
# 4. Credentials
# =============================================================================

def _all_blocks():
    for root in ("core", "connectors"):
        for kind, blocks in _load(root).items():
            for block in blocks:
                yield root, kind, block


def _walk(obj, path=()):
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k.startswith("__"):   # the parser's __comments__, __is_block__
                continue
            yield from _walk(v, path + (_unquote(k),))
    elif isinstance(obj, list):
        for v in obj:
            yield from _walk(v, path)
    else:
        yield path, obj


class TestCredentials:
    def test_every_credential_variable_is_ephemeral(self):
        """sensitive alone only hides a value from the terminal; the state
        still stores it in plain text."""
        for root in ("core", "connectors"):
            for block in _load(root).get("variable", []):
                for name, body in block.items():
                    name = _unquote(name)
                    if re.search(r"password|secret", name):
                        assert body.get("sensitive") is True, name
                        assert body.get("ephemeral") is True, name

    def test_no_resource_sets_a_stored_password(self):
        """password = / secret = on a resource lands in terraform.tfstate.
        password_wo / secret_wo do not (measured: zero occurrences)."""
        for root, kind, block in _all_blocks():
            if kind != "resource":
                continue
            for path, value in _walk(block):
                leaf = path[-1] if path else ""
                assert leaf not in ("password", "secret"), (
                    f"{root}: {'.'.join(path)} is stored in state; use {leaf}_wo")

    def test_no_literal_credential_anywhere(self):
        """Every password-ish value is a reference, never a string."""
        for root, _kind, block in _all_blocks():
            for path, value in _walk(block):
                leaf = path[-1] if path else ""
                if re.search(r"password|secret", str(leaf)) and isinstance(value, str):
                    v = _unquote(value)
                    assert v.startswith("${") or leaf.endswith("_version"), (
                        f"{root}: {'.'.join(path)} is a literal")

    def test_connector_configs_carry_placeholders_not_passwords(self):
        """The reason the connectors root's state holds no credential."""
        for f in (ROOT / "connect").glob("*.json"):
            cfg = json.loads(f.read_text())
            assert cfg["connection.password"].startswith("${env:"), f.name


# =============================================================================
# 5. The replay namespace (Phase 6)
# =============================================================================

class TestReplayNamespace:
    """terraform/core/replay.tf, the namespace the replay actually runs in.

    The rules are about isolation and fidelity, the two things that make a
    replay worth believing: nothing in the namespace can be a live name, and
    every replay topic partitions exactly like the live topic it stands in for,
    because the bunching job's parallelism is pinned to that count.
    """

    def test_namespace_exists(self):
        assert _replay_topics(), "no replay_topics local in terraform/core/replay.tf"

    def test_every_replay_topic_is_namespaced(self):
        for topic in _replay_topics():
            assert topic.startswith("replay."), topic
            assert topic not in LIVE_TOPICS, f"{topic} is a live topic name"

    def test_every_replay_topic_mirrors_a_live_topic(self):
        for topic, spec in _replay_topics().items():
            assert spec["mirrors"] in LIVE_TOPICS, (
                f"{topic} mirrors {spec['mirrors']!r}, which is not a live topic")

    def test_partitions_match_the_live_topic_they_stand_in_for(self):
        """A replay topic partitioned differently from its live counterpart
        makes the replayed job drop (or keep) late data the live job did not,
        and the fidelity check would then blame the logic."""
        for topic, spec in _replay_topics().items():
            live_partitions = LIVE_TOPICS[spec["mirrors"]][0]
            assert int(spec["partitions"]) == live_partitions, (
                f"{topic}: {spec['partitions']} partitions, "
                f"{spec['mirrors']} has {live_partitions}")

    def test_the_replayed_source_is_there_for_the_bunching_job(self):
        """The replayed detector reads replay.enriched.vehicle_positions with
        the live job's parallelism."""
        replay = _replay_topics()
        assert "replay.enriched.vehicle_positions" in replay
        assert int(replay["replay.enriched.vehicle_positions"]["partitions"]) == \
            _parallelism(ROOT / "consumers/bunching/job.py")
