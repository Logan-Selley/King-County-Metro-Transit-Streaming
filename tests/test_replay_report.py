"""The classifications behind docs/findings.md section 13 (replay/report.py).

The two findings the write-up leans on are decided by these functions: which
gate-off alerts are terminal staging, and which gate-on alerts vanish because a
terminal alert's cooldown swallowed them. Pinned here so a change to either
rule shows up as a failing test rather than a quietly different JSON.
"""

import pytest

from replay.report import COOLDOWN_S, LIVE_GATE, by_local_hour, cause_of_added, cause_of_removed, keyed, pair_times

pytestmark = [pytest.mark.contract]

T = 1_790_262_000  # 2026-09-24 15:00:00Z = 08:00 PDT


def alert(a, b, t, route="7"):
    return {"vehicle_id_a": a, "vehicle_id_b": b, "window_end": t, "route_short_name": route,
            "route_id": "r", "direction_id": 0, "gap_ft": 100.0}


class TestCauseOfAdded:
    def test_a_vehicle_before_the_gate_is_terminal(self):
        assert cause_of_added(alert("1", "2", T), LIVE_GATE - 1, {}) == "terminal"

    def test_past_the_gate_and_the_pair_alerts_nearby_in_the_baseline_is_shifted(self):
        base = pair_times([alert("2", "1", T + COOLDOWN_S)])  # listed the other way round
        assert cause_of_added(alert("1", "2", T), LIVE_GATE, base) == "shifted"

    def test_past_the_gate_with_nothing_nearby_is_unexplained(self):
        base = pair_times([alert("1", "2", T + COOLDOWN_S + 60)])
        assert cause_of_added(alert("1", "2", T), LIVE_GATE, base) == "unexplained"

    def test_unknown_stop_sequence_is_not_called_terminal(self):
        assert cause_of_added(alert("1", "2", T), None, {}) == "unexplained"


class TestCauseOfRemoved:
    def test_suppressed_by_a_gate_only_alert_in_the_cooldown_before(self):
        terminal = alert("1", "2", T - 300)
        variant = keyed([terminal])
        added = set(variant)
        assert cause_of_removed(alert("1", "2", T), variant, added) == "suppressed"

    def test_a_prior_alert_that_is_in_both_runs_is_not_suppression(self):
        variant = keyed([alert("1", "2", T - 300)])
        assert cause_of_removed(alert("1", "2", T), variant, set()) == "prior in both"

    def test_outside_the_cooldown_does_not_count(self):
        early = alert("1", "2", T - COOLDOWN_S - 60)
        variant = keyed([early])
        assert cause_of_removed(alert("1", "2", T), variant, set(variant)) == "no prior"


def test_hours_are_pacific_and_all_24_present():
    hours = by_local_hour([alert("1", "2", T)])
    assert len(hours) == 24 and hours[8] == 1 and sum(hours.values()) == 1
