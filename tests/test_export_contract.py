"""Contract for publish/export.py: the marts -> site/data transforms (Phase 7, 7B).

Executable spec, written before the implementation, like the Phase 3-6 suites.

Every input is a plain dict shaped like the loader's rows, so nothing here needs
a warehouse. The numbers are small and hand-checkable on purpose.
"""

from datetime import date, datetime, timezone

import pytest

from publish.export import (
    PM_PEAK_HOURS,
    alerts_by_hour,
    closed_last,
    closed_window,
    days_by_type,
    feed_health_by_day,
    hotspots,
    kpis,
    percentile_from_histogram,
    prediction_curve,
    route_ranking,
    window_days,
)

pytestmark = [pytest.mark.contract]

THU, SAT, SUN = date(2026, 9, 24), date(2026, 9, 26), date(2026, 9, 27)
WEEK = window_days(date(2026, 9, 24), date(2026, 9, 30))


def alert(hour=8, route="7", gap=100.0, lat=47.6, lon=-122.3, stop="s1", name="Stop 1",
          day=THU):
    return {"local_date": day, "local_hour": hour, "route_short_name": route, "gap_ft": gap,
            "latitude": lat, "longitude": lon, "stop_id": stop, "stop_name": name}


# --- the window helpers ------------------------------------------------------

def test_the_study_week_is_five_weekdays_and_a_weekend():
    assert len(WEEK) == 7
    assert days_by_type(WEEK) == {"weekday": 5, "saturday": 1, "sunday": 1}


def test_a_backwards_window_is_refused():
    with pytest.raises(ValueError):
        window_days(date(2026, 9, 30), date(2026, 9, 24))


def test_the_last_closed_day_is_yesterday_in_pacific_time():
    # 05:00 UTC on the 27th is 22:00 PDT on the 26th, so the 26th is still
    # today and the last closed day is the 25th.
    assert closed_last(datetime(2026, 9, 27, 5, 0, tzinfo=timezone.utc)) == date(2026, 9, 25)
    # 08:00 UTC is 01:00 PDT on the 27th: the 26th has ended.
    assert closed_last(datetime(2026, 9, 27, 8, 0, tzinfo=timezone.utc)) == date(2026, 9, 26)
    assert closed_last(datetime(2026, 9, 28, 7, 30, tzinfo=timezone.utc)) == date(2026, 9, 27)


def test_the_study_week_is_clamped_to_the_days_that_have_finished():
    """Cut on 2026-09-27: three days have finished. Unclamped, those three days
    are divided by seven, so the page prints "7 days" over three days of data
    and 213.9 alerts a day where the answer is 498.7."""
    now = datetime(2026, 9, 27, 20, 0, tzinfo=timezone.utc)          # 13:00 PDT
    assert closed_window(date(2026, 9, 24), date(2026, 9, 30), now) == (
        date(2026, 9, 24), date(2026, 9, 26))
    # Once the week is over the clamp does nothing: the study window IS the cut.
    later = datetime(2026, 10, 2, 20, 0, tzinfo=timezone.utc)
    assert closed_window(date(2026, 9, 24), date(2026, 9, 30), later) == (
        date(2026, 9, 24), date(2026, 9, 30))


def test_a_window_that_has_not_started_is_refused():
    now = datetime(2026, 9, 27, 20, 0, tzinfo=timezone.utc)
    with pytest.raises(ValueError):
        closed_window(date(2026, 9, 29), date(2026, 9, 30), now)


# --- alerts_by_hour ----------------------------------------------------------

class TestAlertsByHour:
    def test_rates_per_day_not_counts(self):
        # 5 weekday alerts at 08:00 over 5 weekdays is 1.0 a day; 2 weekend
        # alerts at 08:00 over 2 weekend days is also 1.0.
        alerts = [alert(8, day=THU)] * 5 + [alert(8, day=SAT), alert(8, day=SUN)]
        out = alerts_by_hour(alerts, WEEK)
        assert out["weekday"][8] == 1.0 and out["weekend"][8] == 1.0
        assert len(out["weekday"]) == len(out["weekend"]) == 24

    def test_a_window_without_weekends_gives_zeros_not_an_error(self):
        out = alerts_by_hour([alert(8)], [THU])
        assert out["weekend"] == [0] * 24 or out["weekend"] == [0.0] * 24

    def test_two_decimals(self):
        out = alerts_by_hour([alert(8)], WEEK)   # 1 alert over 5 weekdays
        assert out["weekday"][8] == 0.2


# --- route_ranking -----------------------------------------------------------

class TestRouteRanking:
    def test_order_counts_rates_and_peak_share(self):
        alerts = [alert(17, "E Line", 50.0), alert(8, "E Line", 150.0), alert(9, "7", 10.0)]
        out = route_ranking(alerts, WEEK)
        assert [r["route"] for r in out] == ["E Line", "7"]
        e = out[0]
        assert e["alerts"] == 2 and e["per_day"] == round(2 / 7, 2)
        assert e["pm_peak_share"] == 0.5
        assert e["median_gap_ft"] == 100.0

    def test_ties_break_on_route_and_none_is_last_but_kept(self):
        out = route_ranking([alert(route=None), alert(route="B"), alert(route="A")], WEEK)
        assert [r["route"] for r in out] == ["A", "B", None]

    def test_all_gaps_missing_is_none(self):
        assert route_ranking([alert(gap=None)], WEEK)[0]["median_gap_ft"] is None

    def test_peak_is_phase_3s_three_bins(self):
        assert list(PM_PEAK_HOURS) == [16, 17, 18]
        out = route_ranking([alert(18), alert(19)], WEEK)
        assert out[0]["pm_peak_share"] == 0.5


# --- hotspots ----------------------------------------------------------------

class TestHotspots:
    def test_grouped_by_stop_at_the_mean_midpoint_lon_first(self):
        fc = hotspots([alert(lat=47.60, lon=-122.30), alert(lat=47.62, lon=-122.32),
                       alert(stop="s2", lat=47.7, lon=-122.2)])
        assert fc["type"] == "FeatureCollection"
        first = fc["features"][0]
        assert first["properties"]["stop_id"] == "s1"
        assert first["properties"]["alerts"] == 2
        lon, lat = first["geometry"]["coordinates"]
        assert (lon, lat) == (-122.31, 47.61)   # lon first: GeoJSON's order

    def test_unlocated_alerts_are_not_features(self):
        fc = hotspots([alert(lat=None, lon=None), alert()])
        assert sum(f["properties"]["alerts"] for f in fc["features"]) == 1

    def test_routes_top_three_and_peak_count(self):
        alerts = ([alert(17, "A")] * 3 + [alert(8, "B")] * 2 + [alert(8, "C"), alert(8, "D")])
        props = hotspots(alerts)["features"][0]["properties"]
        assert props["routes"][:2] == [["A", 3], ["B", 2]] and len(props["routes"]) == 3
        assert props["pm_peak_alerts"] == 3

    def test_stable_order(self):
        alerts = [alert(stop="b"), alert(stop="a"), alert(stop="c"), alert(stop="c")]
        ids = [f["properties"]["stop_id"] for f in hotspots(alerts)["features"]]
        assert ids == ["c", "a", "b"]


# --- percentile_from_histogram ------------------------------------------------

def percentile_cont(values, q):
    """Postgres's definition, over the expanded multiset: the reference."""
    s = sorted(values)
    pos = (len(s) - 1) * q
    lo = int(pos)
    hi = min(lo + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (pos - lo)


class TestPercentileFromHistogram:
    @pytest.mark.parametrize("q", [0.0, 0.5, 0.9, 1.0])
    def test_matches_percentile_cont_with_interpolation(self, q):
        counts = {10: 3, 20: 1, 45: 2, 100: 4}
        expanded = [v for v, n in counts.items() for _ in range(n)]
        assert percentile_from_histogram(counts, q) == pytest.approx(percentile_cont(expanded, q))

    def test_even_count_median_interpolates(self):
        assert percentile_from_histogram({1: 1, 2: 1}, 0.5) == 1.5

    def test_unsorted_input_is_handled(self):
        assert percentile_from_histogram({100: 1, 1: 1, 50: 1}, 0.5) == 50

    def test_empty_is_an_error(self):
        with pytest.raises(ValueError):
            percentile_from_histogram({}, 0.5)


# --- prediction_curve --------------------------------------------------------

class TestPredictionCurve:
    def rows(self):
        d1, d2 = date(2026, 9, 24), date(2026, 9, 25)
        hist = [
            {"observed_day": d1, "lead_bucket": "0-2m", "abs_error_s": 30, "predictions": 1},
            {"observed_day": d2, "lead_bucket": "0-2m", "abs_error_s": 60, "predictions": 1},
            {"observed_day": d1, "lead_bucket": "45-60m", "abs_error_s": 200, "predictions": 3},
        ]
        daily = [
            {"observed_day": d1, "lead_bucket": "0-2m", "predictions": 1, "sum_error_s": 30.0, "min_lead_time_s": 5.0},
            {"observed_day": d2, "lead_bucket": "0-2m", "predictions": 1, "sum_error_s": -60.0, "min_lead_time_s": 3.0},
            {"observed_day": d1, "lead_bucket": "45-60m", "predictions": 3, "sum_error_s": 600.0, "min_lead_time_s": 2700.0},
        ]
        return hist, daily

    def test_summed_across_days_in_bucket_order(self):
        out = prediction_curve(*self.rows())
        assert [p["lead_bucket"] for p in out] == ["0-2m", "45-60m"]
        short = out[0]
        assert short["predictions"] == 2
        assert short["median_abs_error_s"] == 45        # (30 + 60) / 2
        assert short["mean_error_s"] == -15             # (30 - 60) / 2

    def test_rounds_half_away_from_zero_like_postgres(self):
        # Median 112.5: Postgres round() gives 113; Python's round() gives 112.
        hist = [{"observed_day": THU, "lead_bucket": "b", "abs_error_s": v, "predictions": 1}
                for v in (112, 113)]
        daily = [{"observed_day": THU, "lead_bucket": "b", "predictions": 2,
                  "sum_error_s": -225.0, "min_lead_time_s": 1.0}]
        point = prediction_curve(hist, daily)[0]
        assert point["median_abs_error_s"] == 113
        assert point["mean_error_s"] == -113            # -112.5 rounds away from zero


# --- feed_health_by_day --------------------------------------------------------

class TestFeedHealthByDay:
    def minute(self, day, hh, silent=False, positions=100, lag=50):
        return {"minute_local": datetime(2026, 9, day, hh, 0), "positions": 0 if silent else positions,
                "is_silent": silent, "median_ingest_lag_s": None if silent else lag}

    def test_per_pacific_day(self):
        rows = [self.minute(24, 1), self.minute(24, 2, silent=True), self.minute(25, 1, lag=70),
                self.minute(24, 3, lag=60)]
        out = feed_health_by_day(rows)
        assert [d["date"] for d in out] == ["2026-09-24", "2026-09-25"]
        d = out[0]
        assert d["minutes"] == 3 and d["silent_minutes"] == 1
        assert d["live_share"] == round(2 / 3, 4)
        assert d["positions"] == 200
        assert d["median_ingest_lag_s"] == 55

    def test_a_day_with_no_live_minute_has_no_lag(self):
        assert feed_health_by_day([self.minute(24, 1, silent=True)])[0]["median_ingest_lag_s"] is None


# --- kpis ----------------------------------------------------------------------

def test_kpis():
    alerts = [alert(17), alert(8, lat=None, lon=None), alert(9), alert(10)]
    health = [{"date": "2026-09-24", "minutes": 1000, "silent_minutes": 0, "live_share": 1.0,
               "positions": 900, "median_ingest_lag_s": 50},
              {"date": "2026-09-25", "minutes": 400, "silent_minutes": 100, "live_share": 0.75,
               "positions": 100, "median_ingest_lag_s": 50}]
    curve = [{"lead_bucket": "past", "predictions": 1, "median_abs_error_s": 176},
             {"lead_bucket": "0-2m", "predictions": 10, "median_abs_error_s": 45},
             {"lead_bucket": "45-60m", "predictions": 5, "median_abs_error_s": 196}]
    k = kpis(alerts, health, curve, [THU, date(2026, 9, 25)])
    assert (k["first"], k["last"], k["days"]) == ("2026-09-24", "2026-09-25", 2)
    assert k["positions"] == 1000
    assert k["live_share"] == round(1300 / 1400, 3)        # weighted by minutes
    assert k["alerts"] == 4 and k["alerts_per_day"] == 2.0
    assert k["located_share"] == 0.75
    assert k["pm_peak_share"] == 0.25
    assert k["predictions"] == 16                          # every bucket, past included
    # The first FORWARD bucket, not "past", which sorts first.
    assert (k["median_abs_error_short_s"], k["median_abs_error_long_s"]) == (45, 196)
