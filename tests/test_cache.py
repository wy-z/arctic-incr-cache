"""Tests for IncrCache."""

import datetime
import random
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import pytest

from arctic_incr_cache import IncrCache

_UTC = ZoneInfo("UTC")
_NY = ZoneInfo("America/New_York")

# ── helpers ──────────────────────────────────────────────────────


def _daily_df(start, n, value_start=100):
    dates = pd.date_range(start=start, periods=n, freq="D", tz=_UTC)
    return pd.DataFrame({"value": range(value_start, value_start + n)}, index=dates)


def _intraday_df(start, n):
    times = pd.date_range(start=start, periods=n, freq="1min", tz=_UTC)
    return pd.DataFrame({"price": range(n)}, index=times)


def _sessions(days, per_day):
    """*days* trading sessions of *per_day* minute bars, from 09:30."""
    return pd.concat(
        [
            _intraday_df(day + pd.Timedelta(hours=9, minutes=30), per_day)
            for day in pd.bdate_range("2024-01-01", periods=days, tz=_UTC)
        ]
    )


def _windowed_read(df):
    """A ``lib.read`` that honours ``date_range``, as ArcticDB does."""
    return lambda _symbol, date_range: MagicMock(
        data=df.loc[(df.index >= date_range[0]) & (df.index <= date_range[1])]
    )


def _span_days(df):
    """Calendar days in the frame's own inclusive span."""
    return (df.index[-1].date() - df.index[0].date()).days + 1


def _gapless(_, df):
    """Continuity hook for the daily fixtures: every calendar day in the
    frame's span must be present, no slack."""
    return len(df) < _span_days(df)


def _make_cache(lib, fetch_data=None, **kw):
    """Build an IncrCache with a mock library and canned fetch data.

    Continuity checks stay off unless a test passes ``is_holey`` —
    fixtures contain legitimate overnight/weekend gaps.
    """
    data = fetch_data if fetch_data is not None else pd.DataFrame()
    kw.setdefault("get_tz", lambda _: _UTC)
    return IncrCache(lib, MagicMock(return_value=data), cache_ttl=0, **kw)


# ── fixtures ─────────────────────────────────────────────────────


@pytest.fixture()
def lib():
    mock = MagicMock()
    mock.has_symbol.return_value = False
    return mock


# ── short window / source depth ──────────────────────────────────


class TestShortWindow:
    def test_fresh_but_short_fetches_more(self, lib):
        """When cache is fresh but has fewer rows than count, fetch to fill."""
        lib.has_symbol.return_value = True
        # Cache has 5 rows ending on the requested end date (fresh but short)
        lib.read.return_value.data = _daily_df("2024-01-11", 5)

        full = _daily_df("2024-01-01", 15)
        cache = _make_cache(lib, full)
        result = cache.get("S", end=datetime.date(2024, 1, 15), count=10)

        assert len(result) == 10
        cache._fetch.assert_called_once()  # type: ignore[union-attr]

    def test_floor_still_fills_stale_right_edge(self, lib):
        """A floor caps backfill on the left only — a stale right edge still
        takes the incremental path instead of another full re-fetch."""
        lib.has_symbol.return_value = True
        cached = _daily_df("2024-01-11", 5)  # ends Jan 15
        lib.read.return_value.data = cached

        cache = _make_cache(lib, cached)  # source stays short → floor set
        cache.get("S", end=datetime.date(2024, 1, 15), count=10)

        fetch_mock: MagicMock = cache._fetch  # type: ignore[assignment]
        fetch_mock.reset_mock()
        fetch_mock.return_value = _daily_df("2024-01-15", 6, value_start=500)
        cache.get("S", end=datetime.date(2024, 1, 20), count=10)

        _, _, call_count = fetch_mock.call_args[0]
        assert call_count == 6  # gap width, not the full 10-bar window

    def test_floor_backoff_caps_and_reprobes(self, lib):
        """Floor backoff never turns permanent: past the TTL ladder every
        expiry re-probes the source, so a floor poisoned by transient
        empty/short fetches heals instead of freezing the cache short."""
        lib.has_symbol.return_value = True
        cached = _daily_df("2024-01-11", 5)
        lib.read.return_value.data = cached

        cache = _make_cache(lib, cached)  # fetch stays short forever
        with patch("arctic_incr_cache.cache.time") as mock_time:
            t = 1_000_000.0
            for _ in range(5):  # ramps hits well past len(FLOOR_TTLS)
                mock_time.time.return_value = t
                cache.get("S", end=datetime.date(2024, 1, 15), count=10)
                t += IncrCache.FLOOR_TTLS[-1] + 1

        # Every expired floor re-probes — with a permanent floor the
        # calls after hit 3 would never fetch again.
        assert cache._fetch.call_count == 5  # type: ignore[union-attr]


# ── incremental update ───────────────────────────────────────────


class TestIncrementalUpdate:
    def test_daily_gap_across_dst_spring_forward(self, lib):
        """Gap calculation uses calendar days, not timedelta, to avoid DST errors."""
        # 2024-03-10 is US spring forward — midnight-to-midnight is only 23h
        dates = pd.date_range("2024-03-05", periods=5, freq="D", tz=_NY)
        cached = pd.DataFrame({"value": range(5)}, index=dates)  # ends Mar 9
        lib.has_symbol.return_value = True
        lib.read.return_value.data = cached

        # Source honours the overlap contract: last 4 bars include Mar 9
        # (the cached tail, unchanged) — a source that skipped it would
        # trigger the discontinuous-fetch gate instead.
        new_dates = pd.date_range("2024-03-09", periods=4, freq="D", tz=_NY)
        new = pd.DataFrame({"value": [4, 200, 201, 202]}, index=new_dates)
        cache = _make_cache(lib, new, get_tz=lambda _: _NY)
        cache.get("S", end=datetime.date(2024, 3, 12), count=5)

        fetch_mock: MagicMock = cache._fetch  # type: ignore[assignment]
        _, _, call_count = fetch_mock.call_args[0]
        assert call_count == 4  # Mar 9 (overlap) + Mar 10, 11, 12


# ── bar alignment ────────────────────────────────────────────────


class TestBarAlignment:
    """A clock change makes a wall-clock hour ambiguous or impossible, and
    flooring reads wall clock."""

    @pytest.mark.parametrize(
        ("fold", "expected"),
        [(0, "2024-11-03 01:00:00-04:00"), (1, "2024-11-03 01:00:00-05:00")],
    )
    def test_a_repeated_hour_keeps_its_side(self, lib, fold, expected):
        cache = _make_cache(lib, bar_minutes=60, get_tz=lambda _: _NY)
        end = datetime.datetime(2024, 11, 3, 1, 30, tzinfo=_NY, fold=fold)

        aligned = cache._align_bar(cache._resolve_end(end, _NY))

        assert str(aligned) == expected

    def test_a_boundary_in_a_repeated_hour_takes_the_later_instant(self, lib):
        """A 90-minute bar puts the boundary at 01:30, which happens twice,
        while the ask at 02:10 is past both.  Taking the first would cut an
        hour off a window the caller asked for."""
        cache = _make_cache(lib, bar_minutes=90, get_tz=lambda _: _NY)
        end = datetime.datetime(2024, 11, 3, 2, 10, tzinfo=_NY)

        aligned = cache._align_bar(cache._resolve_end(end, _NY))

        assert aligned == pd.Timestamp("2024-11-03 06:30", tz=_UTC)

    def test_an_offset_change_without_dst_keeps_its_side(self, lib):
        """Moscow left DST in 2014 by moving the offset, so both sides of the
        repeated hour report ``dst() == 0`` and a DST test cannot separate
        them.  The ask is the earlier side, and must stay there."""
        tz = ZoneInfo("Europe/Moscow")
        cache = _make_cache(lib, bar_minutes=60, get_tz=lambda _: tz)
        end = datetime.datetime(2014, 10, 26, 1, 30, tzinfo=tz, fold=0)

        aligned = cache._align_bar(cache._resolve_end(end, tz))

        assert str(aligned) == "2014-10-26 01:00:00+04:00"

    @pytest.mark.parametrize(
        ("zone", "day", "expected"),
        [
            # Midnight happens twice: the day's own bar carries the first
            # stamp, and the second is still that day.
            ("America/Havana", (2024, 11, 3), "2024-11-03 00:00:00-05:00"),
            # Midnight never happens: the day starts an hour late.
            ("America/Santiago", (2024, 9, 8), "2024-09-08 01:00:00-03:00"),
        ],
    )
    def test_a_daily_boundary_on_a_midnight_change(self, lib, zone, day, expected):
        tz = ZoneInfo(zone)
        cache = _make_cache(lib, get_tz=lambda _, t=tz: t)

        aligned = cache._align_bar(cache._resolve_end(datetime.date(*day), tz))

        assert str(aligned) == expected

    def test_a_skipped_boundary_never_aligns_past_the_ask(self, lib):
        """Lord Howe shifts by 30 minutes, so an hourly boundary lands inside
        the skipped half-hour and shifting it forward overshoots the ask."""
        tz = ZoneInfo("Australia/Lord_Howe")
        cache = _make_cache(lib, bar_minutes=60, get_tz=lambda _: tz)
        end = datetime.datetime(2024, 10, 6, 2, 31, tzinfo=tz)

        aligned = cache._align_bar(cache._resolve_end(end, tz))

        assert aligned == pd.Timestamp("2024-10-05 15:31", tz="UTC")

    def test_todays_bar_stays_incomplete_through_a_repeated_midnight(self, lib):
        """Havana's midnight happens twice.  Today's bar carries the first
        stamp, so a cutoff that follows the clock into the second hour would
        admit a bar that is still updating."""
        tz = ZoneInfo("America/Havana")
        first_midnight = pd.Timestamp("2024-11-03 04:00", tz="UTC").tz_convert(tz)
        second_hour = pd.Timestamp("2024-11-03 05:30", tz="UTC").tz_convert(tz)
        cache = _make_cache(lib, get_tz=lambda _: tz)

        with patch(
            "arctic_incr_cache.cache.pd.Timestamp.now",
            side_effect=lambda _tz: second_hour,
        ):
            assert cache._incomplete_threshold(tz) == first_midnight


# ── intraday ─────────────────────────────────────────────────────


class TestIntraday:
    def test_a_short_session_widens_the_read_instead_of_refetching(self, lib):
        """The read window is elapsed time and a session is a fraction of a
        day, so an ask spanning several sessions comes up short against a
        store holding every bar of it.  Without the widened re-read that is a
        full-window fetch on every read, forever."""
        sessions = _sessions(15, 240)
        lib.has_symbol.return_value = True
        lib.read.side_effect = _windowed_read(sessions)

        cache = _make_cache(lib, bar_minutes=1)
        result = cache.get("S", end=sessions.index[-1].to_pydatetime(), count=1950)

        assert len(result) == 1950
        cache._fetch.assert_not_called()  # type: ignore[union-attr]

    def test_a_failed_widening_keeps_the_rows_already_read(self, lib):
        """The widened read is speculative, and ``_read`` reports an error as
        an empty frame — which, taken at face value, turns a live cache into
        a miss."""
        sessions = _sessions(15, 240)
        windowed, reads = _windowed_read(sessions), []

        def flaky(symbol, date_range):
            reads.append(date_range)
            if len(reads) > 1:
                raise RuntimeError("segment unavailable")
            return windowed(symbol, date_range)

        lib.has_symbol.return_value = True
        lib.read.side_effect = flaky

        cache = _make_cache(lib, bar_minutes=1)
        result = cache.get("S", end=sessions.index[-1].to_pydatetime(), count=1950)

        assert len(reads) == 2  # the widening was attempted
        assert not result.empty  # and what the first read found still serves

    def test_gap_count_uses_minute_resolution(self, lib):
        cached = _intraday_df("2024-01-15 03:40", 500)  # ends at 11:59
        lib.has_symbol.return_value = True
        lib.read.return_value.data = cached

        new = _intraday_df("2024-01-15 11:59", 212)
        cache = _make_cache(lib, new, bar_minutes=1, default_count=1950)
        end = datetime.datetime(2024, 1, 15, 15, 30, tzinfo=_UTC)
        result = cache.get("S", end=end, count=500)

        assert not result.empty
        fetch_mock: MagicMock = cache._fetch  # type: ignore[assignment]
        _, _, call_count = fetch_mock.call_args[0]
        assert call_count == 212

    def test_end_none_floors_to_bar_boundary_for_freshness(self, lib):
        lib.has_symbol.return_value = True
        cached = pd.DataFrame(
            {"price": [1]},
            index=pd.DatetimeIndex([pd.Timestamp("2024-01-15 10:30", tz=_UTC)]),
        )
        lib.read.return_value.data = cached

        cache = _make_cache(lib, bar_minutes=1, default_count=390)
        now = pd.Timestamp("2024-01-15 10:31:05", tz=_UTC)

        def _now(tz=None):
            return now.tz_convert(tz) if tz else now.tz_localize(None)

        with patch("arctic_incr_cache.cache.pd.Timestamp.now", side_effect=_now):
            result = cache.get("S", end=None, count=1)

        assert len(result) == 1
        cache._fetch.assert_not_called()  # type: ignore[union-attr]

    def test_intraday_read_window_uses_calendar_days(self, lib):
        lib.has_symbol.return_value = True
        lib.read.return_value.data = pd.DataFrame()

        cache = _make_cache(lib, bar_minutes=1, default_count=390)
        end = datetime.datetime(2024, 1, 15, 9, 30, tzinfo=_UTC)
        cache.get("S", end=end, count=390)

        start_ts, end_ts = lib.read.call_args.kwargs["date_range"]
        assert end_ts == pd.Timestamp(end)
        assert start_ts <= pd.Timestamp("2024-01-11 09:30", tz=_UTC)


# ── normalize ────────────────────────────────────────────────────


class TestNormalize:
    def test_naive_raises(self):
        from arctic_incr_cache.cache import _normalize

        df = pd.DataFrame(
            {"v": [1]},
            index=pd.DatetimeIndex([pd.Timestamp("2024-01-15 12:00")]),
        )
        with pytest.raises(ValueError, match="tz-aware"):
            _normalize(df, tz=_NY)

    def test_non_datetime_index_raises(self):
        """A non-``DatetimeIndex`` carries no tz to check, so it must not
        slip past the gate — an object index of naive stamps is the case
        that would otherwise reach an ArcticDB write untouched."""
        from arctic_incr_cache.cache import _normalize

        with pytest.raises(ValueError, match="tz-aware"):
            _normalize(pd.DataFrame({"v": [1, 2]}), tz=_NY)

        naive_objects = pd.DataFrame(
            {"v": [1]},
            index=pd.Index([pd.Timestamp("2024-01-15 12:00")], dtype=object),
        )
        with pytest.raises(ValueError, match="tz-aware"):
            _normalize(naive_objects, tz=_NY)

    def test_aware_converted_to_target_tz(self):
        from arctic_incr_cache.cache import _normalize

        df = pd.DataFrame(
            {"v": [1]},
            index=pd.DatetimeIndex([pd.Timestamp("2024-01-15", tz="UTC")]),
        )
        result = _normalize(df, tz=_NY)
        assert str(pd.DatetimeIndex(result.index).tz) == "America/New_York"
        assert result.index[0] == pd.Timestamp("2024-01-14 19:00", tz=_NY)

    def test_deduplicates_keeping_last(self):
        from arctic_incr_cache.cache import _normalize

        dates = [datetime.date(2024, 1, i) for i in [1, 2, 2, 3]]
        df = pd.DataFrame(
            {"v": [10, 20, 25, 30]},
            index=pd.DatetimeIndex(dates).tz_localize(_UTC),
        )
        result = _normalize(df, tz=_UTC)
        assert not result.index.has_duplicates
        assert result.loc[pd.Timestamp("2024-01-02", tz=_UTC), "v"] == 25

    def test_empty_passthrough(self):
        from arctic_incr_cache.cache import _normalize

        result = _normalize(pd.DataFrame(), tz=_UTC)
        assert result.empty


# ── trim ─────────────────────────────────────────────────────────


class TestTrim:
    def test_limits_by_count(self):
        from arctic_incr_cache.cache import _trim

        df = _daily_df("2024-01-01", 20)
        result = _trim(df, pd.Timestamp("2024-01-20", tz=_UTC), 5)
        assert len(result) == 5
        assert result.index[-1] == pd.Timestamp("2024-01-20", tz=_UTC)

    def test_filters_by_end_ts(self):
        from arctic_incr_cache.cache import _trim

        df = _daily_df("2024-01-01", 20)
        result = _trim(df, pd.Timestamp("2024-01-10", tz=_UTC), 1000)
        assert len(result) == 10
        assert result.index[-1] == pd.Timestamp("2024-01-10", tz=_UTC)

    def test_empty_passthrough(self):
        from arctic_incr_cache.cache import _trim

        result = _trim(pd.DataFrame(), pd.Timestamp("2024-01-10"), 10)
        assert result.empty

    def test_the_result_does_not_pin_the_frame_it_came_from(self):
        """A tail is a view.  This one is what the result cache holds, so a
        hundred rows would keep the whole window they were cut from alive."""
        from arctic_incr_cache.cache import _trim

        window = _intraday_df("2024-01-01 09:30", 10_000)
        result = _trim(window, window.index[-1], 100)

        held = np.asarray(result["price"])
        assert held.base is None or held.base.nbytes <= held.nbytes


# ── end_ts ───────────────────────────────────────────────────────


class TestResolveEnd:
    def test_date_becomes_end_of_day(self):
        cache = _make_cache(MagicMock())
        result = cache._resolve_end(datetime.date(2024, 1, 15), tz=_UTC)
        assert result == pd.Timestamp("2024-01-15 23:59:59.999999", tz=_UTC)

    def test_date_end_takes_the_last_instant_of_a_stretched_day(self):
        """Cairo ends DST at midnight, so its last hour happens twice.  End of
        day is the second one — the first drops an hour of bars."""
        tz = ZoneInfo("Africa/Cairo")
        cache = _make_cache(MagicMock(), get_tz=lambda _: tz)

        end = cache._resolve_end(datetime.date(2024, 10, 31), tz)

        assert end == pd.Timestamp("2024-10-31 21:59:59.999999", tz=_UTC)

    def test_naive_datetime_localized_as_local(self):
        cache = _make_cache(MagicMock())
        naive = datetime.datetime(2024, 1, 15, 14, 30)
        result = cache._resolve_end(naive, tz=_UTC)
        expected = pd.Timestamp(naive.astimezone()).tz_convert(_UTC)
        assert result == expected

    def test_naive_pd_timestamp_localized_as_local(self):
        cache = _make_cache(MagicMock())
        naive_ts = pd.Timestamp("2024-01-15 14:30")
        result = cache._resolve_end(naive_ts, tz=_UTC)
        expected = pd.Timestamp(naive_ts.to_pydatetime().astimezone()).tz_convert(_UTC)
        assert result == expected

    def test_aware_datetime_converted(self):
        cache = _make_cache(MagicMock())
        aware = datetime.datetime(2024, 1, 15, 14, 30, tzinfo=_NY)
        result = cache._resolve_end(aware, tz=_UTC)
        assert result == pd.Timestamp("2024-01-15 14:30:00", tz=_NY).tz_convert(_UTC)

    def test_none_defaults_to_now(self):
        cache = _make_cache(MagicMock())
        result = cache._resolve_end(None, tz=_UTC)
        assert result.date() == pd.Timestamp.now(_UTC).date()


# ── write locking ────────────────────────────────────────────────


class TestWriteLock:
    def test_same_symbol_same_lock(self):
        cache = _make_cache(MagicMock())
        a = cache._lock_for("S1")
        b = cache._lock_for("S1")
        assert a is b

    def test_different_symbols_different_locks(self):
        cache = _make_cache(MagicMock())
        a = cache._lock_for("A")
        b = cache._lock_for("B")
        assert a is not b


# ── mid-series continuity ────────────────────────────────────────


def _holey_df():
    """10 daily bars spanning Jan 1–14 with Jan 5–8 missing."""
    return pd.concat(
        [
            _daily_df("2024-01-01", 4),
            _daily_df("2024-01-09", 6, value_start=300),
        ]
    )


class TestContinuity:
    """Delivered-window continuity: the ``is_holey`` hook."""

    def test_disabled_by_default(self, lib):
        """No ``is_holey`` → holey cache served as-is, no re-fetch."""
        lib.has_symbol.return_value = True
        lib.read.return_value.data = _holey_df()

        cache = _make_cache(lib)
        result = cache.get("S", end=datetime.date(2024, 1, 14), count=10)

        cache._fetch.assert_not_called()  # type: ignore[union-attr]
        assert len(result) == 10

    def test_short_frames_never_reach_the_hook(self):
        """Under 2 bars there is no interior to miss.  Hooks index
        ``df.index[0]`` on the strength of this, so it is a contract."""

        def boom(*_):
            raise AssertionError("hook called on a frame it was promised not to")

        cache = _make_cache(MagicMock(), is_holey=boom)

        assert not cache._has_hole("S", pd.DataFrame())
        assert not cache._has_hole("S", _daily_df("2024-01-01", 1))

    def test_slack_is_the_hooks_business(self, lib):
        """Same 4 missing days, a hook that tolerates 5 → written.

        The cache holds no tolerance of its own; the verdict is the hook's.
        """
        lib.has_symbol.return_value = True
        lib.read.return_value.data = _daily_df("2024-01-09", 6)  # short, clean

        cache = _make_cache(
            lib,
            _holey_df(),  # 4 days missing
            is_holey=lambda _, df: len(df) < _span_days(df) - 5,
            spawn=lambda fn: fn(),
        )
        cache.get("S", end=datetime.date(2024, 1, 14), count=10)

        lib.update.assert_called_once()  # tolerated, so stored


class TestWriteGuard:
    """A holey frame is never written.

    `update` replaces the frame's whole span, so writing one deletes cached
    rows in its gaps — and storing it would only have the next read fetch the
    window again.  The frame still reaches the caller: refusing a write never
    drops data.
    """

    def test_refuses_backfill_of_a_short_window(self, lib):
        lib.has_symbol.return_value = True
        lib.read.return_value.data = _daily_df("2024-01-09", 6)  # clean tail

        cache = _make_cache(lib, _holey_df(), is_holey=_gapless)
        result = cache.get("S", end=datetime.date(2024, 1, 14), count=10)

        cache._fetch.assert_called_once()  # type: ignore[union-attr]
        assert len(result) == 10  # source view still served
        lib.update.assert_not_called()

    def test_refuses_hole_outside_the_delivered_window(self, lib):
        """The guard checks the frame being written, not the window being
        served: a source over-returning holey history must not be stored just
        because the slice the caller asked for came back clean.
        """
        lib.has_symbol.return_value = True
        lib.read.return_value.data = pd.DataFrame()
        older = pd.concat(
            [
                _daily_df("2024-01-01", 2),
                _daily_df("2024-01-07", 2, value_start=200),  # Jan 3-6 missing
            ]
        )
        delivered = _daily_df("2024-01-09", 6, value_start=300)
        cache = _make_cache(lib, pd.concat([older, delivered]), is_holey=_gapless)
        result = cache.get("S", end=datetime.date(2024, 1, 14), count=6)

        assert len(result) == 6  # the delivered window itself is clean
        lib.update.assert_not_called()

    def test_a_refused_backfill_still_floors_on_depth(self, lib):
        """Four rows against a ten-bar ask is a statement about depth, and it
        stays true whether or not the write policy accepts the frame.

        This is also what terminates the branch: a refused write leaves the
        window short, so without the floor the next read would put the
        identical question to the source.  The floor's ladder still expires
        and re-probes, so a source that deepens is found again.
        """
        lib.has_symbol.return_value = True
        lib.read.return_value.data = _daily_df("2024-01-05", 6)  # Jan 5-10

        holey = pd.concat(
            [
                _daily_df("2024-01-05", 2),
                _daily_df("2024-01-09", 2, value_start=200),  # Jan 7-8 missing
            ]
        )
        cache = _make_cache(lib, holey, is_holey=_gapless)
        cache.get("S", end=datetime.date(2024, 1, 10), count=10)
        cache.get("S", end=datetime.date(2024, 1, 10), count=10)

        assert cache._floor  # depth recorded from the oldest bar offered
        cache._fetch.assert_called_once()  # type: ignore[union-attr]

    def test_the_bar_on_the_threshold_is_incomplete(self, lib):
        """The cutoff is strict: a bar landing exactly on the threshold may
        still be updating, so the write path drops it."""
        cache = _make_cache(lib, bar_minutes=1, spawn=lambda fn: fn())
        threshold = pd.Timestamp("2024-01-15 10:30", tz=_UTC)
        cache._incomplete_threshold = lambda *_: threshold  # type: ignore[method-assign]

        cache._store("S", _intraday_df("2024-01-15 10:28", 4))  # 10:28..10:31

        stored = lib.update.call_args[0][1]
        assert stored.index[-1] == pd.Timestamp("2024-01-15 10:29", tz=_UTC)


# ── invariant ────────────────────────────────────────────────────


def _days(offsets, value_start=100):
    """Daily frame at 2024-01-01 plus each offset."""
    base = pd.Timestamp("2024-01-01", tz=_UTC)
    idx = pd.DatetimeIndex([base + pd.Timedelta(days=d) for d in sorted(offsets)])
    return pd.DataFrame(
        {"value": range(value_start, value_start + len(idx))}, index=idx
    )


def _punch(days, rng):
    """Drop an interior run of days, opening a hole."""
    if len(days) < 4 or rng.random() < 0.4:
        return days
    start = rng.randrange(1, len(days) - 2)
    return days[:start] + days[start + rng.randrange(1, min(5, len(days) - start)) :]


class TestStoreInvariant:
    """No path may write a frame with a hole — `update` replaces its whole
    span, so doing so deletes cached rows.  Random shapes cover the branch
    interactions the targeted tests reach one at a time."""

    def test_no_holey_frame_is_ever_written(self):
        for seed in range(300):
            rng = random.Random(seed)
            cached = _punch(
                list(range(rng.randrange(0, 8), rng.randrange(10, 30))), rng
            )
            fetched = _punch(
                list(range(rng.randrange(0, 12), rng.randrange(12, 40))), rng
            )
            if rng.random() < 0.2:
                fetched = []  # upstream returns nothing
            elif rng.random() < 0.25 and len(fetched) > 3:
                fetched = fetched[3:]  # upstream skips the cached tail

            lib = MagicMock()
            lib.has_symbol.return_value = bool(cached)
            lib.read.return_value.data = _days(cached) if cached else pd.DataFrame()
            cache = _make_cache(
                lib,
                _days(fetched, value_start=900) if fetched else pd.DataFrame(),
                is_holey=_gapless,
                spawn=lambda fn: fn(),  # write synchronously so we can inspect
            )
            cache.get(
                "S",
                end=datetime.date(2024, 1, 1)
                + datetime.timedelta(rng.randrange(5, 45)),
                count=rng.randrange(3, 20),
            )

            for call in lib.update.call_args_list:
                stored = call[0][1]
                assert len(stored) < 2 or not _gapless(None, stored), (
                    f"seed {seed} wrote {stored.index[0].date()}.."
                    f"{stored.index[-1].date()} with {len(stored)} rows"
                )
