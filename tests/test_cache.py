"""Tests for IncrCache."""

import datetime
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

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


def _make_cache(
    lib,
    fetch_data=None,
    *,
    bar_minutes: int = 1440,
    default_count: int = 252,
    get_tz=lambda _: _UTC,
    min_bars_per_day: int | None = None,
):
    """Build an IncrCache with a mock library and canned fetch data."""
    data = fetch_data if fetch_data is not None else pd.DataFrame()
    fetch = MagicMock(return_value=data)
    return IncrCache(
        lib,
        fetch,
        get_tz=get_tz,
        bar_minutes=bar_minutes,
        default_count=default_count,
        cache_ttl=0,
        min_bars_per_day=min_bars_per_day,
    )


# ── fixtures ─────────────────────────────────────────────────────


@pytest.fixture()
def lib():
    mock = MagicMock()
    mock.has_symbol.return_value = False
    return mock


# ── cache miss ───────────────────────────────────────────────────


class TestCacheMiss:
    def test_fetches_and_stores(self, lib):
        cache = _make_cache(lib, _daily_df("2024-01-01", 15))
        result = cache.get("S", end=datetime.date(2024, 1, 15), count=10)

        assert len(result) == 10
        lib.update.assert_called_once()

    def test_empty_source(self, lib):
        cache = _make_cache(lib)
        result = cache.get("S", end=datetime.date(2024, 1, 15), count=10)

        assert result.empty
        lib.update.assert_not_called()


# ── cache hit ────────────────────────────────────────────────────


class TestCacheHit:
    def test_fresh_cache_skips_fetch(self, lib):
        lib.has_symbol.return_value = True
        lib.read.return_value.data = _daily_df("2024-01-01", 20)

        cache = _make_cache(lib)
        result = cache.get("S", end=datetime.date(2024, 1, 15), count=10)

        assert len(result) == 10

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

    def test_short_floor_suppresses_refetch(self, lib):
        """When source has no more data, record oldest date and skip re-fetch."""
        lib.has_symbol.return_value = True
        # Cache has 5 rows — source also only has 5 rows (can't fill to 10)
        cached = _daily_df("2024-01-11", 5)
        lib.read.return_value.data = cached

        cache = _make_cache(lib, cached)  # fetch returns same data
        cache.get("S", end=datetime.date(2024, 1, 15), count=10)
        cache.get("S", end=datetime.date(2024, 1, 15), count=10)

        # Should only fetch once — second call sees floor covers existing
        cache._fetch.assert_called_once()  # type: ignore[union-attr]


# ── incremental update ───────────────────────────────────────────


class TestIncrementalUpdate:
    def test_merges_new_data(self, lib):
        cached = _daily_df("2024-01-01", 10)
        lib.has_symbol.return_value = True
        lib.read.return_value.data = cached

        new = _daily_df("2024-01-11", 10, value_start=200)
        cache = _make_cache(lib, new)
        result = cache.get("S", end=datetime.date(2024, 1, 20), count=10)

        assert len(result) == 10
        lib.update.assert_called_once()
        fetch_mock: MagicMock = cache._fetch  # type: ignore[assignment]
        _, _, call_count = fetch_mock.call_args[0]
        assert call_count == 10

    def test_deduplicates_unchanged_overlap(self, lib):
        cached = _daily_df("2024-01-01", 10)
        lib.has_symbol.return_value = True
        lib.read.return_value.data = cached

        overlap = cached.iloc[[-1]]  # Jan 10, same value
        new_part = _daily_df("2024-01-11", 5, value_start=500)
        cache = _make_cache(lib, pd.concat([overlap, new_part]))
        cache.get("S", end=datetime.date(2024, 1, 20), count=10)

        stored = lib.update.call_args[0][1]
        assert pd.Timestamp("2024-01-10", tz=_UTC) not in stored.index

    def test_keeps_changed_overlap(self, lib):
        cached = _daily_df("2024-01-01", 10)
        lib.has_symbol.return_value = True
        lib.read.return_value.data = cached

        changed = pd.DataFrame(
            {"value": [999]},
            index=pd.DatetimeIndex([pd.Timestamp("2024-01-10", tz=_UTC)]),
        )
        new_part = _daily_df("2024-01-11", 5, value_start=500)
        cache = _make_cache(lib, pd.concat([changed, new_part]))
        cache.get("S", end=datetime.date(2024, 1, 20), count=10)

        stored = lib.update.call_args[0][1]
        assert pd.Timestamp("2024-01-10", tz=_UTC) in stored.index

    def test_daily_gap_across_dst_spring_forward(self, lib):
        """Gap calculation uses calendar days, not timedelta, to avoid DST errors."""
        # 2024-03-10 is US spring forward — midnight-to-midnight is only 23h
        dates = pd.date_range("2024-03-05", periods=5, freq="D", tz=_NY)
        cached = pd.DataFrame({"value": range(5)}, index=dates)  # ends Mar 9
        lib.has_symbol.return_value = True
        lib.read.return_value.data = cached

        new_dates = pd.date_range("2024-03-10", periods=3, freq="D", tz=_NY)
        new = pd.DataFrame({"value": range(200, 203)}, index=new_dates)
        cache = _make_cache(lib, new, get_tz=lambda _: _NY)
        cache.get("S", end=datetime.date(2024, 3, 12), count=5)

        fetch_mock: MagicMock = cache._fetch  # type: ignore[assignment]
        _, _, call_count = fetch_mock.call_args[0]
        assert call_count == 4  # Mar 9 (overlap) + Mar 10, 11, 12

    def test_empty_source_returns_existing(self, lib):
        cached = _daily_df("2024-01-01", 10)
        lib.has_symbol.return_value = True
        lib.read.return_value.data = cached

        cache = _make_cache(lib)
        result = cache.get("S", end=datetime.date(2024, 1, 20), count=10)

        assert len(result) == 10
        lib.update.assert_not_called()


# ── incomplete bar exclusion ─────────────────────────────────────


class TestIncompleteBarExclusion:
    def test_today_excluded_from_daily_storage(self, lib):
        today_utc = pd.Timestamp.now(_UTC).normalize()
        start = (today_utc - pd.Timedelta(days=14)).date()
        df = _daily_df(start, 15)
        cache = _make_cache(lib, df)
        cache.get("S", count=10)

        stored = lib.update.call_args[0][1]
        assert stored.index[-1] < today_utc


# ── intraday ─────────────────────────────────────────────────────


class TestIntraday:
    def test_cache_miss(self, lib):
        df = _intraday_df("2024-01-15 09:30", 360)
        cache = _make_cache(lib, df, bar_minutes=1, default_count=1950)
        end = datetime.datetime(2024, 1, 15, 15, 30, tzinfo=_UTC)
        result = cache.get("S", end=end, count=100)

        assert len(result) == 100
        lib.update.assert_called_once()

    def test_large_gap_falls_back_to_miss(self, lib):
        """When gap between cached and requested end exceeds count, refetch instead of filling."""
        cached = _intraday_df("2024-01-12 09:30", 150)
        lib.has_symbol.return_value = True
        lib.read.return_value.data = cached

        fresh = _intraday_df("2024-01-15 09:30", 360)
        cache = _make_cache(lib, fresh, bar_minutes=1, default_count=1950)
        end = datetime.datetime(2024, 1, 15, 15, 30, tzinfo=_UTC)
        result = cache.get("S", end=end, count=360)

        assert len(result) == 360
        fetch_mock: MagicMock = cache._fetch  # type: ignore[assignment]
        _, _, call_count = fetch_mock.call_args[0]
        assert call_count == 360

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


# ── timezone end-to-end ───────────────────────────────────────────


class TestTimezoneEndToEnd:
    def test_store_localizes_to_configured_tz(self, lib):
        df = _intraday_df("2024-01-15 09:30", 60)
        cache = _make_cache(
            lib, df, bar_minutes=1, default_count=1950, get_tz=lambda _: _NY
        )
        end = datetime.datetime(2024, 1, 15, 10, 30, tzinfo=_UTC)
        cache.get("S", end=end, count=30)

        stored = lib.update.call_args[0][1]
        assert str(pd.DatetimeIndex(stored.index).tz) == "America/New_York"

    def test_read_returns_tz_aware_in_configured_tz(self, lib):
        raw = _intraday_df("2024-01-15 09:30", 60)
        raw.index = pd.DatetimeIndex(raw.index).tz_convert(_NY)
        lib.has_symbol.return_value = True
        lib.read.return_value.data = raw

        cache = _make_cache(
            lib, bar_minutes=1, default_count=1950, get_tz=lambda _: _NY
        )
        end = datetime.datetime(2024, 1, 15, 10, 30, tzinfo=_UTC)
        result = cache.get("S", end=end, count=30)

        assert str(pd.DatetimeIndex(result.index).tz) == "America/New_York"


# ── normalize ────────────────────────────────────────────────────


class TestNormalize:
    def test_strips_timezone(self):
        from arctic_incr_cache.cache import _normalize

        df = _daily_df("2024-01-01", 5)
        result = _normalize(df)
        assert pd.DatetimeIndex(result.index).tz is None

    def test_naive_raises_when_tz_provided(self):
        from arctic_incr_cache.cache import _normalize

        df = pd.DataFrame(
            {"v": [1]},
            index=pd.DatetimeIndex([pd.Timestamp("2024-01-15 12:00")]),
        )
        with pytest.raises(ValueError, match="tz-aware"):
            _normalize(df, tz=_NY)

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
        df = pd.DataFrame({"v": [10, 20, 25, 30]}, index=pd.DatetimeIndex(dates))
        result = _normalize(df)
        assert not result.index.has_duplicates
        assert result.loc[pd.Timestamp("2024-01-02"), "v"] == 25

    def test_empty_passthrough(self):
        from arctic_incr_cache.cache import _normalize

        result = _normalize(pd.DataFrame())
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


# ── end_ts ───────────────────────────────────────────────────────


class TestResolveEnd:
    def test_date_becomes_end_of_day(self):
        cache = _make_cache(MagicMock())
        result = cache._resolve_end(datetime.date(2024, 1, 15), tz=_UTC)
        assert result == pd.Timestamp("2024-01-15 23:59:59.999999", tz=_UTC)

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


# ── sparse density validation ────────────────────────────────────


class TestSparseDensity:
    """Sparse density validation: detect and recover from garbage cached data."""

    def test_sparse_cache_triggers_refetch(self, lib):
        """1 garbage bar on target date → re-fetch full window."""
        garbage = _intraday_df("2024-10-17 19:59", 1)
        lib.has_symbol.return_value = True
        lib.read.return_value.data = pd.concat([
            _intraday_df("2024-10-16 09:30", 200), garbage,
        ])

        full = _intraday_df("2024-10-17 09:30", 390)
        cache = _make_cache(
            lib, full, bar_minutes=1, default_count=200, min_bars_per_day=60,
        )
        end = datetime.datetime(2024, 10, 17, 20, 0, tzinfo=_UTC)
        result = cache.get("S", end=end, count=200)

        cache._fetch.assert_called_once()  # type: ignore[union-attr]
        on_day = pd.DatetimeIndex(result.index).date == datetime.date(2024, 10, 17)
        assert on_day.sum() > 1

    def test_sparse_upstream_returns_existing(self, lib):
        """Re-fetch also sparse → return existing, don't store."""
        garbage = _intraday_df("2024-10-17 19:59", 1)
        lib.has_symbol.return_value = True
        lib.read.return_value.data = pd.concat([
            _intraday_df("2024-10-16 09:30", 200), garbage,
        ])

        cache = _make_cache(
            lib, garbage, bar_minutes=1, default_count=200, min_bars_per_day=60,
        )
        end = datetime.datetime(2024, 10, 17, 20, 0, tzinfo=_UTC)
        result = cache.get("S", end=end, count=200)

        cache._fetch.assert_called_once()  # type: ignore[union-attr]
        assert len(result) == 200
        lib.update.assert_not_called()

    def test_stale_sparse_triggers_full_refetch(self, lib):
        """Only 1 garbage bar in cache → full re-fetch, not gap fetch."""
        garbage = _intraday_df("2024-10-17 19:59", 1)
        lib.has_symbol.return_value = True
        lib.read.return_value.data = garbage

        full = _intraday_df("2024-10-17 09:30", 390)
        cache = _make_cache(
            lib, full, bar_minutes=1, default_count=390, min_bars_per_day=60,
        )
        end = datetime.datetime(2024, 10, 17, 23, 59, tzinfo=_UTC)
        result = cache.get("S", end=end, count=390)

        cache._fetch.assert_called_once()  # type: ignore[union-attr]
        _, _, call_count = cache._fetch.call_args[0]  # type: ignore[union-attr]
        assert call_count == 390  # full window, not gap
        assert len(result) > 1

    def test_stale_sparse_upstream_also_bad(self, lib):
        """Sparse cache + sparse upstream → return existing."""
        garbage = _intraday_df("2024-10-17 19:59", 1)
        lib.has_symbol.return_value = True
        lib.read.return_value.data = garbage

        cache = _make_cache(
            lib, garbage, bar_minutes=1, default_count=390, min_bars_per_day=60,
        )
        end = datetime.datetime(2024, 10, 17, 23, 59, tzinfo=_UTC)
        result = cache.get("S", end=end, count=390)

        assert len(result) == 1  # can't do better
        lib.update.assert_not_called()

    def test_today_skips_sparse_check(self, lib):
        """Today's date (live session) must not trigger sparse check."""
        now = pd.Timestamp("2024-10-17 10:30:05", tz=_UTC)
        # 55 bars from yesterday + 5 from today = 60 total, fresh.
        # Today has 5 bars < min_bars_per_day=60 → would be sparse,
        # but today guard skips the check.
        cached = pd.concat([
            _intraday_df("2024-10-16 09:30", 55),
            _intraday_df("2024-10-17 10:26", 5),
        ])
        lib.has_symbol.return_value = True
        lib.read.return_value.data = cached

        cache = _make_cache(
            lib, bar_minutes=1, default_count=390, min_bars_per_day=60,
        )

        def _now(tz=None):
            return now.tz_convert(tz) if tz else now.tz_localize(None)

        with patch("arctic_incr_cache.cache.pd.Timestamp.now", side_effect=_now):
            result = cache.get("S", end=now, count=60)

        # count=60 >= min_bars_per_day=60, so only the today guard prevents
        # the sparse check from triggering.
        cache._fetch.assert_not_called()  # type: ignore[union-attr]
        assert len(result) == 60

    def test_daily_no_false_positive(self, lib):
        """Daily bars (1 per day) must not trigger sparse check."""
        lib.has_symbol.return_value = True
        lib.read.return_value.data = _daily_df("2024-01-01", 20)

        cache = _make_cache(lib)
        result = cache.get("S", end=datetime.date(2024, 1, 15), count=10)

        assert len(result) == 10
        cache._fetch.assert_not_called()  # type: ignore[union-attr]

    def test_default_min_bars_per_day(self):
        """Default min_bars_per_day is derived from bar_minutes."""
        cache = _make_cache(MagicMock(), bar_minutes=1)
        assert cache.min_bars_per_day == 60

        cache = _make_cache(MagicMock(), bar_minutes=5)
        assert cache.min_bars_per_day == 12

        cache = _make_cache(MagicMock(), bar_minutes=1440)
        assert cache.min_bars_per_day == 0

