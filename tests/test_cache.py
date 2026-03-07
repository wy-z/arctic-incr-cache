"""Tests for IncrCache."""

import datetime
from unittest.mock import MagicMock

import pandas as pd
import pytest

from arctic_incr_cache import IncrCache

# ── helpers ──────────────────────────────────────────────────────


def _daily_df(start, n, value_start=100):
    dates = pd.date_range(start=start, periods=n, freq="D")
    return pd.DataFrame({"value": range(value_start, value_start + n)}, index=dates)


def _intraday_df(start, n):
    times = pd.date_range(start=start, periods=n, freq="1min")
    return pd.DataFrame({"price": range(n)}, index=times)


def _make_cache(
    lib,
    fetch_data=None,
    *,
    bar_minutes: int = 1440,
    default_count: int = 252,
    get_tz=None,
):
    """Build an IncrCache with a mock library and canned fetch data."""
    data = fetch_data if fetch_data is not None else pd.DataFrame()
    fetch = MagicMock(return_value=data)
    return IncrCache(
        lib, fetch,
        bar_minutes=bar_minutes, default_count=default_count, get_tz=get_tz,
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


# ── incremental update ───────────────────────────────────────────


class TestIncrementalUpdate:
    def test_merges_new_data(self, lib):
        cached = _daily_df("2024-01-01", 10)
        lib.has_symbol.return_value = True
        lib.read.return_value.data = cached

        new = _daily_df("2024-01-11", 10, value_start=200)
        cache = _make_cache(lib, new)
        result = cache.get("S", end=datetime.date(2024, 1, 20), count=15)

        assert len(result) == 15
        lib.update.assert_called_once()

    def test_deduplicates_unchanged_overlap(self, lib):
        cached = _daily_df("2024-01-01", 10)
        lib.has_symbol.return_value = True
        lib.read.return_value.data = cached

        overlap = cached.iloc[[-1]]  # Jan 10, same value
        new_part = _daily_df("2024-01-11", 5, value_start=500)
        cache = _make_cache(lib, pd.concat([overlap, new_part]))
        cache.get("S", end=datetime.date(2024, 1, 20), count=15)

        stored = lib.update.call_args[0][1]
        assert pd.Timestamp("2024-01-10") not in stored.index

    def test_keeps_changed_overlap(self, lib):
        cached = _daily_df("2024-01-01", 10)
        lib.has_symbol.return_value = True
        lib.read.return_value.data = cached

        changed = pd.DataFrame(
            {"value": [999]}, index=pd.DatetimeIndex([pd.Timestamp("2024-01-10")])
        )
        new_part = _daily_df("2024-01-11", 5, value_start=500)
        cache = _make_cache(lib, pd.concat([changed, new_part]))
        cache.get("S", end=datetime.date(2024, 1, 20), count=15)

        stored = lib.update.call_args[0][1]
        assert pd.Timestamp("2024-01-10") in stored.index

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
        today = datetime.date.today()
        df = _daily_df(today - datetime.timedelta(days=14), 15)
        cache = _make_cache(lib, df)
        cache.get("S", count=10)

        stored = lib.update.call_args[0][1]
        assert stored.index[-1] < pd.Timestamp(today)


# ── intraday ─────────────────────────────────────────────────────


class TestIntraday:
    def test_cache_miss(self, lib):
        df = _intraday_df("2024-01-15 09:30", 360)
        cache = _make_cache(lib, df, bar_minutes=1, default_count=1950)
        result = cache.get("S", end=datetime.datetime(2024, 1, 15, 15, 30), count=100)

        assert len(result) == 100
        lib.update.assert_called_once()

    def test_gap_count_uses_minute_resolution(self, lib):
        cached = _intraday_df("2024-01-15 09:30", 150)
        lib.has_symbol.return_value = True
        lib.read.return_value.data = cached

        new = _intraday_df("2024-01-15 11:59", 212)
        cache = _make_cache(lib, new, bar_minutes=1, default_count=1950)
        result = cache.get("S", end=datetime.datetime(2024, 1, 15, 15, 30), count=500)

        assert not result.empty


# ── timezone end-to-end ───────────────────────────────────────────


class TestTimezoneEndToEnd:
    def test_store_localizes_to_configured_tz(self, lib):
        from zoneinfo import ZoneInfo

        tz = ZoneInfo("America/New_York")
        df = _intraday_df("2024-01-15 09:30", 60)
        cache = _make_cache(
            lib, df, bar_minutes=1, default_count=1950, get_tz=lambda _: tz
        )
        cache.get("S", end=datetime.datetime(2024, 1, 15, 10, 30), count=30)

        stored = lib.update.call_args[0][1]
        assert pd.DatetimeIndex(stored.index).tz is not None
        assert str(pd.DatetimeIndex(stored.index).tz) == "America/New_York"

    def test_read_returns_naive_in_configured_tz(self, lib):
        from zoneinfo import ZoneInfo

        tz = ZoneInfo("America/New_York")
        # Simulate ArcticDB returning tz-aware data in NY
        raw = _intraday_df("2024-01-15 09:30", 60)
        raw.index = pd.DatetimeIndex(raw.index).tz_localize(tz)
        lib.has_symbol.return_value = True
        lib.read.return_value.data = raw

        cache = _make_cache(
            lib, bar_minutes=1, default_count=1950, get_tz=lambda _: tz
        )
        result = cache.get(
            "S", end=datetime.datetime(2024, 1, 15, 10, 30), count=30
        )

        assert pd.DatetimeIndex(result.index).tz is None
        # Wall-clock time preserved (not UTC-converted: 09:30 NY != 14:30 UTC)
        assert result.index[0] < pd.Timestamp("2024-01-15 12:00:00")


# ── normalize ────────────────────────────────────────────────────


class TestNormalize:
    def test_strips_timezone(self):
        from arctic_incr_cache.cache import _normalize

        df = _daily_df("2024-01-01", 5)
        df.index = pd.DatetimeIndex(df.index).tz_localize("UTC")
        result = _normalize(df)
        assert pd.DatetimeIndex(result.index).tz is None

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
        result = _trim(df, pd.Timestamp("2024-01-20"), 5)
        assert len(result) == 5
        assert result.index[-1] == pd.Timestamp("2024-01-20")

    def test_filters_by_end_ts(self):
        from arctic_incr_cache.cache import _trim

        df = _daily_df("2024-01-01", 20)
        result = _trim(df, pd.Timestamp("2024-01-10"), 1000)
        assert len(result) == 10
        assert result.index[-1] == pd.Timestamp("2024-01-10")

    def test_empty_passthrough(self):
        from arctic_incr_cache.cache import _trim

        result = _trim(pd.DataFrame(), pd.Timestamp("2024-01-10"), 10)
        assert result.empty


# ── end_ts ───────────────────────────────────────────────────────


class TestResolveEnd:
    def test_date_becomes_end_of_day(self):
        cache = _make_cache(MagicMock())
        result = cache._resolve_end(datetime.date(2024, 1, 15), tz=None)
        assert result == pd.Timestamp("2024-01-15 23:59:59.999999")

    def test_datetime_passes_through(self):
        cache = _make_cache(MagicMock())
        result = cache._resolve_end(datetime.datetime(2024, 1, 15, 14, 30), tz=None)
        assert result == pd.Timestamp("2024-01-15 14:30:00")

    def test_none_defaults_to_now(self):
        cache = _make_cache(MagicMock())
        result = cache._resolve_end(None, tz=None)
        assert result.date() == datetime.date.today()


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
