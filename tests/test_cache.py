"""Tests for IncrCache."""

import datetime
import random
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

    def test_large_gap_fetches_full_window(self, lib):
        """Gap wider than count → full-window fetch, not a gap fill.

        The cache holds enough rows to skip the short branch, so this
        reaches the incremental branch and truncates there.
        """
        cached = _intraday_df("2024-01-12 09:30", 150)  # ends 11:59
        lib.has_symbol.return_value = True
        lib.read.return_value.data = cached

        fresh = _intraday_df("2024-01-15 09:30", 360)
        cache = _make_cache(lib, fresh, bar_minutes=1, default_count=1950)
        end = datetime.datetime(2024, 1, 15, 15, 30, tzinfo=_UTC)
        result = cache.get("S", end=end, count=100)

        assert len(result) == 100
        fetch_mock: MagicMock = cache._fetch  # type: ignore[assignment]
        _, _, call_count = fetch_mock.call_args[0]
        assert call_count == 100  # min(gap_count, count), gap is far wider

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
    def test_naive_raises(self):
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
        lib.read.return_value.data = pd.concat(
            [
                _intraday_df("2024-10-16 09:30", 200),
                garbage,
            ]
        )

        full = _intraday_df("2024-10-17 09:30", 390)
        cache = _make_cache(
            lib,
            full,
            bar_minutes=1,
            default_count=200,
            min_bars_per_day=60,
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
        lib.read.return_value.data = pd.concat(
            [
                _intraday_df("2024-10-16 09:30", 200),
                garbage,
            ]
        )

        cache = _make_cache(
            lib,
            garbage,
            bar_minutes=1,
            default_count=200,
            min_bars_per_day=60,
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
            lib,
            full,
            bar_minutes=1,
            default_count=390,
            min_bars_per_day=60,
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
            lib,
            garbage,
            bar_minutes=1,
            default_count=390,
            min_bars_per_day=60,
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
        cached = pd.concat(
            [
                _intraday_df("2024-10-16 09:30", 55),
                _intraday_df("2024-10-17 10:26", 5),
            ]
        )
        lib.has_symbol.return_value = True
        lib.read.return_value.data = cached

        cache = _make_cache(
            lib,
            bar_minutes=1,
            default_count=390,
            min_bars_per_day=60,
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

    def test_hole_triggers_refetch(self, lib):
        """Window spans 14 expected bars but holds 10 → full re-fetch."""
        lib.has_symbol.return_value = True
        lib.read.return_value.data = _holey_df()

        full = _daily_df("2024-01-01", 14)
        cache = _make_cache(lib, full, is_holey=_gapless)
        result = cache.get("S", end=datetime.date(2024, 1, 14), count=10)

        cache._fetch.assert_called_once()  # type: ignore[union-attr]
        _, _, call_count = cache._fetch.call_args[0]  # type: ignore[union-attr]
        assert call_count == 10  # full window, not gap
        lib.update.assert_called_once()
        assert pd.Timestamp("2024-01-05", tz=_UTC) in result.index

    def test_short_frames_never_reach_the_hook(self):
        """Under 2 bars there is no interior to miss.  Hooks index
        ``df.index[0]`` on the strength of this, so it is a contract."""

        def boom(*_):
            raise AssertionError("hook called on a frame it was promised not to")

        cache = _make_cache(MagicMock(), is_holey=boom)

        assert not cache._has_hole("S", pd.DataFrame())
        assert not cache._has_hole("S", _daily_df("2024-01-01", 1))

    def test_slack_is_the_hooks_business(self, lib):
        """Same 4 missing days, a hook that tolerates 5 → no re-fetch.  The
        cache holds no tolerance of its own; the verdict is the hook's."""
        lib.has_symbol.return_value = True
        lib.read.return_value.data = _holey_df()

        cache = _make_cache(lib, is_holey=lambda _, df: len(df) < _span_days(df) - 5)
        result = cache.get("S", end=datetime.date(2024, 1, 14), count=10)

        cache._fetch.assert_not_called()  # type: ignore[union-attr]
        assert len(result) == 10

    def test_refetch_still_holey_returns_existing_unstored(self, lib):
        """Source has the same hole → return existing, never store: update
        replaces the whole span, a gappy frame would delete good rows."""
        lib.has_symbol.return_value = True
        lib.read.return_value.data = _holey_df()

        cache = _make_cache(lib, _holey_df(), is_holey=_gapless)
        result = cache.get("S", end=datetime.date(2024, 1, 14), count=10)

        cache._fetch.assert_called_once()  # type: ignore[union-attr]
        assert len(result) == 10
        lib.update.assert_not_called()


class TestWriteGuard:
    """`_store` is the integrity boundary: `update` replaces the frame's
    whole span, so a holey frame is served but never written."""

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
        served: a source over-returning holey history must not be stored."""
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

    def test_refuses_gap_fetch_leaving_a_seam_hole(self, lib):
        """The hole sits between the cached tail and the first new bar, so
        the frame that would be written cannot show it — the gap fetch is
        checked before the overlap row is deduplicated away."""
        cached = _daily_df("2024-01-01", 10)  # ends Jan 10
        lib.has_symbol.return_value = True
        lib.read.return_value.data = cached

        gap = pd.concat(
            [
                cached.iloc[[-1]],  # Jan 10 overlap, unchanged
                _daily_df("2024-01-13", 3, value_start=300),  # Jan 11-12 missing
            ]
        )
        cache = _make_cache(lib, gap, is_holey=_gapless)
        cache.get("S", end=datetime.date(2024, 1, 15), count=10)

        assert cache._fetch.call_count == 2  # type: ignore[union-attr]  # full re-fetch
        lib.update.assert_not_called()

    def test_refused_backfill_leaves_no_floor(self, lib):
        """A refused backfill is no evidence of how deep the source goes —
        flooring on it would suppress the retry until the TTL expires."""
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

        assert cache._fetch.call_count == 2  # type: ignore[union-attr]  # not floored

    def test_refuses_backfill_sparse_on_the_target_date(self, lib):
        """Density is checked on the backfill too, hooks or not: a frame
        spanning the target date with a handful of bars on it would replace
        a full day's worth."""
        lib.has_symbol.return_value = True
        lib.read.return_value.data = pd.concat(
            [
                _intraday_df("2024-01-12 09:30", 100),
                _intraday_df("2024-01-15 09:30", 60),  # target day, complete
            ]
        )

        backfill = pd.concat(
            [
                _intraday_df("2024-01-12 09:30", 100),
                _intraday_df("2024-01-15 10:29", 1),  # target day, 1 bar
            ]
        )
        cache = _make_cache(lib, backfill, bar_minutes=1, default_count=390)
        end = datetime.datetime(2024, 1, 15, 10, 29, tzinfo=_UTC)
        cache.get("S", end=end, count=200)

        lib.update.assert_not_called()
        assert not cache._floor  # no floor recorded from a corrupt backfill

    def test_refuses_sparse_fetch_on_a_miss(self, lib):
        """A read error reports an empty frame, so a populated symbol can
        reach the miss branch — writing an unvalidated fetch there would
        replace the day its span covers."""
        lib.has_symbol.return_value = True
        lib.read.side_effect = RuntimeError("transient read failure")

        fetched = pd.concat(
            [
                _intraday_df("2024-01-12 09:30", 100),
                _intraday_df("2024-01-15 10:29", 1),  # target day, 1 bar
            ]
        )
        cache = _make_cache(lib, fetched, bar_minutes=1, default_count=390)
        end = datetime.datetime(2024, 1, 15, 10, 29, tzinfo=_UTC)
        result = cache.get("S", end=end, count=200)

        lib.update.assert_not_called()
        assert not result.empty  # the source's view is still served

    def test_incomplete_bar_exclusion_is_idempotent(self, lib):
        """The floor decision and the store must judge the same frame. Both
        run the exclusion, so a second pass must not eat a complete bar and
        make them disagree at an exact bar boundary."""
        cache = _make_cache(lib, bar_minutes=1)
        # A bar landing exactly on the threshold is the boundary case: it
        # counts as incomplete, so a naive "drop the last row" repeats.
        threshold = pd.Timestamp("2024-01-15 10:30", tz=_UTC)
        cache._incomplete_threshold = lambda _tz: threshold  # type: ignore[method-assign]
        df = _intraday_df("2024-01-15 10:28", 4)  # 10:28..10:31

        once = cache._complete("S", df)
        assert once.index[-1] == pd.Timestamp("2024-01-15 10:29", tz=_UTC)
        assert cache._complete("S", once).equals(once)


class TestDiscontinuousFetchGate:
    """Incremental fetch that skips the cached tail must not store a
    fabricated mid-series hole."""

    def test_upgrades_to_full_refetch(self, lib):
        cached = _daily_df("2024-01-01", 10)  # ends Jan 10
        lib.has_symbol.return_value = True
        lib.read.return_value.data = cached

        gap = _daily_df("2024-01-12", 4, value_start=200)  # skips Jan 10–11
        full = _daily_df("2024-01-06", 10, value_start=300)
        cache = _make_cache(lib)
        cache._fetch.side_effect = [gap, full]  # type: ignore[union-attr]
        cache.get("S", end=datetime.date(2024, 1, 15), count=10)

        assert cache._fetch.call_count == 2  # type: ignore[union-attr]
        _, _, call_count = cache._fetch.call_args[0]  # type: ignore[union-attr]
        assert call_count == 10  # second fetch is the full window
        lib.update.assert_called_once()
        stored = lib.update.call_args[0][1]
        assert pd.Timestamp("2024-01-11", tz=_UTC) in stored.index

    def test_bad_refetch_serves_source_view_unstored(self, lib):
        cached = _daily_df("2024-01-01", 10)
        lib.has_symbol.return_value = True
        lib.read.return_value.data = cached

        gap = _daily_df("2024-01-12", 4, value_start=200)
        cache = _make_cache(lib)
        cache._fetch.side_effect = [gap, pd.DataFrame()]  # type: ignore[union-attr]
        result = cache.get("S", end=datetime.date(2024, 1, 15), count=10)

        lib.update.assert_not_called()
        assert pd.Timestamp("2024-01-12", tz=_UTC) in result.index
        assert len(result) == 10


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
