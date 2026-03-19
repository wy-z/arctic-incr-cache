"""ArcticDB-backed time series cache with incremental updates.

Timezone handling
-----------------
Every symbol has a configured timezone via ``get_tz(symbol) -> tzinfo``.

* **Storage** — data is stored in ArcticDB as tz-aware in the configured
  timezone (e.g., ``America/New_York``).
* **Fetch contract** — ``fetch()`` must return a tz-aware DataFrame.
  Timestamps are converted to the configured timezone internally.

* **Return** — ``get()`` returns a tz-aware DataFrame in the configured
  timezone.
"""

import datetime
import logging
import math
import threading
import time
from collections.abc import Callable
from typing import Any

import pandas as pd
import ring

log = logging.getLogger(__name__)


# ── pure helpers ──────────────────────────────────────────────────


def _normalize(df: pd.DataFrame, tz: datetime.tzinfo | None = None) -> pd.DataFrame:
    """Convert to *tz*-aware, deduplicate (keep last), sort.

    *tz* provided → tz-aware input is converted; tz-naive input raises.
    *tz* is ``None`` → tz-aware input is stripped (keep wall-clock);
    tz-naive input passes through unchanged.
    """
    if df.empty:
        return df
    if isinstance(df.index, pd.DatetimeIndex):
        if tz:
            if df.index.tz is None:
                raise ValueError(
                    "fetch() must return tz-aware timestamps, got tz-naive"
                )
            df = df.set_axis(df.index.tz_convert(tz))
        elif df.index.tz is not None:
            df = df.set_axis(df.index.tz_localize(None))
    return df.loc[~df.index.duplicated(keep="last")].sort_index()


def _trim(df: pd.DataFrame, end: pd.Timestamp, count: int) -> pd.DataFrame:
    """Keep rows <= *end*, return the last *count*."""
    if df.empty:
        return df
    return df.loc[df.index <= end].tail(count)


# ── cache ─────────────────────────────────────────────────────────


class IncrCache:
    """Time series cache with incremental gap-filling, backed by ArcticDB.

    Data is stored in the configured timezone (via ``get_tz``).  All request
    parameters are converted to that timezone before querying ArcticDB.
    Internal comparisons use tz-aware timestamps in the configured timezone.

    Args:
        library: ArcticDB library instance.
        fetch: ``fetch(symbol, end, count) -> DataFrame``.
            Must return a tz-aware DataFrame.  Timestamps are converted
            to the configured timezone internally.
        get_tz: ``get_tz(symbol) -> tzinfo``.
            Determines the storage/comparison timezone for each symbol.
        bar_minutes: Bar width in minutes (1440 = daily, 1 = 1-min).
        default_count: Bars returned when *count* is omitted.
        spawn: Fire-and-forget callable for async writes.
            Defaults to daemon threads.  Pass ``gevent.spawn`` for async runtimes.
        lookback: Read-window multiplier (default 2).
            Daily: ``count * lookback`` calendar days.
            Intraday: ``ceil(count * bar_minutes / 1440) * lookback``
            calendar days, with a 7-day minimum to span weekends.
        lock_class: Lock constructor.  Defaults to ``threading.Lock``.
        floor: Shared floor dict for cross-instance state.
            Maps symbol to ``(oldest_ts, expiry, hits)``.
        cache_ttl: Result TTL in seconds (default 60).
            Repeated ``get()`` calls with the same resolved parameters
            return a cached result within this window.  Set to 0 to disable.
    """

    FLOOR_TTLS = (360, 720, 1440)  # 6min, 12min, 24min progressive
    MIN_LOOKBACK_DAYS = 7  # cover weekends / short holidays

    def __init__(
        self,
        library: Any,
        fetch: Callable[[str, pd.Timestamp, int], pd.DataFrame],
        *,
        get_tz: Callable[[str], datetime.tzinfo],
        bar_minutes: int = 1440,
        default_count: int = 252,
        spawn: Callable[..., Any] | None = None,
        lookback: int = 2,
        lock_class: type | None = None,
        floor: dict[str, tuple[pd.Timestamp, float, int]] | None = None,
        cache_ttl: int = 60,
    ):
        if bar_minutes <= 0:
            raise ValueError("bar_minutes must be > 0")
        self._lib = library
        self._fetch = fetch
        self._get_tz = get_tz
        self.bar_minutes = bar_minutes
        self.default_count = default_count
        self._spawn = spawn
        self.lookback = lookback
        self._lock_class = lock_class or threading.Lock
        self._locks: dict[str, Any] = {}
        self._meta_lock = threading.Lock()
        # Per-symbol (oldest_ts, expiry, hits): skip re-fetch when cache
        # already covers the source's oldest available date.
        self._floor = floor if floor is not None else {}
        get_impl: Callable[[str, pd.Timestamp, int], pd.DataFrame]
        if cache_ttl > 0:
            get_impl = ring.lru(expire=cache_ttl)(self._do_get)  # type: ignore[assignment]
        else:
            get_impl = self._do_get
        self._cached_get = get_impl

    @property
    def is_daily(self) -> bool:
        return self.bar_minutes >= 1440

    # ── time helpers ──────────────────────────────────────────────

    def _resolve_end(
        self, end: datetime.date | datetime.datetime | None, tz: datetime.tzinfo
    ) -> pd.Timestamp:
        """Normalize *end* to a tz-aware ``pd.Timestamp`` in *tz*.

        ``date`` inputs become end-of-day in the configured timezone.
        Naive ``datetime`` inputs are interpreted as **local timezone**,
        then converted to the configured timezone.
        Aware ``datetime`` inputs are converted directly.
        """
        if end is None:
            return pd.Timestamp.now(tz)
        if type(end) is datetime.date:
            return pd.Timestamp(
                datetime.datetime.combine(end, datetime.time.max), tz=tz
            )
        ts = pd.Timestamp(end)
        if ts.tzinfo:
            return ts.tz_convert(tz)
        return pd.Timestamp(ts.to_pydatetime().astimezone()).tz_convert(tz)

    def _align_bar(self, ts: pd.Timestamp) -> pd.Timestamp:
        """Floor *ts* to the bar boundary."""
        if self.is_daily:
            return ts.normalize()
        return ts.floor(f"{self.bar_minutes}min")

    def _incomplete_threshold(self, tz: datetime.tzinfo) -> pd.Timestamp:
        """Bars at or after this tz-aware timestamp may still be updating."""
        if self.is_daily:
            return pd.Timestamp.now(tz).normalize()
        return pd.Timestamp.now(tz) - pd.Timedelta(minutes=self.bar_minutes)

    def is_fresh(
        self, last: pd.Timestamp, end: pd.Timestamp, tz: datetime.tzinfo
    ) -> bool:
        threshold = self._incomplete_threshold(tz)
        backoff = (
            datetime.timedelta(days=1)
            if self.is_daily
            else datetime.timedelta(minutes=self.bar_minutes)
        )
        safe = last - backoff if last >= threshold else last
        if self.is_daily:
            return safe.date() >= end.date()
        expected_last = end - backoff
        return safe >= expected_last

    def _calc_stale_fetch_count(
        self, last: pd.Timestamp, end: pd.Timestamp, count: int
    ) -> int:
        """Return bars to fetch for stale updates.

        Fetch the gap plus one overlap bar so callers can refresh the cached tail.
        If the gap exceeds the requested window, fall back to a full-window fetch.
        """
        if self.is_daily:
            gap_count = (end.date() - last.date()).days + 1
        else:
            bar = pd.Timedelta(minutes=self.bar_minutes)
            gap_count = math.floor((end - last) / bar) + 1
        gap_count = max(gap_count, 1)
        return min(gap_count, count)

    # ── floor ─────────────────────────────────────────────────────

    def _set_floor(self, symbol: str, oldest: pd.Timestamp) -> None:
        prev = self._floor.get(symbol)
        hits = (prev[2] if prev else 0) + 1
        expiry = (
            math.inf
            if hits > len(self.FLOOR_TTLS)
            else time.time() + self.FLOOR_TTLS[hits - 1]
        )
        self._floor[symbol] = (oldest, expiry, hits)

    # ── storage ───────────────────────────────────────────────────

    def _lock_for(self, symbol: str) -> Any:
        with self._meta_lock:
            return self._locks.setdefault(symbol, self._lock_class())

    def _read(
        self, symbol: str, date_range: tuple, tz: datetime.tzinfo
    ) -> pd.DataFrame:
        if not self._lib.has_symbol(symbol):
            return pd.DataFrame()
        try:
            return _normalize(self._lib.read(symbol, date_range=date_range).data, tz)
        except Exception as exc:
            log.warning("read error %s: %s", symbol, exc)
            return pd.DataFrame()

    def _fire(self, fn: Callable) -> Any:
        if self._spawn is not None:
            return self._spawn(fn)
        t = threading.Thread(target=fn, daemon=True)
        t.start()
        return t

    def _store(self, symbol: str, df: pd.DataFrame) -> None:
        """Exclude the incomplete bar, then upsert.

        Data must already be tz-aware in the configured timezone (via
        ``_normalize``) before calling this method.
        """
        tz = self._get_tz(symbol)
        threshold = self._incomplete_threshold(tz)
        if not df.empty and df.index[-1] >= threshold:
            df = df.iloc[:-1]
        if df.empty:
            return
        rows = len(df)
        span = f"{df.index[0].date()}..{df.index[-1].date()}"

        def write():
            try:
                with self._lock_for(symbol):
                    self._lib.update(symbol, df, upsert=True, prune_previous_versions=True)
                log.info("stored %s %s (+%d rows)", symbol, span, rows)
            except Exception:
                log.exception("write error %s", symbol)

        self._fire(write)

    # ── public API ────────────────────────────────────────────────

    def get(
        self,
        symbol: str,
        end: datetime.date | datetime.datetime | None = None,
        count: int | None = None,
    ) -> pd.DataFrame:
        """Return the last *count* bars for *symbol* up to *end*.

        *end* is converted to the configured timezone (via ``get_tz``) before
        querying ArcticDB.  The returned DataFrame is tz-aware in the
        configured timezone.
        """
        tz = self._get_tz(symbol)
        end_ts = self._align_bar(self._resolve_end(end, tz))
        if count is None:
            count = self.default_count
        if count <= 0:
            return pd.DataFrame()
        result = self._cached_get(symbol, end_ts, count)
        return result.copy() if self._cached_get is not self._do_get else result

    def _do_get(
        self, symbol: str, end_ts: pd.Timestamp, count: int
    ) -> pd.DataFrame:
        tz = self._get_tz(symbol)
        if self.is_daily:
            start_ts = end_ts - pd.Timedelta(days=count * self.lookback)
        else:
            cal_days = math.ceil(count * self.bar_minutes / 1440) * self.lookback
            start_ts = end_ts - pd.Timedelta(days=max(cal_days, self.MIN_LOOKBACK_DAYS))
        existing = self._read(symbol, (start_ts, end_ts), tz)

        def trim(df: pd.DataFrame) -> pd.DataFrame:
            return _trim(df, end_ts, count)

        def merge(*dfs: pd.DataFrame) -> pd.DataFrame:
            return trim(_normalize(pd.concat(dfs), tz))

        # Cache miss
        if existing.empty:
            log.info("miss %s, fetching %d bars", symbol, count)
            df = _normalize(self._fetch(symbol, end_ts, count), tz)
            if not df.empty:
                self._store(symbol, df)
            return trim(df)

        # Fresh — but fall through if we have fewer rows than requested
        last = existing.index[-1]
        if self.is_fresh(last, end_ts, tz):
            trimmed = trim(existing)
            if len(trimmed) >= count:
                return trimmed
            # Source's oldest known date still valid and cache covers it?
            floor_entry = self._floor.get(symbol)
            if floor_entry:
                oldest, expiry, hits = floor_entry
                if time.time() < expiry and existing.index[0] <= oldest:
                    if hits <= len(self.FLOOR_TTLS):
                        log.info(
                            "floor hit %s: oldest=%s (hits=%d)",
                            symbol, oldest, hits,
                        )
                    return trimmed

            log.info("short %s: have %d, need %d", symbol, len(trimmed), count)
            df = _normalize(self._fetch(symbol, end_ts, count), tz)
            if df.empty:
                self._set_floor(symbol, existing.index[0])
                return trimmed
            self._store(symbol, df)
            if len(df) < count:
                self._set_floor(symbol, df.index[0])
            return merge(existing, df)

        # Incremental update — fetch only the stale gap (plus overlap) and merge
        fetch_count = self._calc_stale_fetch_count(last, end_ts, count)
        new = _normalize(self._fetch(symbol, end_ts, fetch_count), tz)
        if new.empty:
            return trim(existing)

        new = new.loc[new.index >= last]
        if last in new.index and new.loc[last].equals(existing.loc[last]):
            new = new.iloc[1:]
        if new.empty:
            return trim(existing)

        self._store(symbol, new)
        return merge(existing, new)
