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
import threading
import time
from collections.abc import Callable
from typing import Any

import pandas as pd

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
            ``count * bar_minutes * lookback`` determines how far back to
            read from storage.  Use 1 for range-based queries where the
            count already matches the calendar span (e.g., intraday).
        lock_class: Lock constructor.  Defaults to ``threading.Lock``.
    """

    FLOOR_TTL = 3600  # seconds

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
        # Per-symbol (oldest_ts, expiry): skip re-fetch when cache
        # already covers the source's oldest available date.
        self._floor: dict[str, tuple[pd.Timestamp, float]] = {}

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
        # Freshness is left-closed right-open: the bar at end may not exist yet.
        expected_last = end - backoff
        return safe >= expected_last

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

        def write():
            try:
                with self._lock_for(symbol):
                    self._lib.update(symbol, df, upsert=True, prune_previous_versions=True)
                log.info("stored %s (+%d rows)", symbol, rows)
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
        end_ts = self._resolve_end(end, tz)
        if count is None:
            count = self.default_count
        if count <= 0:
            return pd.DataFrame()
        lookback = self.lookback
        start_ts = end_ts - pd.Timedelta(minutes=count * self.bar_minutes * lookback)
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
            now = time.monotonic()
            floor = self._floor.get(symbol)
            if floor:
                oldest, expiry = floor
                if now < expiry and existing.index[0] <= oldest:
                    log.info("floor hit %s: oldest=%s", symbol, oldest)
                    return trimmed

            log.info("short %s: have %d, need %d", symbol, len(trimmed), count)
            df = _normalize(self._fetch(symbol, end_ts, count), tz)
            if df.empty:
                return trimmed
            self._store(symbol, df)
            if len(df) < count:
                self._floor[symbol] = (df.index[0], now + self.FLOOR_TTL)
            return merge(existing, df)

        # Incremental update — if gap exceeds requested count, treat as miss
        gap = int((end_ts - last).total_seconds() / 60 / self.bar_minutes) + 1
        if gap > count:
            log.info("gap too large %s: %d > %d, refetching", symbol, gap, count)
            df = _normalize(self._fetch(symbol, end_ts, count), tz)
            if df.empty:
                return trim(existing)
            self._store(symbol, df)
            return trim(df)
        log.info("update %s: %s -> %s", symbol, last.date(), end_ts.date())
        new = _normalize(self._fetch(symbol, end_ts, gap), tz)
        if new.empty:
            return trim(existing)

        new = new.loc[new.index >= last]
        if last in new.index and new.loc[last].equals(existing.loc[last]):
            new = new.iloc[1:]
        if new.empty:
            return trim(existing)

        self._store(symbol, new)
        return merge(existing, new)
