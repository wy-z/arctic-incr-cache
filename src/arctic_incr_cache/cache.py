"""ArcticDB-backed time series cache with incremental updates.

Timezone handling
-----------------
When ``get_tz`` returns a timezone for a symbol:

* **Storage** — data is stored in ArcticDB in the configured timezone.
* **Queries** — all request parameters (``end``, ``count``) are first
  converted to the configured timezone before reading or comparing.
* **Fetch contract** — ``fetch()`` must return a tz-naive DataFrame whose
  timestamps represent wall-clock time in the configured timezone.

When ``get_tz`` returns ``None`` (daily data), everything stays tz-naive.
"""

import datetime
import logging
import threading
from collections.abc import Callable
from typing import Any

import pandas as pd

log = logging.getLogger(__name__)


# ── pure helpers ──────────────────────────────────────────────────


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    """Strip timezone (keeping wall-clock time), deduplicate (keep last), sort."""
    if df.empty:
        return df
    if isinstance(df.index, pd.DatetimeIndex) and df.index.tz is not None:
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
    Internal comparisons use tz-naive timestamps in the configured timezone's
    wall-clock time.

    Args:
        library: ArcticDB library instance.
        fetch: ``fetch(symbol, end, count) -> DataFrame``.
            Must return a tz-naive DataFrame in the configured timezone's
            wall-clock time.
        bar_minutes: Bar width in minutes (1440 = daily, 1 = 1-min).
        default_count: Bars returned when *count* is omitted.
        get_tz: ``get_tz(symbol) -> tzinfo | None``.
            Determines the storage timezone.  Return ``None`` for daily data.
        spawn: Fire-and-forget callable for async writes.
            Defaults to daemon threads.  Pass ``gevent.spawn`` for async runtimes.
        lock_class: Lock constructor.  Defaults to ``threading.Lock``.
    """

    def __init__(
        self,
        library: Any,
        fetch: Callable[[str, pd.Timestamp, int], pd.DataFrame],
        *,
        bar_minutes: int = 1440,
        default_count: int = 252,
        get_tz: Callable[[str], datetime.tzinfo | None] | None = None,
        spawn: Callable[..., Any] | None = None,
        lock_class: type | None = None,
    ):
        if bar_minutes <= 0:
            raise ValueError("bar_minutes must be > 0")
        self._lib = library
        self._fetch = fetch
        self.bar_minutes = bar_minutes
        self.default_count = default_count
        self._get_tz = get_tz or (lambda _: None)
        self._spawn = spawn
        self._lock_class = lock_class or threading.Lock
        self._locks: dict[str, Any] = {}
        self._meta_lock = threading.Lock()

    @property
    def is_daily(self) -> bool:
        return self.bar_minutes >= 1440

    # ── time helpers ──────────────────────────────────────────────

    def _resolve_end(
        self, end: datetime.date | datetime.datetime | None, tz: datetime.tzinfo | None
    ) -> pd.Timestamp:
        """Normalize *end* to a ``pd.Timestamp`` in the configured timezone.

        Naive datetime inputs are interpreted as the configured timezone.
        Aware datetime inputs are converted to it.
        """
        if end is None:
            if tz:
                return pd.Timestamp.now(tz)
            return pd.Timestamp(
                datetime.date.today() if self.is_daily else datetime.datetime.now()
            )
        if type(end) is datetime.date:
            return pd.Timestamp(
                datetime.datetime.combine(end, datetime.time.max), tz=tz
            )
        ts = pd.Timestamp(end)
        if tz:
            return (ts if ts.tzinfo else ts.tz_localize(tz)).tz_convert(tz)
        return ts.tz_localize(None) if ts.tzinfo else ts

    def _incomplete_threshold(
        self, tz: datetime.tzinfo | None = None
    ) -> pd.Timestamp:
        """Bars at or after this naive timestamp may still be updating.

        For intraday data with a timezone, uses ``now(tz)`` so the threshold
        is in the configured timezone's wall-clock time — matching the naive
        timestamps used throughout internal comparisons.
        """
        if self.is_daily:
            return pd.Timestamp(datetime.date.today())
        now = pd.Timestamp.now(tz).tz_localize(None) if tz else pd.Timestamp.now()
        return now - pd.Timedelta(minutes=self.bar_minutes)

    def _is_fresh(
        self, last: pd.Timestamp, end: pd.Timestamp, tz: datetime.tzinfo | None = None
    ) -> bool:
        threshold = self._incomplete_threshold(tz)
        backoff = (
            datetime.timedelta(days=1)
            if self.is_daily
            else datetime.timedelta(minutes=self.bar_minutes)
        )
        safe = last - backoff if last >= threshold else last
        return safe.date() >= end.date() if self.is_daily else safe >= end

    # ── storage ───────────────────────────────────────────────────

    def _lock_for(self, symbol: str) -> Any:
        with self._meta_lock:
            return self._locks.setdefault(symbol, self._lock_class())

    def _read(self, symbol: str, date_range: tuple) -> pd.DataFrame:
        if not self._lib.has_symbol(symbol):
            return pd.DataFrame()
        try:
            return _normalize(self._lib.read(symbol, date_range=date_range).data)
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
        """Exclude the incomplete bar, localize to configured tz, then upsert."""
        tz = self._get_tz(symbol)
        threshold = self._incomplete_threshold(tz)
        if not df.empty and df.index[-1] >= threshold:
            df = df.iloc[:-1]
        if df.empty:
            return
        if tz and isinstance(df.index, pd.DatetimeIndex) and df.index.tz is None:
            df = df.set_axis(df.index.tz_localize(tz))
        rows = len(df)

        def write():
            try:
                with self._lock_for(symbol):
                    self._lib.update(symbol, df, upsert=True)
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
        querying ArcticDB.  The returned DataFrame is tz-naive, with
        timestamps in the configured timezone's wall-clock time.
        """
        tz = self._get_tz(symbol)
        end_ts = self._resolve_end(end, tz)
        if count is None:
            count = self.default_count
        if count <= 0:
            return pd.DataFrame()
        start_ts = end_ts - pd.Timedelta(minutes=count * self.bar_minutes * 2)
        existing = self._read(symbol, (start_ts, end_ts))
        end_naive = end_ts.tz_localize(None) if tz else end_ts

        # Cache miss
        if existing.empty:
            log.info("miss %s, fetching %d bars", symbol, count)
            df = _normalize(self._fetch(symbol, end_ts, count))
            if not df.empty:
                self._store(symbol, df)
            return _trim(df, end_naive, count)

        # Fresh
        last = existing.index[-1]
        if self._is_fresh(last, end_naive, tz):
            return _trim(existing, end_naive, count)

        # Incremental update
        gap = int((end_naive - last).total_seconds() / 60 / self.bar_minutes) + 1
        log.info("update %s: %s -> %s", symbol, last.date(), end_naive.date())
        new = _normalize(self._fetch(symbol, end_ts, gap))
        if new.empty:
            return _trim(existing, end_naive, count)

        new = new.loc[new.index >= last]
        if last in new.index and new.loc[last].equals(existing.loc[last]):
            new = new.iloc[1:]
        if new.empty:
            return _trim(existing, end_naive, count)

        self._store(symbol, new)
        return _trim(_normalize(pd.concat([existing, new])), end_naive, count)
