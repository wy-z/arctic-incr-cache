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


def _normalize(df: pd.DataFrame, tz: datetime.tzinfo) -> pd.DataFrame:
    """Convert to *tz*-aware, deduplicate (keep last), sort.

    tz-naive input raises — every symbol has a configured timezone.
    """
    if df.empty:
        return df
    if isinstance(df.index, pd.DatetimeIndex):
        if df.index.tz is None:
            raise ValueError("fetch() must return tz-aware timestamps, got tz-naive")
        df = df.set_axis(df.index.tz_convert(tz))
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
        is_holey: ``is_holey(symbol, df) -> bool``.
            True when *df* misses bars its source should hold over its own
            span — ``df.index[0]..df.index[-1]``, tz-aware in the configured
            timezone.  Implement with a trading calendar or a heuristic, and
            size the slack for what the source legitimately misses: the
            whole verdict is yours.  Frames shorter than 2 bars never reach
            it — they have no interior to miss.  Defaults to never holey,
            which disables continuity checks.
        bar_minutes: Bar width in minutes (1440 = daily, 1 = 1-min).
        default_count: Bars returned when *count* is omitted.
        spawn: Fire-and-forget callable for async writes.
            Defaults to daemon threads.  Pass ``gevent.spawn`` for async runtimes.
        lock_class: Lock constructor.  Defaults to ``threading.Lock``.
        floor: Shared floor dict for cross-instance state.
            Maps symbol to ``(oldest_ts, expiry, hits)``.  Keys are bare
            symbols — give every dataset (library / bar width) its own
            dict, or one cache's floor will suppress another's backfills.
        cache_ttl: Result TTL in seconds (default 60).
            Repeated ``get()`` calls with the same resolved parameters
            return a cached result within this window.  Set to 0 to disable.
    """

    FLOOR_TTLS = (360, 720, 1440)  # 6/12/24 min backoff, capped at the last
    LOOKBACK = 2  # read-window multiplier over the requested bar count
    MIN_LOOKBACK_DAYS = 7  # cover weekends / short holidays

    def __init__(
        self,
        library: Any,
        fetch: Callable[[str, pd.Timestamp, int], pd.DataFrame],
        *,
        get_tz: Callable[[str], datetime.tzinfo],
        is_holey: Callable[[str, pd.DataFrame], bool] = lambda *_: False,
        bar_minutes: int = 1440,
        default_count: int = 252,
        spawn: Callable[..., Any] | None = None,
        lock_class: type | None = None,
        floor: dict[str, tuple[pd.Timestamp, float, int]] | None = None,
        cache_ttl: int = 60,
    ):
        if bar_minutes <= 0:
            raise ValueError("bar_minutes must be > 0")
        self._lib = library
        self._fetch = fetch
        self._get_tz = get_tz
        self._is_holey = is_holey
        self.bar_minutes = bar_minutes
        self.default_count = default_count
        self._spawn = spawn
        self._lock_class = lock_class or threading.Lock
        self._locks: dict[str, Any] = {}
        self._meta_lock = threading.Lock()
        # Per-symbol (oldest_ts, expiry, hits): skip re-fetch when cache
        # already covers the source's oldest available date.
        self._floor = floor if floor is not None else {}
        self._cached_get: Callable[[str, pd.Timestamp, int], pd.DataFrame]
        if cache_ttl > 0:
            memoized: Any = ring.lru(expire=cache_ttl)(self._do_get)
            # ring hands every caller the same frame — copy so they can mutate
            self._cached_get = lambda *args: memoized(*args).copy()
        else:
            self._cached_get = self._do_get

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

    def _calc_gap_count(self, last: pd.Timestamp, end: pd.Timestamp) -> int:
        """Bars from *last* to *end*, including one overlap bar at *last*
        so callers can refresh the cached tail."""
        if self.is_daily:
            gap_count = (end.date() - last.date()).days + 1
        else:
            bar = pd.Timedelta(minutes=self.bar_minutes)
            gap_count = math.floor((end - last) / bar) + 1
        return max(gap_count, 1)

    # ── floor ─────────────────────────────────────────────────────

    def _set_floor(self, symbol: str, oldest: pd.Timestamp) -> None:
        # Backoff caps at the last TTL rather than going permanent: a floor
        # poisoned by transient empty/short fetches (or a persistent floor
        # dict outliving the source's real depth) must heal on re-probe.
        prev = self._floor.get(symbol)
        hits = (prev[2] if prev else 0) + 1
        ttl = self.FLOOR_TTLS[min(hits, len(self.FLOOR_TTLS)) - 1]
        self._floor[symbol] = (oldest, time.time() + ttl, hits)

    def _at_floor(self, symbol: str, oldest_cached: pd.Timestamp) -> bool:
        """True when the cache already reaches the source's oldest known bar,
        so a short window is the source's depth rather than a gap to fill."""
        entry = self._floor.get(symbol)
        if not entry:
            return False
        oldest, expiry, hits = entry
        if time.time() >= expiry or oldest_cached > oldest:
            return False
        if hits <= len(self.FLOOR_TTLS):
            log.info("floor hit %s: oldest=%s (hits=%d)", symbol, oldest, hits)
        return True

    # ── continuity validation ──────────────────────────────────────

    def _has_hole(self, symbol: str, df: pd.DataFrame) -> bool:
        """True when *df* misses bars over its own span, per ``is_holey``."""
        return len(df) >= 2 and self._is_holey(symbol, df)

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

    def _complete(self, symbol: str, df: pd.DataFrame) -> pd.DataFrame:
        """Drop trailing bars that may still be updating.

        Idempotent, so a caller may prepare a frame and hand that same frame
        to ``_store`` without the second exclusion eating a complete bar.
        """
        if df.empty:
            return df
        return df.loc[df.index < self._incomplete_threshold(self._get_tz(symbol))]

    def _store(self, symbol: str, df: pd.DataFrame) -> None:
        """Exclude still-updating bars, then upsert unless *df* is holey.

        This is the integrity boundary: ``update`` replaces the whole span
        between the frame's first and last timestamp, so a holey frame would
        delete every cached row inside its gaps.  Such a frame is refused,
        not written — callers may serve it, but the store keeps what it has.

        Data must already be tz-aware in the configured timezone (via
        ``_normalize``) before calling this method.
        """
        df = self._complete(symbol, df)
        if df.empty:
            return
        if self._has_hole(symbol, df):
            log.warning(
                "refusing holey write %s: %s..%s holds only %d bars",
                symbol,
                df.index[0],
                df.index[-1],
                len(df),
            )
            return
        rows = len(df)
        span = f"{df.index[0].date()}..{df.index[-1].date()}"

        def write():
            try:
                with self._lock_for(symbol):
                    self._lib.update(
                        symbol, df, upsert=True, prune_previous_versions=True
                    )
                log.info("stored %s %s (+%d rows)", symbol, span, rows)
            except Exception:
                log.exception("write error %s", symbol)

        if self._spawn is not None:
            self._spawn(write)
        else:
            threading.Thread(target=write, daemon=True).start()

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
        return self._cached_get(symbol, end_ts, count)

    def _do_get(self, symbol: str, end_ts: pd.Timestamp, count: int) -> pd.DataFrame:
        tz = self._get_tz(symbol)
        if self.is_daily:
            start_ts = end_ts - pd.Timedelta(days=count * self.LOOKBACK)
        else:
            cal_days = math.ceil(count * self.bar_minutes / 1440) * self.LOOKBACK
            start_ts = end_ts - pd.Timedelta(days=max(cal_days, self.MIN_LOOKBACK_DAYS))
        existing = self._read(symbol, (start_ts, end_ts), tz)

        def trim(df: pd.DataFrame) -> pd.DataFrame:
            return _trim(df, end_ts, count)

        def merge(*dfs: pd.DataFrame) -> pd.DataFrame:
            return trim(_normalize(pd.concat(dfs), tz))

        def corrupt_reason(df: pd.DataFrame) -> str | None:
            """Why *df* is unusable — a mid-series hole in the delivered
            window — or ``None`` when it is clean."""
            window = trim(df)
            if self._has_hole(symbol, window):
                return (
                    f"{window.index[0]}..{window.index[-1]} "
                    f"holds only {len(window)} bars"
                )
            return None

        def refetch_full() -> pd.DataFrame | None:
            """Re-fetch the whole window and store it, or ``None`` when the
            re-fetch is corrupt too and the caller should fall back."""
            df = _normalize(self._fetch(symbol, end_ts, count), tz)
            if df.empty or corrupt_reason(df):
                return None
            self._store(symbol, df)
            return merge(existing, df)

        # Cache miss.  A read error also lands here (``_read`` reports an
        # empty frame), so the symbol may well hold rows this fetch would
        # replace — validate before storing, like every other write path.
        if existing.empty:
            log.info("miss %s, fetching %d bars", symbol, count)
            df = _normalize(self._fetch(symbol, end_ts, count), tz)
            if not df.empty:
                if reason := corrupt_reason(df):
                    log.warning("corrupt fetch %s: %s, not storing", symbol, reason)
                else:
                    self._store(symbol, df)
            return trim(df)

        last = existing.index[-1]
        trimmed = trim(existing)

        # Corrupt cache — repair with a validated full re-fetch
        if reason := corrupt_reason(existing):
            log.warning("corrupt %s: %s, refetching", symbol, reason)
            repaired = refetch_full()
            return trimmed if repaired is None else repaired

        # Short — not enough rows, and the source isn't known to be exhausted
        if len(trimmed) < count and not self._at_floor(symbol, existing.index[0]):
            log.info("short %s: have %d, need %d", symbol, len(trimmed), count)
            df = _normalize(self._fetch(symbol, end_ts, count), tz)
            if df.empty:
                self._set_floor(symbol, existing.index[0])
                return trimmed
            # Same validation as a repair — this is a full-window fetch too.
            # A corrupt backfill is served but never stored, and never
            # floored: it says nothing about how deep the source goes, and
            # flooring on it would suppress the retry.
            if reason := corrupt_reason(df):
                log.warning("corrupt backfill %s: %s", symbol, reason)
                return merge(existing, df)
            storable = self._complete(symbol, df)
            self._store(symbol, storable)
            if len(df) < count and not self._has_hole(symbol, storable):
                self._set_floor(symbol, df.index[0])
            return merge(existing, df)

        # Fresh
        if self.is_fresh(last, end_ts, tz):
            return trimmed

        # Incremental update — fetch only the stale gap (plus overlap) and merge
        gap_count = self._calc_gap_count(last, end_ts)
        fetch_count = min(gap_count, count)  # large gap → full-window fetch
        new = _normalize(self._fetch(symbol, end_ts, fetch_count), tz)
        if new.empty:
            return trimmed

        new = new.loc[new.index >= last]
        if new.empty:
            return trimmed
        # Checked before the overlap is deduplicated, so the span still
        # reaches the cached tail: a hole between ``last`` and the first new
        # bar is invisible in the frame that would be written.  Either the
        # un-truncated fetch skipped ``last`` outright, or it came back gappy
        # — storing it would leave a gap the source may not have.  Re-fetch
        # the full window; if that is corrupt too, serve it unstored.
        skipped_tail = fetch_count == gap_count and new.index[0] > last
        if skipped_tail or self._has_hole(symbol, new):
            log.warning(
                "discontinuous fetch %s: %s..%s vs cached tail %s, refetching",
                symbol,
                new.index[0],
                new.index[-1],
                last,
            )
            repaired = refetch_full()
            return merge(existing, new) if repaired is None else repaired

        if last in new.index and new.loc[last].equals(existing.loc[last]):
            new = new.iloc[1:]
        if new.empty:
            return trimmed

        self._store(symbol, new)
        return merge(existing, new)
