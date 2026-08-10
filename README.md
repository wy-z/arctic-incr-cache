# arctic-incr-cache

ArcticDB-backed time series cache with **incremental updates**.

First call fetches the full window from your data source and stores it in
ArcticDB.  Subsequent calls only fetch the gap between the cached tail and
the requested end — then merge and upsert.  Incomplete (still-updating) bars
are automatically excluded from storage so they never overwrite finalised data.

## Install

```bash
pip install arctic-incr-cache
# or
uv add arctic-incr-cache
```

## Quick start

```python
import datetime
import arcticdb as adb
from zoneinfo import ZoneInfo
from arctic_incr_cache import IncrCache

arctic = adb.Arctic("lmdb://data/arcticdb")
lib = arctic.get_library("ohlcv-1d", create_if_missing=True)

cache = IncrCache(
    lib,
    fetch=lambda symbol, end, count: your_api.get_daily_bars(symbol, end=end, count=count),
    get_tz=lambda symbol: ZoneInfo("America/New_York"),
)

df = cache.get("AAPL", end=datetime.date(2024, 6, 1), count=60)
```

- **First call** — fetches 60 bars from your API, stores in ArcticDB, returns.
- **Second call** (same or later end) — serves from ArcticDB; fetches only the
  gap if the cache is stale.

## Intraday data

Set `bar_minutes` to the bar width and provide `get_tz` to return the market
timezone:

```python
from zoneinfo import ZoneInfo

intraday = IncrCache(
    lib,
    fetch=lambda symbol, end, count: your_api.get_minute_bars(symbol, end=end, count=count),
    bar_minutes=1,
    default_count=390 * 5,
    get_tz=lambda symbol: ZoneInfo("America/New_York"),
)
```

## Concurrency

By default writes run in a daemon thread.  Pass `spawn` and `lock_class` for
gevent or other async runtimes:

```python
import gevent
import gevent.lock

cache = IncrCache(
    lib,
    fetch=my_fetch,
    get_tz=lambda symbol: ZoneInfo("America/New_York"),
    spawn=gevent.spawn,
    lock_class=gevent.lock.BoundedSemaphore,
)
```

## Timezone handling

When `get_tz` returns a timezone for a symbol:

- **`fetch` return** — must be tz-aware. Timestamps are converted to the configured market timezone internally.
- **Storage** — data is stored in ArcticDB as tz-aware in the configured timezone.
- **Return** — `get()` returns a tz-aware DataFrame in the configured timezone.
- **`end` parameter** — `date` becomes end-of-day in market timezone; naive `datetime` is interpreted as **local timezone**, then converted; tz-aware is converted directly.

## Interval convention

`end` is a bar timestamp (a point), not a range boundary.

- **Filter** — closed: `index <= end`. `fetch()` must follow the same rule (`start <= ts <= end`); a strict `<` silently drops `bar@end`.
- **Freshness**:
  - **Daily** — closed: `last.date() >= end.date()`.
  - **Intraday** — right-open: `last >= end - bar_width`. An intraday bar at `t` covers `[t, t+bar_width)`, so `bar@end` doesn't exist at session boundaries (e.g. 16:00 close, 20:00 POST end).

A still-updating bar (`now` daily; within `bar_width` intraday) counts as one bar older for freshness — mirroring its exclusion from storage.

## Continuity

The cache never invents continuity the source doesn't have — but it must not
*lose* bars the source does have (a partial write, a fetch that skipped the
cached tail). Mid-series hole detection is **opt-in**: supply `is_holey` and
every delivered window is put to it.

```python
import math

import exchange_calendars as xcals

xnys = xcals.get_calendar("XNYS")


def is_holey(symbol: str, df: pd.DataFrame) -> bool:
    expected = len(xnys.sessions_in_range(df.index[0].date(), df.index[-1].date()))
    return expected - len(df) > max(math.ceil(expected * 0.05), 1)


cache = IncrCache(
    lib,
    fetch=my_fetch,
    get_tz=lambda symbol: ZoneInfo("America/New_York"),
    is_holey=is_holey,
)
```

- **`is_holey(symbol, df)`** — True when `df` misses bars its source should
  hold over its own span, `df.index[0]..df.index[-1]`, tz-aware in the
  configured timezone. Build it on a trading calendar
  (e.g. [exchange_calendars](https://github.com/gerrymanoim/exchange_calendars))
  or a heuristic. Frames under 2 bars have no interior and never reach it.
  Defaults to never holey, which disables the check.

The cache keeps no tolerance of its own — how much the source may
legitimately miss is a property of that source, so the whole verdict lives in
your hook.

A hole has two consequences:

- **Read side** — a delivered window missing more bars than tolerated
  triggers a full re-fetch, validated the same way.
- **Write side** — no frame that fails the check is written, whatever path
  produced it (first fetch, backfill, gap merge, repair). It is still
  served; it just never reaches the store, because `update` would delete the
  good rows sitting inside its gaps (see below).
- **The seam** — a gap fetch is checked *before* its overlap row with the
  cached tail is deduplicated, so a hole between the cached tail and the
  first genuinely new bar is caught too. That hole lives in neither frame
  alone, only in the join.

A gap fetch that fails any of these is upgraded to a validated full
re-fetch. One part of the gate needs no hooks at all: if the fetch window
covered the cached tail but the earliest bar returned lands *after* it, the
source skipped that tail and storing the result would fabricate a hole.

Calibrate `is_holey` against what the source really delivers. Call a frame
holey that the source can never improve on and every window looks corrupt:
the data is served but never cached, and the backfill floor that normally
suppresses hopeless re-fetches is never recorded — costing one full re-fetch
per result-TTL window, indefinitely. When in doubt, widen the slack or leave
`is_holey` at its default.

## Storage semantics

Stores use ArcticDB `update`, which **replaces the entire span** between the
first and last timestamp of the stored frame: cached rows inside that span
that the new fetch doesn't contain are deleted. Every write is therefore a
potential deletion, which is why a frame failing the [continuity](#continuity)
check is refused rather than written.

With continuity disabled (the default) there is nothing to check against:
fetch is the source of truth, and a fetch with holes (e.g. an upstream
returning partial data) erases the cached rows in those holes. Make `fetch()`
return complete data for the range it covers, or return empty on failure
rather than a partial result.

## Index convention

`fetch()` must return a DataFrame with a **`DatetimeIndex`** as the index — this is
the time axis for all cache operations (querying, merging, freshness checks).
ArcticDB's `date_range` queries operate on the index, so no column name
configuration is needed. If your data source returns time as a regular column,
call `df.set_index("date")` (or similar) inside your `fetch` function.

Anything else — a tz-naive index, or an index that isn't a `DatetimeIndex` at
all — raises `ValueError` before the frame can be stored.

## Constructor parameters

| Parameter | Required | Description |
|---|---|---|
| `library` | yes | ArcticDB library instance |
| `fetch(symbol, end, count)` | yes | Fetch raw data from upstream; must return tz-aware timestamps |
| `get_tz(symbol)` | yes | Market timezone (`tzinfo`) for each symbol |
| `is_holey(symbol, df)` | no | True when the frame misses bars over its own span; default `False` disables continuity checks |
| `bar_minutes` | no | Bar width in minutes (default 1440 = daily) |
| `default_count` | no | Bars returned when `count` is omitted (default 252) |
| `spawn` | no | Fire-and-forget callable for async writes (default: daemon thread) |
| `lock_class` | no | Lock constructor (default: `threading.Lock`) |

## License

MIT
