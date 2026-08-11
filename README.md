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

The cache never invents continuity the source doesn't have. What it can do is
notice the absence, at both ends of the store, once you say what "complete"
means for your data.

Supply `is_holey` and you get one invariant: **every frame this cache writes
passes it, and every window it serves from the store is checked.** Not that
the store is clean — a hole another writer left outlives a re-fetch carrying
the same hole, because refusing that write leaves the old rows where they are,
and a hole can open at the seam between two contiguous writes without either
frame failing the hook. Nor that the store is checked beyond what is served:
the read may pull more rows than the ask, and the hook grades the ask.

- **On write** — a holey frame is refused. Storing it would buy nothing (the
  next read finds the hole and fetches the window again) and cost plenty:
  `update` replaces the whole span between a frame's first and last timestamp,
  so writing a holey frame deletes every cached row inside its gaps. That is
  what protects a store something else also writes — another process, a
  scheduled repair job.
- **On read** — a holey stored window is re-fetched. No frame written here
  contained that gap, so only the source can say what belongs in it.

Either way the frame reaches the caller — refusing a write never drops data.
A fetch that comes back empty is the one exception, and it drops nothing
either: an empty answer replaces nothing, so the cached rows go on serving.

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

**`is_holey(symbol, df)`** — True when `df` misses bars its source should hold
over its own span, `df.index[0]..df.index[-1]`, tz-aware in the configured
timezone. Build it on a trading calendar
(e.g. [exchange_calendars](https://github.com/gerrymanoim/exchange_calendars))
or a heuristic. Frames under 2 bars have no interior and never reach it.
Defaults to never holey, which makes both ends inert.

The cache keeps no tolerance of its own — how much the source may
legitimately miss is a property of that source, so the whole verdict lives in
your hook. Size the slack accordingly, and calibrate it per dataset: a grid
that fits daily equity bars will mis-grade a dataset whose per-symbol history
is fragmentary, and there is no appeal.

### The one cost

A window your hook rejects that the source cannot supply either is asked for
again on every read that misses the result cache — once per `cache_ttl` per
reader, indefinitely, and the bill scales with how wide a window and how often
a consumer asks for it. The cache has no way out of that on its own: it can
only ask, and the source has already answered. Resolving it means repairing
the store, or fixing a hook that is grading a complete frame as incomplete.
Both are outside this library.

## Storage semantics

Stores use ArcticDB `update`, which **replaces the entire span** between the
first and last timestamp of the stored frame: cached rows inside that span
that the new fetch doesn't contain are deleted. Every write is therefore a
potential deletion, which is what the `is_holey` write guard exists to stop.

Without the hook, fetch is the source of truth, and a fetch with holes (e.g.
an upstream returning partial data) erases the cached rows in those holes.
Make `fetch()` return complete data for the range it covers, or return empty
on failure rather than a partial result.

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
| `is_holey(symbol, df)` | no | True when the frame misses bars over its own span. Holey frames are never written; a holey stored window is re-fetched |
| `bar_minutes` | no | Bar width in minutes (default 1440 = daily) |
| `default_count` | no | Bars returned when `count` is omitted (default 252) |
| `spawn` | no | Fire-and-forget callable for async writes (default: daemon thread) |
| `lock_class` | no | Lock constructor (default: `threading.Lock`) |
| `floor` | no | Shared dict holding each symbol's known source depth, so a short window isn't re-fetched forever. Pass one to share it across instances — keys are bare symbols, so give every library / bar width its own dict |
| `cache_ttl` | no | Result TTL in seconds (default 60); repeated `get()` calls with the same resolved parameters reuse the result. `0` disables |

## License

MIT
