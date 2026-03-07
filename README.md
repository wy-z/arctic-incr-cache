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

## Constructor parameters

| Parameter | Required | Description |
|---|---|---|
| `library` | yes | ArcticDB library instance |
| `fetch(symbol, end, count)` | yes | Fetch raw data from upstream; must return tz-aware timestamps |
| `get_tz(symbol)` | yes | Market timezone (`tzinfo`) for each symbol |
| `bar_minutes` | no | Bar width in minutes (default 1440 = daily) |
| `default_count` | no | Bars returned when `count` is omitted (default 252) |
| `spawn` | no | Fire-and-forget callable for async writes (default: daemon thread) |
| `lock_class` | no | Lock constructor (default: `threading.Lock`) |

## License

MIT
