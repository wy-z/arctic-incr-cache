# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

ArcticDB-backed time series cache with incremental updates. First call fetches the full window via a user-provided `fetch` callable and stores it in ArcticDB. Subsequent calls only fetch the gap between the cached tail and the requested end, then merge and upsert. Incomplete (still-updating) bars are excluded from storage.

## Commands

```bash
uv sync --group dev          # install dependencies
uv run pytest                # run all tests
uv run pytest tests/test_cache.py::TestCacheMiss::test_fetches_and_stores  # single test
uv run ruff check src/ tests/       # lint
uv run ruff format --check src/ tests/  # format check
```

## Architecture

Single-module library: `src/arctic_incr_cache/cache.py`.

**`IncrCache`** — the only public class. Instantiated with an ArcticDB library and a `fetch` callable. No global state, no abstract base class.

### Cache flow (`get()`)

1. Read existing data from ArcticDB for the symbol
2. **Miss** — nothing cached: call `fetch()`, store, return
3. **Fresh** — cached data covers the requested range: return from cache
4. **Stale** — compute gap size, fetch only new bars, merge with existing, upsert

### Timezone model

When `get_tz` returns a timezone for a symbol:

- **Fetch contract** — `fetch()` must return a tz-aware DataFrame. `_normalize(df, tz)` converts to the configured market timezone.
- **Storage** — data is stored in ArcticDB as tz-aware in the configured timezone. `_store()` receives already-converted data from `_normalize()`.
- **Queries** — `end` parameter: naive datetime is interpreted as **local timezone**, then converted to market timezone via `_resolve_end()`. Aware inputs are converted directly.
- **Return** — `get()` returns a tz-aware DataFrame in the configured timezone.
- **Internal processing** — all comparisons happen in tz-aware configured-timezone time.

`get_tz` is required — every symbol must have a configured timezone.

### Other design details

- `is_daily` is derived from `bar_minutes >= 1440` — no separate type flag
- `_incomplete_threshold()` determines the cutoff for "still updating" bars (today for daily, now minus bar width for intraday) — excluded from storage but included in returned results
- Storage writes are fire-and-forget (daemon thread by default), with per-symbol locks
- `_normalize` and `_trim` are module-level pure functions

## Ruff Config

Target: Python 3.12. Lint rules: `E, F, I, W, UP, B, SIM, ARG`.
