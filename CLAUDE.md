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

- **Storage** — data is stored in ArcticDB in the configured timezone (e.g., `America/New_York`), via `_store()` calling `tz_localize(tz)`.
- **Queries** — all request parameters (`end`) are converted to the configured timezone by `_resolve_end()` before reading or comparing. Naive datetime inputs are interpreted as the configured timezone; aware inputs are converted to it.
- **Internal processing** — after reading from ArcticDB, `_normalize()` strips the timezone (keeping wall-clock time), so all comparisons happen in tz-naive configured-timezone time.
- **Fetch contract** — `fetch()` must return a tz-naive DataFrame whose timestamps represent wall-clock time in the configured timezone.

When `get_tz` returns `None` (daily data), everything stays tz-naive.

### Other design details

- `is_daily` is derived from `bar_minutes >= 1440` — no separate type flag
- `_incomplete_threshold()` determines the cutoff for "still updating" bars (today for daily, now minus bar width for intraday) — excluded from storage but included in returned results
- Storage writes are fire-and-forget (daemon thread by default), with per-symbol locks
- `_normalize` and `_trim` are module-level pure functions

## Ruff Config

Target: Python 3.12. Lint rules: `E, F, I, W, UP, B, SIM, ARG`.
