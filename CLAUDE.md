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
2. **Miss** — nothing read: call `fetch()`, validate, store only if clean. A read error reports an empty frame too (`_read` swallows it), so the symbol may hold rows the fetch would replace
3. **Corrupt** — `corrupt_reason()` finds a mid-series hole in the delivered window: `refetch_full()` repairs it
4. **Short** — fewer rows than requested and no valid floor: full fetch, validated like a repair (it is a full-window fetch too). A corrupt backfill is served but neither stored nor floored — it is no evidence of how deep the source goes. A clean one is stored, and records the floor when the source stays short
5. **Fresh** — cached data covers the requested range: return from cache
6. **Stale** — compute gap size, fetch only new bars, merge with existing, upsert. A gap fetch that skips the cached tail is never stored (would fabricate a hole) — upgraded to `refetch_full()`

`corrupt_reason()` and `refetch_full()` are closures in `_do_get`: the same validator decides whether cached data is broken and whether a repair is safe to store.

### Timezone model

When `get_tz` returns a timezone for a symbol:

- **Fetch contract** — `fetch()` must return a tz-aware DataFrame. `_normalize(df, tz)` converts to the configured market timezone.
- **Storage** — data is stored in ArcticDB as tz-aware in the configured timezone. `_store()` receives already-converted data from `_normalize()`.
- **Queries** — `end` parameter: naive datetime is interpreted as **local timezone**, then converted to market timezone via `_resolve_end()`. Aware inputs are converted directly.
- **Return** — `get()` returns a tz-aware DataFrame in the configured timezone.
- **Internal processing** — all comparisons happen in tz-aware configured-timezone time.

`get_tz` is required — every symbol must have a configured timezone.

### Continuity model

Opt-in via the `is_holey(symbol, df)` constructor callable (default returns False = disabled). `_has_hole()` guards it with the 2-bar minimum — a shorter frame has no interior — and otherwise delegates the whole verdict, slack included; the cache owns no tolerance of its own. It is applied on a different frame at each of:

- **Read side** — `corrupt_reason()` checks the *delivered window* (`trim(existing)`, not the whole store) to decide whether to repair
- **Write side** — `_store()` checks the *frame being written*, after the incomplete bar is dropped, and refuses a holey one
- **Seam** — the incremental branch checks the gap fetch *before* the overlap row is deduplicated, so its span still reaches the cached tail. A hole between the cached tail and the first new bar exists in neither frame alone and would otherwise be invisible

Each is needed: ArcticDB `update` replaces the stored frame's whole span, so the write check is what stops a holey frame from deleting good rows — and it covers every store path (miss, backfill, gap merge, repair) instead of relying on each caller to validate. A refused frame is still served, just not stored. The discontinuous-fetch write gate runs regardless of the hooks. The cache guarantees fidelity to the fetch source, never continuity the source doesn't have.

### Other design details

- `is_daily` is derived from `bar_minutes >= 1440` — no separate type flag
- `_incomplete_threshold()` determines the cutoff for "still updating" bars (today for daily, now minus bar width for intraday) — excluded from storage but included in returned results
- Storage writes are fire-and-forget (daemon thread by default), with per-symbol locks
- `_normalize` and `_trim` are module-level pure functions

## Ruff Config

Target: Python 3.12. Lint rules: `E, F, I, W, UP, B, SIM, ARG`.
