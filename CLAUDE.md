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
2. **Miss** — nothing read: call `fetch()` and offer it to `_store()`. A read error reports an empty frame too (`_read` swallows it), so the symbol may hold rows the fetch would replace
3. **Holey** — the stored window fails `is_holey`: full fetch, offered to `_store()`, merged over the stored rows
4. **Short** — fewer rows than requested and no valid floor: full fetch, offered to `_store()`, and the floor recorded when the source stays short
5. **Fresh** — cached data covers the requested range: return from cache
6. **Stale** — compute gap size, fetch only new bars, merge with existing, offer to `_store()`

A fetched frame is never withheld from the caller. The source is
authoritative: whatever it returns is the data, and a cache that second-
guesses it has no higher court to appeal to — it can only ask again, which is
a loop with no exit. Bad source data is the consumer's problem. The frame is
still graded for *persistence* — see the write guard below — but a refused
write returns the frame all the same.

### Timezone model

When `get_tz` returns a timezone for a symbol:

- **Fetch contract** — `fetch()` must return a tz-aware DataFrame. `_normalize(df, tz)` converts to the configured market timezone.
- **Storage** — data is stored in ArcticDB as tz-aware in the configured timezone. `_store()` receives already-converted data from `_normalize()`.
- **Queries** — `end` parameter: naive datetime is interpreted as **local timezone**, then converted to market timezone via `_resolve_end()`. Aware inputs are converted directly.
- **Return** — `get()` returns a tz-aware DataFrame in the configured timezone.
- **Internal processing** — all comparisons happen in tz-aware configured-timezone time.

`get_tz` is required — every symbol must have a configured timezone.

### Continuity model

`is_holey(symbol, df)` buys **one invariant: every frame this cache writes
passes it, and every stored window it reads is checked.** Not "the store is
clean" — a hole another writer left survives a re-fetch that carries the same
hole, since the refused write leaves the old rows where they are, and a hole
can open at the seam between two contiguous writes without either frame
failing the hook.
`_has_hole()` guards the hook with the 2-bar minimum — a shorter frame has no
interior — and otherwise delegates the whole verdict, slack included; the
cache owns no tolerance of its own.

Two consult sites, one at each end of the store:

- `_store()`, on the frame being written, after the incomplete bar is dropped.
  Holey → refused, no exceptions. ArcticDB `update` replaces the frame's whole
  span, so writing one deletes every cached row inside its gaps; and storing
  it would only have the next read fetch the same window again.
- `_do_get()`, on the trimmed stored window, before the short and stale
  branches. Holey → full re-fetch. No frame written here contained that gap;
  only the source can say what belongs in it.

A refused frame is still served. Refusing a write never drops data.

**The accepted cost.** A window the hook rejects that the source cannot supply
either is re-asked once per `cache_ttl` per reader, indefinitely. One
production incident measured 79 re-fetches of a 12-year daily window in twelve
hours, but that is one consumer's window and refresh cadence, not a ceiling:
the bill is (readers × distinct windows × refreshes), and a consumer asking
for 24 years on a six-minute cron pays far more. There
is no bound the cache can apply that is not a guess about a question only the
source can answer; three attempts at one (a cooldown, a depth ladder, a
stored-state fingerprint) each left the loop reachable another way. Resolution
is repairing the store, or fixing a hook grading a complete frame as
incomplete — that one was the real cause of the production case, and both are
outside this library.

### Other design details

- `is_daily` is derived from `bar_minutes >= 1440` — no separate type flag
- `_incomplete_threshold()` determines the cutoff for "still updating" bars (today for daily, now minus bar width for intraday) — excluded from storage but included in returned results
- Storage writes are fire-and-forget (daemon thread by default), with per-symbol locks
- `_normalize` and `_trim` are module-level pure functions

## Ruff Config

Target: Python 3.12. Lint rules: `E, F, I, W, UP, B, SIM, ARG`.
