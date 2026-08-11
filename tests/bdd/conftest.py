"""BDD support: an in-memory ArcticDB stand-in, and every step.

The unit tests drive ``IncrCache`` against mocks, one branch at a time.  These
scenarios drive it against a store that behaves like ArcticDB — ``update``
replacing the whole span the written frame covers — so a rule that only shows
itself across two reads (a floor, a seam, a hole) can be stated as one.
"""

import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

import pandas as pd
import pytest
from pytest_bdd import given, parsers, then, when

from arctic_incr_cache import IncrCache

_UTC = ZoneInfo("UTC")


# ── the store ─────────────────────────────────────────────────────


class FakeLibrary:
    """In-memory stand-in for an ArcticDB library.

    ``update`` replaces every row between the written frame's first and last
    timestamp — the semantic the write guard exists for.
    """

    def __init__(self):
        self.frames: dict[str, pd.DataFrame] = {}
        self.writes: list[pd.DataFrame] = []

    def has_symbol(self, symbol: str) -> bool:
        return symbol in self.frames

    def read(self, symbol: str, date_range: tuple | None = None):
        df = self.frames[symbol]
        if date_range:
            start, end = date_range
            df = df.loc[(df.index >= start) & (df.index <= end)]
        return SimpleNamespace(data=df)

    def update(self, symbol: str, df: pd.DataFrame, **_) -> None:
        self.writes.append(df)
        held = self.frames.get(symbol)
        if held is not None:
            outside = held.loc[(held.index < df.index[0]) | (held.index > df.index[-1])]
            df = pd.concat([outside, df]).sort_index()
        self.frames[symbol] = df


# ── helpers ───────────────────────────────────────────────────────


def daily_df(start, n, value_start=100):
    dates = pd.date_range(start=start, periods=n, freq="D", tz=_UTC)
    return pd.DataFrame({"value": range(value_start, value_start + n)}, index=dates)


def intraday_df(start, n, tz=_UTC):
    times = pd.date_range(start=start, periods=n, freq="1min", tz=tz)
    return pd.DataFrame({"price": range(n)}, index=times)


def punched_df(start, end, gap_start, gap_end):
    """Daily bars over *start*..*end* with *gap_start*..*gap_end* missing."""
    idx = pd.date_range(start=start, end=end, freq="D", tz=_UTC)
    idx = idx[
        (idx < pd.Timestamp(gap_start, tz=_UTC))
        | (idx > pd.Timestamp(gap_end, tz=_UTC))
    ]
    return pd.DataFrame({"value": range(100, 100 + len(idx))}, index=idx)


def _gapless(_, df):
    """Continuity hook: every calendar day in the frame's span, no slack."""
    return len(df) < (df.index[-1].date() - df.index[0].date()).days + 1


def _cache(ctx) -> IncrCache:
    """The cache under test, built on first use so every ``given`` lands first.

    Kept in *ctx* because scenarios read twice: the second read is what sees
    a floor, or a seam that only exists in the store.
    """
    if "cache" not in ctx:
        tz = ctx.get("tz", _UTC)
        ctx["fetch"] = MagicMock(return_value=ctx["fetch_data"])
        ctx["cache"] = IncrCache(
            ctx["lib"],
            ctx["fetch"],
            get_tz=lambda _: tz,
            cache_ttl=0,  # every request is a real read
            spawn=lambda fn: fn(),  # write before the next step asserts
            **ctx["config"],
        )
    return ctx["cache"]


def _written(ctx) -> pd.DataFrame:
    assert ctx["lib"].writes, "nothing was written"
    return ctx["lib"].writes[-1]


@pytest.fixture()
def ctx():
    return {"lib": FakeLibrary(), "config": {}, "fetch_data": pd.DataFrame()}


# ── given: configuration ──────────────────────────────────────────


@given("a continuity hook that expects every calendar day")
def _hook(ctx):
    ctx["config"]["is_holey"] = _gapless


@given(parsers.parse('a market timezone of "{tz_name}"'))
def _market_tz(ctx, tz_name):
    ctx["tz"] = ZoneInfo(tz_name)


@given("minute bars")
def _minute_bars(ctx):
    ctx["config"]["bar_minutes"] = 1


# ── given: the store ──────────────────────────────────────────────


@given("an empty store")
def _empty_store():
    pass  # the ctx fixture starts empty


@given(parsers.parse('a store holding {n:d} daily bars from "{start}"'))
def _store_daily(ctx, n, start):
    ctx["lib"].frames["S"] = daily_df(start, n)


@given(
    parsers.parse(
        'a store holding daily bars from "{start}" to "{end}"'
        ' missing "{gap_start}".."{gap_end}"'
    )
)
def _store_punched(ctx, start, end, gap_start, gap_end):
    ctx["lib"].frames["S"] = punched_df(start, end, gap_start, gap_end)


@given(parsers.parse('a store holding {n:d} minute bars in "{tz_name}" from "{start}"'))
def _store_minutes(ctx, n, tz_name, start):
    ctx["lib"].frames["S"] = intraday_df(start, n, ZoneInfo(tz_name))


# ── given: the source ─────────────────────────────────────────────


@given("an upstream source with no data")
def _source_empty():
    pass  # the ctx fixture starts empty


@given(parsers.parse('an upstream source with {n:d} daily bars from "{start}"'))
def _source_daily(ctx, n, start):
    ctx["fetch_data"] = daily_df(start, n)


@given(
    parsers.parse(
        'an upstream source with {n:d} daily bars from "{start}"'
        " starting at value {value:d}"
    )
)
def _source_daily_valued(ctx, n, start, value):
    ctx["fetch_data"] = daily_df(start, n, value_start=value)


@given(parsers.parse("an upstream source with {n:d} daily bars ending today"))
def _source_ending_today(ctx, n):
    start = (pd.Timestamp.now(_UTC).normalize() - pd.Timedelta(days=n - 1)).date()
    ctx["fetch_data"] = daily_df(start, n)


@given(
    parsers.parse(
        'an upstream source with daily bars from "{start}" to "{end}"'
        ' missing "{gap_start}".."{gap_end}"'
    )
)
def _source_punched(ctx, start, end, gap_start, gap_end):
    ctx["fetch_data"] = punched_df(start, end, gap_start, gap_end)


@given(
    parsers.parse(
        'an upstream source reporting {n:d} minute bars in UTC from "{start}"'
    )
)
def _source_minutes(ctx, n, start):
    ctx["fetch_data"] = intraday_df(start, n)


@given(
    parsers.parse(
        "an upstream source returning the stored tail {state}"
        ' plus {n:d} new bars from "{start}"'
    )
)
def _source_with_overlap(ctx, state, n, start):
    held = ctx["lib"].frames["S"]
    tail = held.iloc[[-1]]
    if state == "changed":
        tail = pd.DataFrame({"value": [999]}, index=tail.index)
    ctx["fetch_data"] = pd.concat([tail, daily_df(start, n, value_start=500)])


# ── when ──────────────────────────────────────────────────────────


def _end(ctx, text):
    """A bare date asks for end-of-day; a wall clock is market-local."""
    if " " not in text:
        return datetime.date.fromisoformat(text)
    return datetime.datetime.fromisoformat(text).replace(tzinfo=ctx.get("tz", _UTC))


@when(parsers.parse('I request {count:d} bars for "{symbol}" ending "{end}"'))
def _request(ctx, count, symbol, end):
    ctx["result"] = _cache(ctx).get(symbol, end=_end(ctx, end), count=count)


@when(parsers.parse('I request {count:d} bars for "{symbol}" with no end date'))
def _request_no_end(ctx, count, symbol):
    ctx["result"] = _cache(ctx).get(symbol, count=count)


# ── then: the result ──────────────────────────────────────────────


@then(parsers.parse("the result has {n:d} rows"))
def _result_rows(ctx, n):
    assert len(ctx["result"]) == n


@then("the result is empty")
def _result_empty(ctx):
    assert ctx["result"].empty


@then(parsers.parse('the result contains "{date}"'))
def _result_contains(ctx, date):
    assert pd.Timestamp(date, tz=_UTC) in ctx["result"].index


@then("the result contains today")
def _result_contains_today(ctx):
    assert ctx["result"].index[-1] == pd.Timestamp.now(_UTC).normalize()


@then(parsers.parse('the result is in "{tz_name}"'))
def _result_tz(ctx, tz_name):
    assert str(pd.DatetimeIndex(ctx["result"].index).tz) == tz_name


# ── then: the source ──────────────────────────────────────────────


@then("the upstream was not called")
def _source_untouched(ctx):
    ctx["fetch"].assert_not_called()


@then("the upstream was called once")
def _source_called_once(ctx):
    ctx["fetch"].assert_called_once()


@then(parsers.parse("the upstream was called {n:d} times"))
def _source_call_count(ctx, n):
    assert ctx["fetch"].call_count == n


@then(parsers.parse("the upstream was asked for {n:d} bars"))
def _source_asked_for(ctx, n):
    _, _, count = ctx["fetch"].call_args[0]
    assert count == n


# ── then: the store ───────────────────────────────────────────────


@then("nothing was stored")
def _nothing_stored(ctx):
    assert not ctx["lib"].writes


@then(parsers.parse('the store holds "{date}"'))
def _store_holds(ctx, date):
    assert pd.Timestamp(date, tz=_UTC) in ctx["lib"].frames["S"].index


@then(parsers.parse('the store does not hold "{date}"'))
def _store_lacks(ctx, date):
    assert pd.Timestamp(date, tz=_UTC) not in ctx["lib"].frames["S"].index


@then("the store does not hold today")
def _store_lacks_today(ctx):
    assert ctx["lib"].frames["S"].index[-1] < pd.Timestamp.now(_UTC).normalize()


@then(parsers.parse('the written frame contains "{date}"'))
def _written_contains(ctx, date):
    assert pd.Timestamp(date, tz=_UTC) in _written(ctx).index


@then(parsers.parse('the written frame does not contain "{date}"'))
def _written_lacks(ctx, date):
    assert pd.Timestamp(date, tz=_UTC) not in _written(ctx).index


@then(parsers.parse('the written frame is in "{tz_name}"'))
def _written_tz(ctx, tz_name):
    assert str(pd.DatetimeIndex(_written(ctx).index).tz) == tz_name
