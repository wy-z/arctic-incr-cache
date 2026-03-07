"""Timezone handling scenarios + tz-specific steps."""

import datetime
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

import pandas as pd
from pytest_bdd import given, parsers, scenarios, then, when

from arctic_incr_cache import IncrCache

from .conftest import intraday_df

scenarios("timezone.feature")


# ── given ─────────────────────────────────────────────────────────


@given(parsers.parse('an upstream source with {n:d} minute bars from "{start}"'))
def _upstream_minute_bars(ctx, n, start):
    ctx["fetch_data"] = intraday_df(start, n)


@given(
    parsers.parse(
        'an ArcticDB library with {n:d} tz-aware minute bars in "{tz_name}" from "{start}"'
    ),
)
def _lib_with_tz_bars(ctx, n, tz_name, start):
    tz = ZoneInfo(tz_name)
    raw = intraday_df(start, n)
    raw.index = raw.index.tz_localize(tz)
    ctx["lib"].has_symbol.return_value = True
    ctx["lib"].read.return_value.data = raw


# ── when ──────────────────────────────────────────────────────────


@when(
    parsers.parse(
        'I request {count:d} bars for "{symbol}" ending "{end}" with timezone "{tz_name}"'
    ),
)
def _request_with_tz(ctx, count, symbol, end, tz_name):
    tz = ZoneInfo(tz_name)
    fetch = MagicMock(return_value=ctx.get("fetch_data", pd.DataFrame()))
    cache = IncrCache(
        ctx["lib"], fetch, bar_minutes=1, default_count=1950,
        get_tz=lambda _, t=tz: t,
    )
    ctx["result"] = cache.get(
        symbol, end=datetime.datetime.fromisoformat(end), count=count
    )


# ── then ──────────────────────────────────────────────────────────


@then(parsers.parse('the stored data has timezone "{tz_name}"'))
def _stored_has_tz(ctx, tz_name):
    stored = ctx["lib"].update.call_args[0][1]
    assert stored.index.tz is not None
    assert str(stored.index.tz) == tz_name


@then("the result index is tz-naive")
def _result_is_naive(ctx):
    assert ctx["result"].index.tz is None


@then("the result timestamps are in New York wall-clock time")
def _result_in_ny_time(ctx):
    assert ctx["result"].index[0] < pd.Timestamp("2024-01-15 12:00:00")
