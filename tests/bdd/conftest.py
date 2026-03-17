"""Shared fixtures, helpers, and common steps for BDD tests."""

import datetime
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

import pandas as pd
import pytest
from pytest_bdd import given, parsers, then, when

from arctic_incr_cache import IncrCache

_UTC = ZoneInfo("UTC")


# ── helpers ───────────────────────────────────────────────────────


def daily_df(start, n, value_start=100):
    dates = pd.date_range(start=start, periods=n, freq="D", tz=_UTC)
    return pd.DataFrame({"value": range(value_start, value_start + n)}, index=dates)


def intraday_df(start, n):
    times = pd.date_range(start=start, periods=n, freq="1min", tz=_UTC)
    return pd.DataFrame({"price": range(n)}, index=times)


# ── fixtures ──────────────────────────────────────────────────────


@pytest.fixture()
def ctx():
    lib = MagicMock()
    lib.has_symbol.return_value = False
    return {"lib": lib}


# ── shared given ──────────────────────────────────────────────────


@given("an empty ArcticDB library")
def _empty_lib():
    pass  # ctx fixture already defaults to empty


@given("an upstream source with no data")
def _upstream_empty(ctx):
    ctx["fetch_data"] = pd.DataFrame()


@given(parsers.parse('an upstream source with {n:d} daily bars from "{start}"'))
def _upstream_daily(ctx, n, start):
    ctx["fetch_data"] = daily_df(start, n)


@given(parsers.parse('an ArcticDB library with {n:d} daily bars from "{start}"'))
def _lib_with_daily(ctx, n, start):
    cached = daily_df(start, n)
    ctx["lib"].has_symbol.return_value = True
    ctx["lib"].read.return_value.data = cached
    ctx["cached"] = cached


# ── shared when ───────────────────────────────────────────────────


@when(parsers.parse('I request {count:d} bars for "{symbol}" ending "{end}"'))
def _request_bars(ctx, count, symbol, end):
    fetch = MagicMock(return_value=ctx.get("fetch_data", pd.DataFrame()))
    ctx["fetch"] = fetch
    cache = IncrCache(
        ctx["lib"], fetch, get_tz=lambda _: _UTC, bar_minutes=1440, default_count=252,
        cache_ttl=0,
    )
    ctx["result"] = cache.get(
        symbol, end=datetime.date.fromisoformat(end), count=count
    )


# ── shared then ───────────────────────────────────────────────────


@then(parsers.parse("the result has {n:d} rows"))
def _result_has_n_rows(ctx, n):
    assert len(ctx["result"]) == n


@then("the result is empty")
def _result_is_empty(ctx):
    assert ctx["result"].empty


@then("the data is stored in ArcticDB")
def _data_stored(ctx):
    ctx["lib"].update.assert_called_once()


@then("nothing is stored in ArcticDB")
def _nothing_stored(ctx):
    ctx["lib"].update.assert_not_called()


@then("the upstream was not called")
def _upstream_not_called(ctx):
    ctx["fetch"].assert_not_called()
