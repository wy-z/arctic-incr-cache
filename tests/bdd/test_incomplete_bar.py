"""Incomplete bar exclusion scenario + today-specific steps."""

from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

import pandas as pd
from pytest_bdd import given, scenarios, then, when

from arctic_incr_cache import IncrCache

from .conftest import daily_df

_UTC = ZoneInfo("UTC")

scenarios("incomplete_bar.feature")


@given("an upstream source with 15 daily bars ending today")
def _upstream_ending_today(ctx):
    today_utc = pd.Timestamp.now(_UTC).normalize()
    start = (today_utc - pd.Timedelta(days=14)).date()
    ctx["fetch_data"] = daily_df(start, 15)


@when('I request 10 bars for "S" with no end date')
def _request_no_end(ctx):
    fetch = MagicMock(return_value=ctx.get("fetch_data", pd.DataFrame()))
    cache = IncrCache(
        ctx["lib"], fetch, get_tz=lambda _: _UTC, bar_minutes=1440, default_count=252,
        cache_ttl=0,
    )
    ctx["result"] = cache.get("S", count=10)


@then("the stored data does not include today")
def _stored_excludes_today(ctx):
    stored = ctx["lib"].update.call_args[0][1]
    assert stored.index[-1] < pd.Timestamp.now(_UTC).normalize()
