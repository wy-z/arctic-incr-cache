"""Incremental update scenarios + overlap-specific steps."""

from zoneinfo import ZoneInfo

import pandas as pd
from pytest_bdd import given, parsers, scenarios, then

from .conftest import daily_df

_UTC = ZoneInfo("UTC")

scenarios("incremental_update.feature")


@given(
    parsers.parse(
        'an upstream source with {n:d} daily bars from "{start}" starting at value {v:d}'
    ),
)
def _upstream_with_value(ctx, n, start, v):
    ctx["fetch_data"] = daily_df(start, n, value_start=v)


@given(
    parsers.parse(
        'an upstream source returning the last cached row unchanged'
        ' plus {n:d} new bars from "{start}"'
    ),
)
def _upstream_unchanged_overlap(ctx, n, start):
    overlap = ctx["cached"].iloc[[-1]]
    ctx["fetch_data"] = pd.concat([overlap, daily_df(start, n, value_start=500)])


@given(
    parsers.parse(
        'an upstream source returning the last cached row changed'
        ' plus {n:d} new bars from "{start}"'
    ),
)
def _upstream_changed_overlap(ctx, n, start):
    changed = pd.DataFrame(
        {"value": [999]},
        index=pd.DatetimeIndex([ctx["cached"].index[-1]]),
    )
    ctx["fetch_data"] = pd.concat([changed, daily_df(start, n, value_start=500)])


# ── scenario-specific then ────────────────────────────────────────


@then(parsers.parse('the stored data does not contain "{date}"'))
def _stored_missing(ctx, date):
    stored = ctx["lib"].update.call_args[0][1]
    assert pd.Timestamp(date, tz=_UTC) not in stored.index


@then(parsers.parse('the stored data contains "{date}"'))
def _stored_has(ctx, date):
    stored = ctx["lib"].update.call_args[0][1]
    assert pd.Timestamp(date, tz=_UTC) in stored.index
