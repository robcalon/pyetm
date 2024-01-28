"""PeriodIndex related utilities."""
from __future__ import annotations

from datetime import datetime
from typing import Any

import calendar

import pandas as pd


def make_period_index(
    year: int,
    name: str | None = None,
    periods: int | None = None,
    as_datetime: bool = False,
) -> pd.PeriodIndex | pd.DatetimeIndex:
    """Make a PeriodIndex for a specific year.

    Parameters
    ----------
    year : int, default None
        Year for which to construct the index.
    name : str
        Name of the constructed index.
    periods : int, default None
        Number of periods in index. Defaults
        to number of days in year or the
        length of a passed object.
    as_datetime : bool, default False
        Tranform PeriodIndex to DatetimeIndex.

    Return
    ------
    index : pd.PeriodIndex or pd.DatetimeIndex
        The constructed index."""

    # make start period
    start = datetime(year, 1, 1)

    # default name for datetime index
    if (name is None) & as_datetime:
        name = "Datetime"

    # default name for period index
    if (name is None) & (not as_datetime):
        name = "Period"

    # check periods
    if periods is None:
        periods = (365 + calendar.isleap(int(year))) * 24

    # check lenght of object
    if not isinstance(periods, int):
        periods = len(periods)

    # make periodindex
    index = pd.period_range(start=start, periods=periods, freq="h", name=name)

    # convert type
    if as_datetime:
        index = index.astype("datetime64[ns]")

    return index


def validate_profile(series: pd.Series[Any], name: str | None = None) -> pd.Series[Any]:
    """Validate profile object.

    Parameters
    ----------
    series : pd.Series
        Series to be validated.
    name : str
        Name of returned series.

    Return
    ------
    series : pd.Series
        Validates series with optional new
        name and/or PeriodIndex."""

    # squeeze dataframe
    if isinstance(series, pd.DataFrame):
        squeezed = pd.DataFrame(series).squeeze(axis=1)

        # cannot process frames
        if not isinstance(squeezed, pd.Series):
            raise TypeError("Cannot squeeze DataFrame to Series")

        # assign squeezed
        series = squeezed

    # default to series name
    if name is None:
        name = str(series.name)

    # check series lenght
    series = validate_profile_lenght(series, length=8760)

    # # check index type
    # objs = (pd.DatetimeIndex, pd.PeriodIndex)
    # if not isinstance(series.index, objs):
    #     # check for year parameter
    #     if year is None:
    #         raise ValueError(
    #             f"Must specify year for profile '{name}' when "
    #             "passed without pd.DatetimeIndex or pd.PeriodIndex"
    #         )

    #     # assign period index
    #     series.index = make_period_index(year, periods=8760)

    return pd.Series(series, name=name)


def validate_profile_lenght(
    series: pd.Series[Any], name: str | None = None, length: int | None = None
) -> pd.Series[Any]:
    """Validate profile length.

    Parameters
    ----------
    series : pd.Series
        Series to be validated.
    name : str, default None
        Name of series object.
    lenght : int, default None
        Lenght against which to validate
        the series. Defaults to 8760 hours.

    Return
    ------
    series : pd.Series
        Validates series."""

    # get default length
    if length is None:
        length = 8760

    # default name
    if name is None:
        name = str(series.name)

    # check series lenght
    if len(series) != 8760:
        raise ValueError(
            f"Profile '{name}' must contain 8760 values, "
            f"counted '{len(series)}' instead."
        )

    return pd.Series(series, name=name)
