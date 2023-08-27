"""Cooling degree hours, adapted from:
https://ec.europa.eu/eurostat/statistics-explained/index.php?title=Heating_and_cooling_degree_days_-_statistics#Heating_and_cooling_degree_days_at_EU_level"""

from __future__ import annotations

import pandas as pd


class Cooling:
    """Aggregate cooling model."""

    def __init__(
        self,
        daily_threshold: float | None = None,
        hourly_threshold: float | None = None,
        name: str | None = None,
    ):
        """Initialize class object.

        Parameters
        ----------
        daily_threshold : float, default None
            Threshold minimum average daily temperature in degree
            Celcius above which cooling is needed and CDH will be
            calculated. If the daily average is below this value, the
            CDH for that day are zero.
        hourly_threshold : float, default None
            Temperature in degree Celcius above which cooling is
            required, on days where cooling is needed.
        name : str
            Name of object."""

        # set name
        self.name = name

        # default daily threshold
        if daily_threshold is None:
            daily_threshold = 21

        # default hourly threshold
        if hourly_threshold is None:
            hourly_threshold = 19.8

        # set parameters
        self.daily_threshold = daily_threshold
        self.hourly_threshold = hourly_threshold

    def __repr__(self) -> str:
        """Reproduction string"""
        return f"Cooling(name={self.name})"

    def make_cooling_profile(
        self,
        temperature: pd.Series,
    ) -> pd.Series:
        """Evaluate profile for cooling degree hours throughout a year.

        Parameters
        ----------
        temperature : pd.Series
            Hourly temperature profile.

        Return
        ------
        cooling : pd.Series
            Cooling degree profile."""

        # construct mask for daily threshold
        # daily average temperature exceeds daily threshold value
        daily = temperature.groupby(pd.Grouper(freq="1D"), group_keys=False).apply(
            lambda group: pd.Series(
                group.mean() > self.daily_threshold, index=group.index
            )
        )

        # construct mask for hourly threshold
        # hourly temperature exceeds hourly threshold value
        hourly = temperature > self.hourly_threshold

        # set values outside mutual masks to zero
        # subtract hourly threshold value from cooling degrees
        temperature = temperature.where(daily & hourly, self.hourly_threshold).sub(
            self.hourly_threshold
        )

        return temperature.div(temperature.sum())
