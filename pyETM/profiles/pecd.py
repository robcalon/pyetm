"""PECD reader"""

from __future__ import annotations

from pathlib import Path
from pyETM.utils.profiles import make_period_index

import pandas as pd


class PECDReader:
    """PECD Reader"""

    def __init__(
        self,
        dirpath: str,
        temperature: str,
        irradiance: str,
        wind_speed: str,
        extension: str | None = None):
        """Initialize class object."""

        # default extension
        if extension is None:
            extension = '.csv'

        # set relpaths
        self.temperature = temperature
        self.irradiance = irradiance
        self.wind_speed = wind_speed

        # set extension
        self.extension = extension

        # set dirpath
        self.dirpath = dirpath

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        return None

    def make_path(self, relpath):
        """Make filepath"""
        return Path(self.dirpath).joinpath(relpath)

    def load_profile(
        self,
        filepath: str,
        zone: str,
        name : str | None = None,
        scalar: float | None = None
    ) -> pd.Series:
        """Load profile from path."""

        # default scalar
        if scalar is None:
            scalar = 1

        # read profile
        profile = pd.read_csv(
            filepath, usecols=[zone], nrows=8760).squeeze(axis=1)

        return scalar * pd.Series(profile, name=name)

    def load_temperature(self, year, zone):
        """Load temperature profile for year and zone."""

        # make filepath
        kwargs = {'year': year}
        filepath = self.make_path(
            self.temperature.format(**kwargs))

        return self.load_profile(
            filepath, zone, name='temperature')

    def load_irradiance(self, year, zone):
        """Load irradiance profile for year and zone."""

        # make filepath
        kwargs = {'year': year}
        filepath = self.make_path(
            self.irradiance.format(**kwargs))

        return self.load_profile(
            filepath, zone, name='irradiance', scalar=1e-3)

    def load_wind_speed(self, year, zone):
        """Load windspeed profile for year and zone."""

        # make filepath
        kwargs = {'year': year}
        filepath = self.make_path(
            self.wind_speed.format(**kwargs))

        return self.load_profile(
            filepath, zone, name='wind_speed')

    def load_weather_profiles(self, year, zone):
        """Load weather profiles for year and zone."""

        # load relevant profiles
        profiles = pd.concat([
            self.load_temperature(year, zone),
            self.load_irradiance(year, zone),
            self.load_wind_speed(year, zone),
        ], axis=1)

        # format index
        profiles.index = make_period_index(year, periods=8760)

        return profiles
