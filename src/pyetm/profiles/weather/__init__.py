"""Initialize weather profile module"""

from __future__ import annotations

from pyetm.utils.profiles import validate_profile

import pandas as pd

from .buildings import Buildings
from .households import HousePortfolio
# from .cooling import Cooling


class WeatherDemandProfiles:
    """Weather-related profile generator."""

    @classmethod
    def from_defaults(cls, name: str = 'default') -> WeatherDemandProfiles:
        """Initialize with Quintel default settings."""

        # default object configurations
        households = HousePortfolio.from_defaults()
        buildings = Buildings.from_defaults()

        return cls(households, buildings, name=name)

    def __init__(
        self,
        households: HousePortfolio,
        buildings: Buildings,
        name : str | None = None
    ) -> WeatherDemandProfiles:
        """Initialize class object.

        Parameters
        ----------
        households : HousePortolio
            HousePortfolio object.
        buildings : Buildings
            Buidlings object.
        name : str, default None
            Name of object."""

        # set name
        self.name = name

        # set parameters
        self.households = households
        self.buildings = buildings

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        return None

    def __str__(self) -> str:
        """String name"""
        return f"WeatherProfiles(name={self.name})"

    def make_demand_profiles(
        self,
        temperature: pd.Series,
        irradiance: pd.Series,
        wind_speed : pd.Series,
        year: int | None = None
    ) -> pd.DataFrame:
        """weather related profiles"""

        # validate temperature profile
        temperature = validate_profile(
            temperature, name='temperature', year=year)

        # validate irradiance profile
        irradiance = validate_profile(
            irradiance, name='irradiance', year=year)

        # validate wind_speed profile
        wind_speed = validate_profile(
            wind_speed, name='wind_speed', year=year)

        # check for allignment
        if not irradiance.index.equals(wind_speed.index):
            raise ValueError("Periods or Datetimes of 'irradiance' "
                "and 'wind_speed' profiles are not alligned.")

        # make household heat demand profiles
        households = self.households.make_heat_demand_profiles(
            temperature, irradiance, year)

        # make buildings heat demand profile
        buildings = self.buildings.make_heat_demand_profile(
            temperature, wind_speed, year)

        # make air temperature
        key = 'weather/air_temperature'
        temperature = pd.Series(temperature, name=key)

        # add agriculture
        key = 'weather/agriculture_heating'
        agriculture = pd.Series(buildings, name=key)

        # merge profiles
        profiles = pd.concat(
            [agriculture, temperature, buildings, households], axis=1)

        return profiles.sort_index(axis=1)
