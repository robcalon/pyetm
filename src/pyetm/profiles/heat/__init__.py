"""Initialize weather profile module"""

from __future__ import annotations

import pandas as pd

from .buildings import Buildings
from .households import HousePortfolio


class HeatDemandProfileGenerator:
    """Heat demand profile generator."""

    @classmethod
    def from_defaults(cls) -> HeatDemandProfileGenerator:
        """Initialize with Quintel default settings."""

        # default object configurations
        households = HousePortfolio.from_defaults()
        buildings = Buildings.from_defaults()

        return cls(households, buildings)

    def __init__(self, households: HousePortfolio, buildings: Buildings) -> None:
        """Initialize class object.

        Parameters
        ----------
        households : HousePortolio
            HousePortfolio object.
        buildings : Buildings
            Buidlings object."""

        # set objects
        self.households = households
        self.buildings = buildings

    def make_heat_demand_profiles(
        self,
        temperature: pd.Series[float],
        irradiance: pd.Series[float],
        wind_speed: pd.Series[float],
    ) -> pd.DataFrame:
        """heat demand related profiles"""

        # make household heat demand profiles
        households = self.households.make_heat_demand_profiles(temperature, irradiance)

        # make buildings heat demand profile
        buildings = self.buildings.make_heat_demand_profile(temperature, wind_speed)

        # add other profiles
        temperature = pd.Series(temperature, name="weather/air_temperature")
        agriculture = pd.Series(buildings, name="weather/agriculture_heating")

        # merge profiles
        profiles = pd.concat([agriculture, temperature, buildings, households], axis=1)

        return profiles.sort_index(axis=1)
