"""Heat profile generator for buildings, adapted from:
https://github.com/quintel/etdataset-public/tree/master/curves/demand/buildings/space_heating"""

from __future__ import annotations

import pandas as pd

from pyetm.logger import _PACKAGEPATH_
from pyetm.utils.profiles import validate_profile, make_period_index


class Buildings:
    """Aggregate heating model for buildings."""

    @classmethod
    def from_defaults(cls, name: str = "default") -> Buildings:
        """Initialize with Quintel default values.

        Parameters
        ----------
        name : str, default 'default'
            name of object."""

        # relevant columns
        dtypes = {"reference": float, "slope": float, "constant": float}

        # filepath
        file = _PACKAGEPATH_.joinpath("data/G2A_parameters.csv")
        usecols = [key for key in dtypes]

        # load G2A parameters
        frame = pd.read_csv(file, usecols=usecols, dtype=dtypes)

        # get relevant profiles
        reference = frame["reference"]
        slope = frame["slope"]
        constant = frame["constant"]

        return cls(
            name=name,
            reference=reference,
            slope=slope,
            constant=constant,
        )

    def __init__(
        self,
        reference: pd.Series[float],
        slope: pd.Series[float],
        constant: pd.Series[float],
        name: str | None = None,
    ):
        """Initialize class object.

        Parameters
        ----------
        reference : pd.Series,
            Reference temperatures in G2A
            profiles (TST).
        slope : pd.Series
            Temperature dependent slope in G2A
            profiles (RER).
        constant : pd.Series
            Temperature independent constant G2A
            profiles (TOP).
        name : str
            Name of object."""

        # set parameters
        self.name = name
        self.reference = validate_profile(reference)
        self.slope = validate_profile(slope)
        self.constant = validate_profile(constant)

    def __repr__(self) -> str:
        """Reproduction string"""
        return f"Buildings(name={self.name})"

    def _calculate_heat_demand(
        self, effective: float, reference: float, slope: float, constant: float
    ) -> float:
        """Calculates the required heating demand for the hour.

        Parameters
        ----------
        reference : float,
            Reference temperatures
            for hour (TST).
        slope : float
            Temperature dependent slope
            for hour (RER).
        constant : pd.Series
            Temperature independent constant
            for hour (TOP)

        Return
        ------
        demand : float
            Required heating demand"""
        return (
            (reference - effective) * slope + constant
            if effective < reference
            else constant
        )

    def _make_parameters(self, effective: pd.Series[float]) -> pd.DataFrame:
        """Make parameters frame from effective temperature
         and G2A parameters.

         Parameters
         ----------
         effective : pd.Series
            Effective temperature profile.

        Return
        ------
        parameters : pd.DataFrame
            Merged parameters with correct index."""

        # resample effective temperature for each hour
        effective = effective.resample("H").ffill()

        # check for index equality
        if (not self.reference.index.equals(self.slope.index)) or (
            not self.slope.index.equals(self.constant.index)
        ):
            raise ValueError(
                "indices for 'reference', 'slope' and "
                "'constant' profiles are not alligned."
            )

        # merge G2A parameters
        parameters = pd.concat([self.reference, self.slope, self.constant], axis=1)

        # reindex parameters
        parameters.index = effective.index

        return pd.concat([effective, parameters], axis=1)

    def make_heat_demand_profile(
        self,
        temperature: pd.Series[float],
        wind_speed: pd.Series[float],
    ) -> pd.Series[float]:
        """Make heat demand profile for buildings.

        Effective temperature is defined as daily average temperature
        in degree Celcius minus daily average wind speed in m/s
        divided by 1.5.

        Parameters
        ----------
        temperature : pd.Series
            Outdoor temperature profile in degrees
            Celcius for 8760 hours.
        wind_speed : pd.Series
            Wind speed profile in m/s for 8760 hours.

        Return
        ------
        profile : pd.Series
            Heat demand profile for buildings."""

        # validate temperature profile
        temperature = validate_profile(temperature, name="temperature")
        wind_speed = validate_profile(wind_speed, name="wind_speed")

        # check for allignment
        if not temperature.index.equals(wind_speed.index):
            raise ValueError(
                "index of 'temperature' and 'wind_speed' profiles are not alligned."
            )

        # merge profiles and assign periodindex
        merged = pd.concat([temperature, wind_speed], axis=1)
        merged.index = make_period_index(year=2019, periods=8760)

        # evaluate daily average
        merged = merged.groupby(pd.Grouper(freq="1D")).mean()

        # evaluate effective temperature
        effective = merged["temperature"] - (merged["wind_speed"] / 1.5)
        effective = pd.Series(effective, name="effective", dtype=float)

        # make parameters
        profiles = self._make_parameters(effective)

        # apply calculate demand functon
        profile = profiles.apply(lambda row: self._calculate_heat_demand(**row), axis=1)

        # name profile
        name = "weather/buildings_heating"
        profile = pd.Series(profile, name=name, dtype=float)

        # scale profile values
        profile = profile / profile.sum() / 3.6e3

        # reassign origin index
        profile.index = temperature.index

        return profile
