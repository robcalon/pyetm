"""Heat profile generator for buildings, adapted from:
https://github.com/quintel/etdataset-public/tree/master/curves/demand/buildings/space_heating"""

from __future__ import annotations

from pyetm.logger import PACKAGEPATH
from pyetm.utils.profiles import validate_profile, validate_profile_lenght

import pandas as pd


class Buildings:
    """Aggregate heating model for buildings."""

    @classmethod
    def from_defaults(cls, name: str = 'default') -> Buildings:
        """Initialize with Quintel default values.

        Parameters
        ----------
        name : str, default 'default'
            name of object."""

        # load G2A parameters
        file = PACKAGEPATH.joinpath('data/G2A_parameters.csv')
        parameters = pd.read_csv(file)

        return cls(name=name, **parameters)

    def __init__(
        self,
        reference: pd.Series,
        slope: pd.Series,
        constant: pd.Series,
        name: str | None = None
    ) -> Buildings:
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
        self.reference = validate_profile_lenght(reference)
        self.slope = validate_profile_lenght(slope)
        self.constant = validate_profile_lenght(constant)

    def __repr__(self) -> str:
        """Reproduction string"""
        return f"Buildings(name={self.name})"

    def _calculate_heat_demand(
            self,
            effective: float,
            reference: float,
            slope: float,
            constant: float
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
        return (reference - effective) * slope + constant if effective < reference else constant

    def _make_parameters(self, effective: pd.Series) -> pd.DataFrame:
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
        effective = effective.resample('H').ffill()

        # check for index equality
        if ((not self.reference.index.equals(self.slope.index))
            | (not self.slope.index.equals(self.constant.index))):
            raise ValueError("indices for 'reference', 'slope' and "
                "'constant' profiles are not alligned.")

        # merge G2A parameters
        parameters = pd.concat(
            [self.reference, self.slope, self.constant], axis=1)

        # reindex parameters
        parameters.index = effective.index

        return pd.concat([effective, parameters], axis=1)

    def make_heat_demand_profile(
        self,
        temperature: pd.Series,
        wind_speed : pd.Series,
        year: int | None = None) -> pd.DataFrame:
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
        year : int, default None
            Optional year to help construct a
            PeriodIndex when a series are passed
            without PeriodIndex or DatetimeIndex.

        Return
        ------
        profile : pd.Series
            Heat demand profile for buildings."""

        # validate temperature profile
        temperature = validate_profile(
            temperature, name='temperature', year=year)

        # validate irradiance profile
        wind_speed = validate_profile(
            wind_speed, name='wind_speed', year=year)

        # check for allignment
        if not temperature.index.equals(wind_speed.index):
            raise ValueError("Periods or Datetimes of 'temperature' "
                "and 'wind_speed' profiles are not alligned.")

        # merge profiles and get daily average
        effective = pd.concat([temperature, wind_speed], axis=1)
        effective = effective.groupby(pd.Grouper(freq='1D')).mean()

        # evaluate effective temperature
        effective = effective['temperature'] - (effective['wind_speed'] / 1.5)
        effective = pd.Series(effective, name='effective')

        # make parameters
        profiles = self._make_parameters(effective)

        # apply calculate demand functon
        profile = profiles.apply(
            lambda row: self._calculate_heat_demand(**row), axis=1)

        # name profile
        name = 'weather/buildings_heating'
        profile = pd.Series(profile, name=name, dtype='float64')

        return profile / profile.sum() / 3.6e3
