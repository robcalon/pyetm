"""Heat profile generator for households, adapted from:
https://github.com/quintel/etdataset-public/tree/master/curves/demand/households/space_heating"""

from __future__ import annotations
from collections.abc import Iterable

from pyetm.logger import PACKAGEPATH
from pyetm.utils.profiles import validate_profile

import pandas as pd

from .smoothing import ProfileSmoother


class Houses:
    """Aggregate heating model for a specific type of houses"""

    @property
    def p_concrete(self) -> float:
        """Concrete density in kg/m3"""
        return 2.4e3

    @property
    def c_concrete(self) -> float:
        """Concrete thermal conductance in W/kg"""
        return .88e3

    @property
    def u_value(self) -> float:
        """Concrete thermal transmittance in W/(m2*K)"""
        return 1 / self.r_value

    @property
    def concrete_mass(self) -> float:
        """Concrete mass in kg"""
        return self.surface_area * self.wall_thickness * self.p_concrete

    @property
    def heat_capacity(self) -> float:
        """Concrete heat capacity in J/K"""
        return self.concrete_mass * self.c_concrete / 3.6e6

    @property
    def exchange_delta(self) -> float:
        """Exchange delta for house"""
        return self.u_value * self.surface_area / 1e3

    @classmethod
    def from_defaults(cls, house_type: str, insulation_level: str) -> Houses:
        """Initialize with Quintel default values.

        Parameters
        ----------
        house_type : str
            Name of default house type.
        insulation_level : str
            Name of default insulation type."""

        # load properties
        file = PACKAGEPATH.joinpath('data/house_properties.csv')
        properties = pd.read_csv(
            file, index_col=['house_type', 'insulation_level'])

        # subset correct house and insulation profile
        properties = properties.loc[(house_type, insulation_level)]

        # load thermostat values
        file = PACKAGEPATH.joinpath('data/thermostat_values.csv')
        thermostat = pd.read_csv(
            file, usecols = [insulation_level]).squeeze('columns')

        # initialize house
        house = cls(
            thermostat=thermostat,
            house_type=house_type,
            insulation_level=insulation_level,
            smoother=ProfileSmoother(),
            **properties,
        )

        return house

    def __init__(self,
        behaviour: float,
        r_value: float,
        surface_area: float,
        wall_thickness: float,
        window_area: float,
        thermostat: Iterable[float],
        house_type: str | None = None,
        insulation_level: str | None = None,
        smoother : ProfileSmoother | None = None,
        **kwargs
    ) -> Houses:
        """Initialize class object.

        Parameters
        ----------
        behaviour : float
            Behaviour
        r_value : float
            House insulation value.
        surface_area : float
            House surface area.
        wall_thickness : float
            House wall thickness.
        window_area : float
            House wall area.
        thermostat : Iterable[float]
            Daily thermostat settings. Iterable
            must contain 24 values.
        house_type : str, default None
            Name of house type.
        insulation_level : str, default None
            Name of insulation level.
        smoother : Smoother, default None
            Smoother object, uses ProfileSmoother
            by default.

        All kwargs are passed to default ProfileSmoother"""

        # default house type
        if house_type is None:
            house_type = 'unnamed'

        # default insulation type
        if insulation_level is None:
            insulation_level = 'unnamed'

        # default smoother
        if smoother is None:
            smoother = ProfileSmoother(**kwargs)

        # set parameters
        self.behaviour = behaviour
        self.surface_area = surface_area
        self.r_value = r_value
        self.wall_thickness = wall_thickness
        self.window_area = window_area

        # set thermostat
        self.thermostat = thermostat
        self._inside = self.thermostat[0]

        # set type names
        self.house_type = house_type
        self.insulation_level = insulation_level

        # set smoother
        self.smoother = smoother

    def __repr__(self) -> str:
        """Reproduction string"""
        return (
            f"House(house_type='{self.house_type}', "
            f"insulation_level='{self.insulation_level}')")

    def _calculate_heat_demand(
        self,
        temperature: float,
        irradiance: float,
        hour: int
    ) -> float:
        """"Calculates the required heating demand for the hour.

        Parameters
        ----------
        temperature : float
            Outside temperature value.
        irradiance : float
            Solar irradiance value.
        hour : int
            Hour of the day.

        Return
        ------
        demand : float
            Required heating demand."""

        # determine energy demand at hour and update inside temperature
        demand = (max(self.thermostat[hour] - self._inside, 0)
            * self.heat_capacity)

        # determine new inside temperature
        self._inside = max(self.thermostat[hour], self._inside)

        # determine energy leakage and absorption
        leakage = (self._inside - temperature) * self.exchange_delta
        absorption = irradiance * self.window_area

        # account for leaking and absorption
        self._inside -= (leakage + absorption) / self.heat_capacity

        return demand

    def make_heat_demand_profile(
        self,
        temperature: pd.Series,
        irradiance: pd.Series,
        year: int | None = None
    ) -> pd.DataFrame:
        """Make heat demand profile for house.

        Parameters
        ----------
        temperature : pd.Series
            Outdoor temperature profile in degrees
            Celcius for 8760 hours.
        irradiance : pd.Series
            Irradiance profile in W/m2 for 8760 hours.
        year : int, default None
            Optional year to help construct a
            PeriodIndex when a series are passed
            without PeriodIndex or DatetimeIndex.

        Return
        ------
        profile : pd.Series
            Heat demand profile for house type at
            house insulation level."""

        # validate temperature profile
        temperature = validate_profile(
            temperature, name='temperature', year=year)

        # validate irradiance profile
        irradiance = validate_profile(
            irradiance, name='irradiance', year=year)

        # check for allignment
        if not temperature.index.equals(irradiance.index):
            raise ValueError("Periods or Datetimes of 'temperature' "
                "and 'irradiance' profiles are not alligned.")

        # merge profiles and get index hours
        profile = pd.concat([temperature, irradiance], axis=1)
        profile['hour'] = profile.index.hour

        # apply calculate demand function
        profile = profile.apply(
            lambda row: self._calculate_heat_demand(**row), axis=1)

        # smooth resulting profile
        values = self.smoother.calculate_smoothed_demand(
            profile.values, self.insulation_level)

        # name profile
        name = f'weather/insulation_{self.house_type}_{self.insulation_level}'
        profile = pd.Series(values, profile.index, dtype='float64', name=name)

        return profile / profile.sum() / 3.6e3


class HousePortfolio:
    """Collection of House objects."""

    @property
    def houses(self) -> Iterable[Houses]:
        """Collection of house types."""
        return self._houses

    @houses.setter
    def houses(self, houses: Iterable[Houses]):
        """Setter for houses property."""

        # check items in iterable for House
        for house in houses:
            if not isinstance(house, Houses):
                raise TypeError('houses must be of type House')

        # set parameter
        self._houses = houses

    @classmethod
    def from_defaults(cls, name: str = 'default') -> HousePortfolio:
        """From Quintel default house types and insulation levels."""

        # load properties
        file = PACKAGEPATH.joinpath('data/house_properties.csv')
        properties = pd.read_csv(
            file, usecols = ['house_type', 'insulation_level'])

        # newlist
        houses = []

        # iterate over house types and insulation levels
        for house_type in properties.house_type.unique():
            for insultation_level in properties.insulation_level.unique():

                # init house from default settings
                houses.append(
                    Houses.from_defaults(
                        house_type, insultation_level))

        return cls(houses, name=name)

    def __init__(
        self,
        houses: Iterable[Houses],
        name: str | None = None
    ) -> HousePortfolio:
        """Initialize class object.

        Parameters
        ----------
        houses : Iterable[House]
            Iterable with House objects.
        name : str, default None
            Name of object"""

        # set houses
        self.name = name
        self.houses = houses

    def __repr__(self) -> str:
        """Reproduction string"""
        houses = "', ".join([str(house) for house in self.houses])
        return f"HousePortfolio(houses='{houses}')"

    def __str__(self) -> str:
        """String name"""
        return f'HousePortfolio(name={self.name})'

    def make_heat_demand_profiles(
        self,
        temperature : pd.Series,
        irradiance : pd.Series,
        year: int | None = None
    ) -> pd.DataFrame:
        """Make heat demand profiles for all houses.

        Parameters
        ----------
        temperature : pd.Series
            Outdoor temperature profile in degrees
            Celcius for 8760 hours.
        irradiance : pd.Series
            Irradiance profile in W/m2 for 8760 hours.
        year : int, default None
            Optional year to help construct a
            PeriodIndex when a series are passed
            without PeriodIndex or DatetimeIndex.

        Return
        ------
        profiles : pd.DataFrame
            Heat demand profiles for each house
            type and insulation level."""

        # get heat profile for each house object
        profiles = [
            house.make_heat_demand_profile(
                temperature, irradiance, year) for house in self.houses]

        return pd.concat(profiles, axis=1)
