"""Heat profile generator for households, adapted from:
https://github.com/quintel/etdataset-public/tree/master/curves/demand/households/space_heating

Profile smoother, adapted from:
https://github.com/quintel/etdataset-public/blob/master/curves/demand/households/space_heating/script/smoothing.pys"""

from __future__ import annotations

__all__ = ["HouseModel", "HousePortfolioModel"]

from collections.abc import Iterable

import numpy as np
import pandas as pd

from pyetm.logger import _PACKAGEPATH_
from pyetm.utils.profiles import validate_profile, make_period_index

class HouseModel:
    """Aggregate heating model for a specific type of houses"""

    @property
    def p_concrete(self) -> float:
        """Concrete density in kg/m3"""
        return 2.4e3

    @property
    def c_concrete(self) -> float:
        """Concrete thermal conductance in W/kg"""
        return 0.88e3

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
    def from_defaults(cls, house_type: str, insulation_level: str) -> HouseModel:
        """Initialize with Quintel default values.

        Parameters
        ----------
        house_type : str
            Name of default house type.
        insulation_level : str
            Name of default insulation type."""

        # relevant columns
        dtypes = {
            "house_type": str,
            "insulation_level": str,
            "behaviour": float,
            "r_value": float,
            "window_area": float,
            "surface_area": float,
            "wall_thickness": float,
        }

        # filepath
        file = _PACKAGEPATH_.joinpath("data/house_properties.csv")
        usecols = [key for key in dtypes]

        # load properties
        properties = pd.read_csv(file, usecols=usecols, index_col=[0, 1], dtype=dtypes)
        props = properties.T[(house_type, insulation_level)]

        # get relevant properties
        behaviour = props["behaviour"]
        r_value = props["r_value"]
        window_area = props["window_area"]
        surface_area = props["surface_area"]
        wall_thickness = props.loc["wall_thickness"]

        # load thermostat values
        file = _PACKAGEPATH_.joinpath("data/thermostat_values.csv")
        thermostat = pd.read_csv(file, usecols=[insulation_level]).squeeze("columns")

        # initialize house
        house = cls(
            behaviour=behaviour,
            r_value=r_value,
            window_area=window_area,
            surface_area=surface_area,
            wall_thickness=wall_thickness,
            thermostat=thermostat,
            house_type=house_type,
            insulation_level=insulation_level,
            smoother=_ProfileSmoother(),
        )

        return house

    def __init__(
        self,
        behaviour: float,
        r_value: float,
        surface_area: float,
        wall_thickness: float,
        window_area: float,
        thermostat: Iterable[float],
        house_type: str | None = None,
        insulation_level: str | None = None,
        smoother: _ProfileSmoother | None = None,
        **kwargs,
    ):
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
            house_type = "unnamed"

        # default insulation type
        if insulation_level is None:
            insulation_level = "unnamed"

        # default smoother
        if smoother is None:
            smoother = _ProfileSmoother(**kwargs)

        # set parameters
        self.behaviour = behaviour
        self.surface_area = surface_area
        self.r_value = r_value
        self.wall_thickness = wall_thickness
        self.window_area = window_area

        # set thermostat
        self.thermostat = list(thermostat)
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
            f"insulation_level='{self.insulation_level}')"
        )

    def _calculate_heat_demand(
        self, temperature: float, irradiance: float, hour: int
    ) -> float:
        """ "Calculates the required heating demand for the hour.

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
        demand = max(self.thermostat[int(hour)] - self._inside, 0) * self.heat_capacity

        # determine new inside temperature
        self._inside = max(self.thermostat[int(hour)], self._inside)

        # determine energy leakage and absorption
        leakage = (self._inside - temperature) * self.exchange_delta
        absorption = irradiance * self.window_area

        # account for leaking and absorption
        self._inside -= (leakage + absorption) / self.heat_capacity

        return demand

    def make_heat_demand_profile(
        self, temperature: pd.Series[float], irradiance: pd.Series[float]
    ) -> pd.Series[float]:
        """Make heat demand profile for house.

        Parameters
        ----------
        temperature : pd.Series
            Outdoor temperature profile in degrees
            Celcius for 8760 hours.
        irradiance : pd.Series
            Irradiance profile in W/m2 for 8760 hours.

        Return
        ------
        profile : pd.Series
            Heat demand profile for house type at
            house insulation level."""

        # validate profiles
        temperature = validate_profile(temperature, name="temperature")
        irradiance = validate_profile(irradiance, name="irradiance")

        # check for allignment
        if not temperature.index.equals(irradiance.index):
            raise ValueError(
                "index of 'temperature' and 'irradiance' profiles are not alligned."
            )

        # merge profiles and assign hour of day
        merged = pd.concat([temperature, irradiance], axis=1)
        merged["hour"] = make_period_index(2019, periods=8760).hour

        # apply calculate demand function
        profile = merged.apply(lambda row: self._calculate_heat_demand(**row), axis=1)

        # smooth resulting profile
        values = self.smoother.calculate_smoothed_demand(
            profile.values, self.insulation_level
        )

        # name profile
        name = f"weather/insulation_{self.house_type}_{self.insulation_level}"
        profile = pd.Series(values, profile.index, dtype=float, name=name)

        # reassign origin index
        profile.index = temperature.index

        return profile / profile.sum() / 3.6e3


class HousePortfolioModel:
    """Collection of House objects."""

    @property
    def houses(self) -> Iterable[HouseModel]:
        """Collection of house types."""
        return self._houses

    @houses.setter
    def houses(self, houses: Iterable[HouseModel]):
        """Setter for houses property."""

        # check items in iterable for House
        for house in houses:
            if not isinstance(house, HouseModel):
                raise TypeError("houses must be of type House")

        # set parameter
        self._houses = houses

    @classmethod
    def from_defaults(cls, name: str = "default") -> HousePortfolioModel:
        """From Quintel default house types and insulation levels."""

        # load properties
        file = _PACKAGEPATH_.joinpath("data/house_properties.csv")
        properties = pd.read_csv(file, usecols=["house_type", "insulation_level"])

        # newlist
        houses = []

        # iterate over house types and insulation levels
        for house_type in properties["house_type"].unique():
            for insultation_level in properties["insulation_level"].unique():
                # init house from default settings
                houses.append(HouseModel.from_defaults(house_type, insultation_level))

        return cls(houses, name=name)

    def __init__(self, houses: Iterable[HouseModel], name: str | None = None):
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
        return f"HousePortfolio(name={self.name})"

    def make_heat_demand_profiles(
        self, temperature: pd.Series[float], irradiance: pd.Series[float]
    ) -> pd.DataFrame:
        """Make heat demand profiles for all houses.

        Parameters
        ----------
        temperature : pd.Series
            Outdoor temperature profile in degrees
            Celcius for 8760 hours.
        irradiance : pd.Series
            Irradiance profile in W/m2 for 8760 hours.

        Return
        ------
        profiles : pd.DataFrame
            Heat demand profiles for each house
            type and insulation level."""

        # get heat profile for each house object
        profiles = [
            house.make_heat_demand_profile(temperature, irradiance)
            for house in self.houses
        ]

        return pd.concat(profiles, axis=1)


class _ProfileSmoother:
    """
    The profiles generator is based on data for an individual household.
    To transform this into a profile for a typical neighbourhood (e.g. 300 houses)
    we shift the heat demand curves multiple times based on a normal distribution
    and return the average profile
    The following steps are taken:
    1. The original heat demand curves are interpolated with a step size of 10.
    This simulates a curve with 6 minute time intervals rather than a 1 hour.
    2. X random numbers are drawn based on a normal distribution with mean 0
    and an Y-hour standard deviation. These numbers are rounded to 1 decimal point
    and multiplied by 10. Each number represents the number of 6 minute intervals
    deviation from the mean.
    3. The original heat demand curve is loaded 300 times and shifted Z intervals
    backwards or forwards in time depending on the value of the random numbers.
    This results in 300 demand curves that follow the exact same pattern
    over time, but with a different starting point. Depending on the value of the
    random number a curve starts earlier or later than the original curve
    4. The X curves are summed and converted back to a 1 hour interval.
    This results in a curve that represents the aggregated/average heat demand of
    X houses rather than an individual household.
    """

    def __init__(
        self,
        number_of_houses=300,
        hours_shifted=None,
        interpolation_steps=10,
        random_seed=1337,
    ):
        """init"""

        # Standard deviation per insulation type.
        # See README for source
        if hours_shifted is None:
            hours_shifted = {"low": 2, "medium": 2.5, "high": 3}

        # interpolation steps
        # use intervals of 6 minutes when shifting curves

        self.number_of_houses = number_of_houses
        self.hours_shifted = hours_shifted
        self.interpolation_steps = interpolation_steps
        self.random_seed = random_seed

    def generate_deviations(self, size, scale):
        """
        Generate X random numbers with a standard deviation of Y hours
        Round to 1 decimal place and multiply by 10 to get
        integer value. The number designates the number of
        6 minute time slots the demand profile will be shifted
        compared to the original demand profile.
        E.g. '15' means that the demand profile will be shifted
        forward 1.5 hours, '-10' means it will be shifted backwards 1 hour
        """
        # (re)set random seed
        np.random.seed(self.random_seed)

        # generate X random numbers with normal distribution
        random_numbers = np.random.normal(loc=0.0, scale=scale, size=size)

        # round by 1 decimal point
        rounded_numbers = np.round(random_numbers, 1)

        # multiply by 10 to get integer numbers for the deviations
        shifts = rounded_numbers * 10

        return shifts.astype(int)

    def interpolate(self, arr, steps):
        """
        Interpolate the original demand profile
        to allow for smaller intervals than 1
        hour (steps=10 means 6 minute intervals)
        """
        interpolated_arr = []
        for index, value in enumerate(arr):
            start = value
            if index == len(arr) - 1:
                stop = arr[0]
            else:
                stop = arr[index + 1]

            step_size = (stop - start) / steps
            for i in range(0, steps):
                interpolated_arr.append(start + i * step_size)

        return interpolated_arr

    def shift_curve(self, arr, num):
        """
        Rotate the elements of an array based on num.
        Example: if num = 5, each element will be shifted 5 places forwards.
        Elements at the end of the array will be put at the front.
        """
        return np.roll(arr, num)

    def trim_interpolated(self, arr, steps):
        """
        Converts the curve back to the original number of data points (8760) by
        taking the average of X data points before and X data points after each
        hour (where X = INTERPOLATION_STEPS)
        Example: If INTERPOLATION_STEPS = 10, the interpolated curve has 87600 data
        points. Trimming it results in a curve with 8760 data points, where each
        data point (hour) is the average of 30 minutes before and after the whole
        hour.
        """
        arr = self.shift_curve(arr, self.interpolation_steps // 2)
        return [sum(arr[i : (i + steps)]) / steps for i in range(0, len(arr), steps)]

    def calculate_smoothed_demand(self, heat_demand, insulation_type):
        """calculate smoothed demand"""

        # start out with list of zeroes
        cumulative_demand = [0] * len(heat_demand) * self.interpolation_steps

        # generate random numbers
        deviations = self.generate_deviations(
            self.number_of_houses, self.hours_shifted[insulation_type]
        )

        # interpolate the demand curve to increase the number of data points
        # (i.e. reduce the time interval 1 hour to e.g. 6 minutes)
        interpolated_demand = self.interpolate(heat_demand, self.interpolation_steps)

        # for each random number, shift the demand curve X places forwards or
        # backwards (depending on the number value) and add it to the
        # cumulative demand array
        for num in deviations:
            demand_list = self.shift_curve(interpolated_demand, num)
            cumulative_demand = [x + y for x, y in zip(cumulative_demand, demand_list)]

        # Trim the cumulative demand array such that it has 8760 data points again
        # (hourly intervals instead of 6 minute intervals)
        smoothed_demand = self.trim_interpolated(
            cumulative_demand, self.interpolation_steps
        )

        return smoothed_demand
