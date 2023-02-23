"""Loadfactor scaling, sigmoid adapted from:
https://stackoverflow.com/questions/3985619/how-to-calculate-a-logistic-sigmoid-function-in-python"""

from __future__ import annotations
from pyetm.utils.profiles import validate_profile

import numpy as np
import pandas as pd


class CapacityFactorProfiles:
    """Loading factors for renewables"""

    @property
    def year(self):
        """year for which generator is configured"""
        return self._year

    @property
    def wind_offshore(self):
        """validated wind offshore profile"""
        return self._wind_offshore

    @property
    def wind_onshore(self):
        """validated wind onshore profile"""
        return self._wind_onshore

    @property
    def wind_coastal(self):
        """validated wind coastal profile"""
        return self._wind_coastal

    @property
    def solar_pv(self):
        """validated solar pv profile"""
        return self._solar_pv

    @property
    def solar_thermal(self):
        """validated solar thermal profile"""
        return self._solar_thermal

    def __init__(
        self,
        year : int,
        wind_offshore : pd.Series,
        wind_onshore : pd.Series,
        wind_coastal : pd.Series,
        solar_pv : pd.Series,
        solar_thermal : pd.Series,
    ) -> CapacityFactorProfiles:
        """Initialize class object."""

        # set year
        self._year = year

        # set wind offshore
        self._wind_offshore = validate_profile(
            wind_offshore, name='weather/wind_offshore_baseline', year=year)

        # set wind onshore
        self._wind_onshore = validate_profile(
            wind_onshore, name='weather/wind_inland_baseline', year=year)

        # set wind coastal
        self._wind_coastal = validate_profile(
            wind_coastal, name='weather/wind_coastal_baseline', year=year)

        # set solar pv
        self._solar_pv = validate_profile(
            solar_pv, name='weather/solar_pv_profile_1', year=year)

        # set solar thermal
        self._solar_thermal = validate_profile(
            solar_thermal, name='weather/solar_thermal', year=year)

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        return None

    def __str__(self) -> str:
        """String name"""
        return f"LoadFactorProfiles(year={self.year})"

    def make_capacity_factors(
        self,
        wind_offshore_scalar: float | None = None,
        wind_onshore_scalar: float | None = None,
        wind_coastal_scalar: float | None = None,
        solar_pv_scalar: float | None = None,
        solar_thermal_scalar: float | None = None,
        **kwargs
) -> pd.DataFrame:
        """Scale profiles and return ETM compatible format"""

        # scale offshore profile
        offshore = self.scale_profile(
            profile=self.wind_offshore, scalar=wind_offshore_scalar, **kwargs)

        # scale onshore profile
        onshore = self.scale_profile(
            profile=self.wind_onshore, scalar=wind_onshore_scalar, **kwargs)

        # scale wind coastal profile
        coastal = self.scale_profile(
            profile=self.wind_coastal, scalar=wind_coastal_scalar, **kwargs)

        # scale solar pv profile
        solar = self.scale_profile(
            profile=self.solar_pv, scalar=solar_pv_scalar, **kwargs)

        # scale solar thermal profile
        thermal = self.scale_profile(
            profile=self.solar_thermal, scalar=solar_thermal_scalar, **kwargs)

        # merge profiles
        profiles = pd.concat(
            [offshore, onshore, coastal, solar, thermal], axis=1)

        return profiles.sort_index(axis=1)

    def scale_profile(
        self,
        profile: pd.Series,
        scalar: float | None = None,
        **kwargs
    ) -> pd.Series:
        """aply sigmoid?"""

        # check for values within range 0, 1

        # don't scale profile
        if (scalar is None) | (scalar == 1):
            return profile

        # get derivative, normalize and scale with volume
        factor = profile.apply(self._scaler)
        factor = factor / factor.sum() * (scalar - 1) * profile.sum()

        # recheck if values within range or warn user.

        return profile.add(factor)

    def _sigmoid(self, factor: float) -> float:
        """parameterised sigmoid function for normalized profiles"""
        return -7.9921 + (9.2005 / (1 + np.exp(-1.7363*factor + -1.8928)))

    def _scaler(self, factor: float) -> float:
        """scale by derivative of sigmoid"""
        return self._sigmoid(factor) * (1 - self._sigmoid(factor))

    # def sigmoid(x):
    #     return np.exp(-np.logaddexp(0, -x))

    # def sigmoid(x):
    #     return 1 / (1 + np.exp(-(np.log(40000) / 1) * (x-1) + np.log(0.005)))
