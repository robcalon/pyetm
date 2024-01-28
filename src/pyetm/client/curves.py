"""hourly curves"""
from __future__ import annotations

import functools
import pandas as pd
from .session import SessionMethods


def _get_curves(client: SessionMethods, extra: str, **kwargs) -> pd.DataFrame:
    """wrapper to fetch curves from curves-endpoint"""

    # request parameters
    url = client.make_endpoint_url(endpoint="curves", extra=extra)
    buffer = client.session.get(url, content_type="text/csv")

    return pd.read_csv(buffer, **kwargs)


class CurveMethods(SessionMethods):
    """hourly curves"""

    @property
    def hourly_electricity_curves(self):
        """hourly electricity curves"""
        return self.get_hourly_electricity_curves()

    @functools.lru_cache(maxsize=1)
    def get_hourly_electricity_curves(self):
        """get the hourly electricity curves"""

        # get curves
        curves = _get_curves(self, extra="merit_order", index_col="Time")

        # set periodindex
        curves.index = pd.PeriodIndex(curves.index, freq="h").set_names(None)

        return curves

    @property
    def hourly_electricity_price_curve(self):
        """hourly electricity price"""
        return self.get_hourly_electricity_price_curve()

    @functools.lru_cache(maxsize=1)
    def get_hourly_electricity_price_curve(self):
        """get the hourly electricity price curve"""

        # get squeezed curves
        curves: pd.Series = _get_curves(
            self, extra="electricity_price", index_col="Time"
        ).squeeze(axis=1)

        # set periodindex
        curves.index = pd.PeriodIndex(curves.index, freq="h").set_names(None)

        return curves.round(2)

    @property
    def hourly_heat_curves(self):
        """hourly heat curves"""
        return self.get_hourly_heat_curves()

    @functools.lru_cache(maxsize=1)
    def get_hourly_heat_curves(self):
        """get the hourly heat network curves"""

        # get curves
        curves = _get_curves(self, extra="heat_network", index_col="Time")

        # set periodindex
        curves.index = pd.PeriodIndex(curves.index, freq="h").set_names(None)

        return curves

    @property
    def hourly_household_curves(self):
        """hourly household curves"""
        return self.get_hourly_household_curves()

    @functools.lru_cache(maxsize=1)
    def get_hourly_household_curves(self):
        """get the hourly household heat curves"""
        return _get_curves(self, extra="household_heat")

    @property
    def hourly_hydrogen_curves(self):
        """hourly hydrogen curves"""
        return self.get_hourly_hydrogen_curves()

    @functools.lru_cache(maxsize=1)
    def get_hourly_hydrogen_curves(self):
        """get the hourly hydrogen curves"""

        # get curves
        curves = _get_curves(self, extra="hydrogen", index_col="Time")

        # set periodindex
        curves.index = pd.PeriodIndex(curves.index, freq="h").set_names(None)

        return curves

    @property
    def hourly_methane_curves(self):
        """hourly methane curves"""
        return self.get_hourly_methane_curves()

    @functools.lru_cache(maxsize=1)
    def get_hourly_methane_curves(self):
        """get the hourly methane curves"""

        # get curves
        curves = _get_curves(self, extra="network_gas", index_col="Time")

        # set periodindex
        curves.index = pd.PeriodIndex(curves.index, freq="h").set_names(None)

        return curves
