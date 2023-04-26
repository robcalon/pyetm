"""hourly curves"""
from __future__ import annotations

import functools
import pandas as pd

from pyetm.logger import get_modulelogger
from .session import SessionMethods

# get modulelogger
logger = get_modulelogger(__name__)


class CurveMethods(SessionMethods):
    """hourly curves"""

    @property
    def _merit_order_enabled(self) -> bool:
        """see if merit order is enabled"""

        # target input parameter
        key = "settings_enable_merit_order"
        url = f"scenarios/{self.scenario_id}/inputs/{key}"

        # make request
        headers = {'content-type': 'application/json'}
        resp: dict = self.session.get(url, headers=headers)

        # format setting
        enabled = resp.get('user', resp['default'])

        # check for iterpolation issues
        if not ((enabled == 1) | (enabled == 0)):
            raise ValueError(f"invalid setting: '{key}'={enabled}")

        return bool(enabled)

    def _validate_merit_order(self):
        """check if merit order is enabled"""

        # warn for disabled merit order
        if self._merit_order_enabled is False:
            logger.warning("%s: merit order disabled", self)

    @property
    def hourly_electricity_curves(self) -> pd.DataFrame:
        """hourly electricity curves"""
        return self.get_hourly_electricity_curves()

    @functools.lru_cache(maxsize=1)
    def get_hourly_electricity_curves(self) -> pd.DataFrame:
        """get the hourly electricity curves"""

        # raise without scenario id
        self._validate_scenario_id()
        self._validate_merit_order()

        # return empty frame with disabled merit order
        if self._merit_order_enabled is False:
            return pd.DataFrame()

        # make request
        post = f'scenarios/{self.scenario_id}/curves/merit_order'
        resp = self.session.get(post, decoder="BytesIO")

        # convert to frame and set periodindex
        curves = pd.read_csv(resp, index_col='Time', parse_dates=True)
        curves.index = curves.index.to_period(freq='H')

        return curves.rename_axis(None, axis=0)

    @property
    def hourly_electricity_price_curve(self) -> pd.Series:
        """hourly electricity price"""
        return self.get_hourly_electricity_price_curve()

    @functools.lru_cache(maxsize=1)
    def get_hourly_electricity_price_curve(self) -> pd.Series:
        """get the hourly electricity price curve"""

        # raise without scenario id
        self._validate_scenario_id()
        self._validate_merit_order()

        # return empty series with disabled merit order
        if self._merit_order_enabled is False:
            return pd.Series()

        # make request
        post = f'scenarios/{self.scenario_id}/curves/electricity_price'
        resp = self.session.get(post, decoder="BytesIO")

        # convert to series
        curves = pd.read_csv(resp, index_col='Time', parse_dates=True)
        curves: pd.Series = curves.squeeze(axis=1)

        # set periodindex
        curves.index = curves.index.to_period(freq='H')
        curves = curves.rename_axis(None, axis=0)

        return curves.round(2)

    @property
    def hourly_heat_curves(self) -> pd.DataFrame:
        """hourly heat curves"""
        return self.get_hourly_heat_curves()

    @functools.lru_cache(maxsize=1)
    def get_hourly_heat_curves(self) -> pd.DataFrame:
        """get the hourly heat network curves"""

        # raise without scenario id
        self._validate_scenario_id()
        self._validate_merit_order()

        # return empty frame with disabled merit order
        if self._merit_order_enabled is False:
            return pd.DataFrame()

        # request response and convert to frame
        post = f'scenarios/{self.scenario_id}/curves/heat_network'
        resp = self.session.get(post, decoder="BytesIO")

        # convert to frame and set periodindex
        curves = pd.read_csv(resp, index_col='Time', parse_dates=True)
        curves.index = curves.index.to_period(freq='H')

        return curves.rename_axis(None, axis=0)

    @property
    def hourly_household_curves(self) -> pd.DataFrame:
        """hourly household curves"""
        return self.get_hourly_household_curves()

    @functools.lru_cache(maxsize=1)
    def get_hourly_household_curves(self) -> pd.DataFrame:
        """get the hourly household heat curves"""

        # raise without scenario id
        self._validate_scenario_id()
        self._validate_merit_order()

        # return empty frame with disabled merit order
        if self._merit_order_enabled is False:
            return pd.DataFrame()

        # make request
        post = f'scenarios/{self.scenario_id}/curves/household_heat'
        resp = self.session.get(post, decoder="BytesIO")

        return pd.read_csv(resp)

    @property
    def hourly_hydrogen_curves(self) -> pd.DataFrame:
        """hourly hydrogen curves"""
        return self.get_hourly_hydrogen_curves()

    @functools.lru_cache(maxsize=1)
    def get_hourly_hydrogen_curves(self) -> pd.DataFrame:
        """get the hourly hydrogen curves"""

        # raise without scenario id
        self._validate_scenario_id()
        self._validate_merit_order()

        # return empty frame with disabled merit order
        if self._merit_order_enabled is False:
            return pd.DataFrame()

        # make request
        post = f'scenarios/{self.scenario_id}/curves/hydrogen'
        resp = self.session.get(post, decoder="BytesIO")

        # convert to frame and set periodindex
        curves = pd.read_csv(resp, index_col='Time', parse_dates=True)
        curves.index = curves.index.to_period(freq='H')

        return curves.rename_axis(None, axis=0)

    @property
    def hourly_methane_curves(self) -> pd.DataFrame:
        """hourly methane curves"""
        return self.get_hourly_methane_curves()

    @functools.lru_cache(maxsize=1)
    def get_hourly_methane_curves(self) -> pd.DataFrame:
        """get the hourly methane curves"""

        # raise without scenario id
        self._validate_scenario_id()
        self._validate_merit_order()

        # return empty frame with disabled merit order
        if self._merit_order_enabled is False:
            return pd.DataFrame()

        # make request
        post = f'scenarios/{self.scenario_id}/curves/network_gas'
        resp = self.session.get(post, decoder="BytesIO")

        # convert to frame and set periodindex
        curves = pd.read_csv(resp, index_col='Time', parse_dates=True)
        curves.index = curves.index.to_period(freq='H')

        return curves.rename_axis(None, axis=0)
