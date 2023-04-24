"""hourly curves"""
from __future__ import annotations

import functools
import pandas as pd

from .session import SessionMethods


class CurveMethods(SessionMethods):
    """hourly curves"""

    @property
    def hourly_electricity_curves(self):
        """hourly electricity curves"""
        return self.get_hourly_electricity_curves()

    @functools.lru_cache(maxsize=1)
    def get_hourly_electricity_curves(self):
        """get the hourly electricity curves"""

        # raise without scenario id
        self._validate_scenario_id()

        # make request
        post = f'scenarios/{self.scenario_id}/curves/merit_order'
        resp = self.session.get(post, decoder="BytesIO")

        # convert to frame and reset index
        curves = pd.read_csv(resp, index_col='Time')
        curves = curves.reset_index(drop=True)

        return curves

    @property
    def hourly_electricity_price_curve(self):
        """hourly electricity price"""
        return self.get_hourly_electricity_price_curve()

    @functools.lru_cache(maxsize=1)
    def get_hourly_electricity_price_curve(self):
        """get the hourly electricity price curve"""

        # raise without scenario id
        self._validate_scenario_id()

        # make request
        post = f'scenarios/{self.scenario_id}/curves/electricity_price'
        resp = self.session.get(post, decoder="BytesIO")

        # convert to frame and reset index
        curves = pd.read_csv(resp, index_col='Time')
        curves = curves.squeeze('columns').reset_index(drop=True)

        return curves.round(2)

    @property
    def hourly_heat_curves(self):
        """hourly heat curves"""
        return self.get_hourly_heat_curves()

    @functools.lru_cache(maxsize=1)
    def get_hourly_heat_curves(self):
        """get the hourly heat network curves"""

        # raise without scenario id
        self._validate_scenario_id()

        # request response and convert to frame
        post = f'scenarios/{self.scenario_id}/curves/heat_network'
        resp = self.session.get(post, decoder="BytesIO")

        # convert to frame and reset index
        curves = pd.read_csv(resp, index_col='Time')
        curves = curves.reset_index(drop=True)

        return curves

    @property
    def hourly_household_curves(self):
        """hourly household curves"""
        return self.get_hourly_household_curves()

    @functools.lru_cache(maxsize=1)
    def get_hourly_household_curves(self):
        """get the hourly household heat curves"""

        # raise without scenario id
        self._validate_scenario_id()

        # make request
        post = f'scenarios/{self.scenario_id}/curves/household_heat'
        resp = self.session.get(post, decoder="BytesIO")

        return pd.read_csv(resp)

    @property
    def hourly_hydrogen_curves(self):
        """hourly hydrogen curves"""
        return self.get_hourly_hydrogen_curves()

    @functools.lru_cache(maxsize=1)
    def get_hourly_hydrogen_curves(self):
        """get the hourly hydrogen curves"""

        # raise without scenario id
        self._validate_scenario_id()

        # make request
        post = f'scenarios/{self.scenario_id}/curves/hydrogen'
        resp = self.session.get(post, decoder="BytesIO")

        # convert to frame and reset index
        curves = pd.read_csv(resp, index_col='Time')
        curves = curves.reset_index(drop=True)

        return curves

    @property
    def hourly_methane_curves(self):
        """hourly methane curves"""
        return self.get_hourly_methane_curves()

    @functools.lru_cache(maxsize=1)
    def get_hourly_methane_curves(self):
        """get the hourly methane curves"""

        # raise without scenario id
        self._validate_scenario_id()

        # make request
        post = f'scenarios/{self.scenario_id}/curves/network_gas'
        resp = self.session.get(post, decoder="BytesIO")

        # convert to frame and reset index
        curves = pd.read_csv(resp, index_col='Time')
        curves = curves.reset_index(drop=True)

        return curves
