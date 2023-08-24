"""parameters object"""
from __future__ import annotations
import functools

from typing import Any

import numpy as np
import pandas as pd

from pyetm.logger import get_modulelogger

from .session import SessionMethods

logger = get_modulelogger(__name__)


class ParameterMethods(SessionMethods):
    """collector class for parameter objects"""

    @property
    def application_demands(self):
        """application demands"""
        return self.get_application_demands()

    @functools.lru_cache(maxsize=1)
    def get_application_demands(self):
        """get the application demands"""

        # make url
        url = self.make_endpoint_url(
            endpoint="scenario_id", extra="application_demands"
        )

        # make request
        buffer = self.session.get(url, content_type="text/csv")

        return pd.read_csv(buffer, index_col="key")

    @property
    def energy_flows(self):
        """energy flows"""
        return self.get_energy_flows()

    @functools.lru_cache(maxsize=1)
    def get_energy_flows(self):
        """get the energy flows"""

        # make url
        url = self.make_endpoint_url(endpoint="scenario_id", extra="energy_flow")

        # make request
        buffer = self.session.get(url, content_type="text/csv")

        return pd.read_csv(buffer, index_col="key")

    @property
    def forecast_storage_order(self):
        """forecast storage order"""
        return self.get_forecast_storage_order()

    @forecast_storage_order.setter
    def forecast_storage_order(self, order: list[str]):
        self.change_forecast_storage_order(order)

    @functools.lru_cache(maxsize=1)
    def get_forecast_storage_order(self):
        """get the heat network order"""

        # make url
        url = self.make_endpoint_url(
            endpoint="scenario_id", extra="forecast_storage_order"
        )

        # make request
        order = self.session.get(url, content_type="application/json")

        return order["order"]

    def change_forecast_storage_order(self, order: list[str] | dict[str, list[str]]):
        """change forecast storage order

        parameters
        ----------
        order : list
            Desired forecast storage order"""

        # raise without scenario id
        self._validate_scenario_id()

        # convert np array to list
        if isinstance(order, np.ndarray):
            order = order.tolist()

        # acces dict for order
        if isinstance(order, dict):
            order = order["order"]

        # check items in order
        for item in order:
            if item not in self.forecast_storage_order:
                raise ValueError(f"Invalid forecast storage order item: '{item}'")

        # request parameters
        data = {"order": order}
        headers = {"content-type": "application/json"}
        url = self.make_endpoint_url(
            endpoint="scenario_id", extra="forecast_storage_order"
        )

        # make request
        self.session.put(url, json=data, headers=headers)

        # reinitialize scenario
        self._reset_cache()

    @property
    def heat_network_order(self):
        """heat network order"""
        return self.get_heat_network_order()

    @heat_network_order.setter
    def heat_network_order(self, order):
        self.change_heat_network_order(order)

    @functools.lru_cache(maxsize=1)
    def get_heat_network_order(self):
        """get the heat network order"""

        # make url
        url = self.make_endpoint_url(endpoint="scenario_id", extra="heat_network_order")

        # make request
        order = self.session.get(url, content_type="application/json")

        return order["order"]

    def change_heat_network_order(self, order):
        """change heat network order

        parameters
        ----------
        order : list
            Desired heat network order"""

        # convert np array to list
        if isinstance(order, np.ndarray):
            order = order.tolist()

        # acces dict for order
        if isinstance(order, dict):
            order = order["order"]

        # check items in order
        for item in order:
            if item not in self.heat_network_order:
                raise ValueError(f"Invalid heat network order item: '{item}'")

        # request parameters
        data = {"order": order}
        headers = {"content-type": "application/json"}
        url = self.make_endpoint_url(endpoint="scenario_id", extra="heat_network_order")

        # make request
        self.session.put(url, json=data, headers=headers)

        # reinitialize scenario
        self._reset_cache()

    @property
    def input_values(self):
        """input values"""
        return self.get_input_values()

    @input_values.setter
    def input_values(self, _: Any):
        raise AttributeError("protected attribute; change user values instead.")

    @functools.lru_cache(maxsize=1)
    def get_input_values(self):
        """get configuration information of all available input parameters.
        direct dump of inputs json from engine."""

        # make request
        url = self.make_endpoint_url(endpoint="inputs")
        records = self.session.get(url, content_type="application/json")

        # convert to frame
        inputs = pd.DataFrame.from_dict(records, orient="index")

        # add user to column when absent
        if "user" not in inputs.columns:
            inputs.insert(loc=5, column="user", value=np.nan)

        # convert user dtype to object and set disabled
        inputs["user"] = inputs["user"].astype("object")
        inputs["disabled"] = inputs["disabled"].fillna(False)

        return inputs

    @property
    def production_parameters(self):
        """production parameters"""
        return self.get_production_parameters()

    @functools.lru_cache(maxsize=1)
    def get_production_parameters(self):
        """get the production parameters"""

        # make url
        url = self.make_endpoint_url(
            endpoint="scenario_id", extra="production_parameters"
        )

        # make request
        buffer = self.session.get(url, content_type="text/csv")

        return pd.read_csv(buffer)

    @property
    def sankey(self):
        """sankey diagram"""
        return self.get_sankey()

    @functools.lru_cache(maxsize=1)
    def get_sankey(self):
        """get the sankey data"""

        # make request
        url = self.make_endpoint_url(endpoint="scenario_id", extra="sankey")
        buffer = self.session.get(url, content_type="text/csv")

        # convert to frame
        cols = ["Group", "Carrier", "Category", "Type"]
        sankey = pd.read_csv(buffer, index_col=cols)

        return sankey

    @property
    def scenario_parameters(self):
        """all user values including non-user defined parameters"""

        # get user and fillna with default
        uparams = self.user_parameters
        sparams = uparams.user.fillna(uparams.default)

        # set name of series
        sparams.name = "scenario"

        return sparams

    @scenario_parameters.setter
    def scenario_parameters(self, sparams):
        # check and set scenario parameters
        self._check_scenario_parameters(sparams)
        self.change_user_values(sparams)

    def _check_scenario_parameters(self, sparams=None):
        """Utility function to check the validity of the scenario
        parameters that are set in the scenario."""

        # default sparams
        if sparams is None:
            sparams = self.scenario_parameters

        # check passed parameters as user values
        sparams = self._check_user_values(sparams)

        # ensure that they are complete
        passed = self.scenario_parameters.index.isin(sparams.index)
        if not passed.all():
            missing = self.scenario_parameters[~passed]

            # warn for each missing key
            for key in missing.index:
                logger.warning(f"'{key}' not in passed scenario parameters")

    @property
    def storage_parameters(self):
        """storage volumes and capacities"""
        return self.get_storage_parameters()

    @functools.lru_cache(maxsize=1)
    def get_storage_parameters(self):
        """get the storage parameter data"""

        # make request
        url = self.make_endpoint_url(endpoint="scenario_id", extra="storage_parameters")
        buffer = self.session.get(url, content_type="text/csv")

        # convert to frame
        cols = ["Group", "Carrier", "Key", "Parameter"]
        parameters = pd.read_csv(buffer, index_col=cols)

        return parameters

    @property
    def user_parameters(self):
        """user parameters"""
        return self.get_user_parameters()

    @user_parameters.setter
    def user_parameters(self, _: Any):
        raise AttributeError("protected attribute; change user values instead.")

    def get_user_parameters(self):
        """get configuration information of all available user parameters"""

        # drop disabled parameters
        ivalues = self.input_values
        uparams = ivalues[~ivalues["disabled"]]

        return uparams

    @property
    def user_values(self):
        """all user set values without non-user defined parameters"""
        return self.get_user_values()

    @user_values.setter
    def user_values(self, uvalues):
        self.change_user_values(uvalues)

    def get_user_values(self):
        """get the parameters that are configued by the user"""

        # raise without scenario id
        self._validate_scenario_id()

        # subset values from user parameter df
        uvalues = self.user_parameters["user"]
        uvalues = uvalues.dropna()

        return uvalues

    def change_user_values(self, uvalues):
        """change the passed user values in the ETM.

        parameters
        ----------
        uvalues : pandas.Series
            collection of key, value pairs of user values."""

        # raise without scenario id
        self._validate_scenario_id()

        # validate passed user values
        uvalues = self._check_user_values(uvalues)

        # convert uvalues to dict
        uvalues = uvalues.to_dict()

        # map values to correct scenario parameters
        data = {"scenario": {"user_values": uvalues}, "detailed": True}

        # evaluate request
        url = f"scenarios/{self.scenario_id}"
        self.session.put(url, json=data)

        # reinitialize scenario
        self._reset_cache()

    def _check_user_values(self, uvalues):
        """check if all user values can be passed to ETM."""

        # convert None to dict
        if uvalues is None:
            uvalues = {}

        # convert dict to series
        if isinstance(uvalues, dict):
            uvalues = pd.Series(uvalues, name="user", dtype="object")

        # subset series from df
        if isinstance(uvalues, pd.DataFrame):
            uvalues = uvalues.user

        return uvalues

    def _get_sharegroup(self, key):
        """return subset of parameters in share group"""

        # get user and scenario parameters
        uparams = self.user_parameters
        sparams = self.scenario_parameters

        return sparams[uparams["share_group"] == key]
