"""parameters object"""
import functools

from pyetm.logger import get_modulelogger

import numpy as np
import pandas as pd

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

        # raise without scenario id
        self._validate_scenario_id()

        # make request
        url = f'scenarios/{self.scenario_id}/application_demands'
        resp = self.session.get(url, decoder="BytesIO")

        return pd.read_csv(resp, index_col='key')

    @property
    def energy_flows(self):
        """energy flows"""
        return self.get_energy_flows()

    @functools.lru_cache(maxsize=1)
    def get_energy_flows(self):
        """get the energy flows"""

        # raise without scenario id
        self._validate_scenario_id()

        # make request
        url = f'scenarios/{self.scenario_id}/energy_flow'
        resp = self.session.get(url, decoder="BytesIO")

        # convert to frame
        flows = pd.read_csv(resp, index_col='key')

        return flows

    @property
    def forecast_storage_order(self):
        """forecast storage order"""
        return self.get_forecast_storage_order()

    @forecast_storage_order.setter
    def heat_netforecast_storage_orderwork_order(self, order):
        self.change_forecast_storage_order(order)

    @functools.lru_cache(maxsize=1)
    def get_forecast_storage_order(self):
        """get the heat network order"""

        # raise without scenario id
        self._validate_scenario_id()

        # make request
        url = f'scenarios/{self.scenario_id}/forecast_storage_order'
        resp = self.session.get(url)

        # get order
        order = resp["order"]

        return order

    def change_forecast_storage_order(self, order):
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
            order = order['order']

        # check items in order
        for item in order:
            if item not in self.forecast_storage_order:
                raise ValueError(
                    f"Invalid forecast storage order item: '{item}'")

        # map order to correct scenario parameter
        data = {'order': order}

        # make request
        url = f'scenarios/{self.scenario_id}/forecast_storage_order'
        self.session.put(url, json=data)

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

        # raise without scenario id
        self._validate_scenario_id()

        # make request
        url = f'scenarios/{self.scenario_id}/heat_network_order'
        resp = self.session.get(url)

        # get order
        order = resp["order"]

        return order

    def change_heat_network_order(self, order):
        """change heat network order

        parameters
        ----------
        order : list
            Desired heat network order"""

        # raise without scenario id
        self._validate_scenario_id()

        # convert np array to list
        if isinstance(order, np.ndarray):
            order = order.tolist()

        # acces dict for order
        if isinstance(order, dict):
            order = order['order']

        # check items in order
        for item in order:
            if item not in self.heat_network_order:
                raise ValueError(
                    f"Invalid heat network order item: '{item}'")

        # map order to correct scenario parameter
        data = {'order': order}

        # make request
        url = f'scenarios/{self.scenario_id}/heat_network_order'
        self.session.put(url, json=data)

        # reinitialize scenario
        self._reset_cache()

    @property
    def input_values(self):
        """input values"""
        return self.get_input_values()

    @input_values.setter
    def input_values(self, uparams):
        raise AttributeError('protected attribute; change user values instead.')

    @functools.lru_cache(maxsize=1)
    def get_input_values(self):
        """get configuration information of all available input parameters.
        direct dump of inputs json from engine."""

        # raise without scenario id
        self._validate_scenario_id()

        # make request
        url = f'scenarios/{self.scenario_id}/inputs'
        resp = self.session.get(url)

        # convert to frame
        ivalues = pd.DataFrame.from_dict(resp, orient='index')

        # add user to column when absent
        if 'user' not in ivalues.columns:
            ivalues.insert(loc=5, column='user', value=np.nan)

        # convert user dtype to object and set disabled
        ivalues.user = ivalues.user.astype('object')
        ivalues.disabled = ivalues.disabled.fillna(False)

        return ivalues

    @property
    def production_parameters(self):
        """production parameters"""
        return self.get_production_parameters()

    @functools.lru_cache(maxsize=1)
    def get_production_parameters(self):
        """get the production parameters"""

        # raise without scenario id
        self._validate_scenario_id()

        # make request
        url = f'scenarios/{self.scenario_id}/production_parameters'
        resp = self.session.get(url, decoder="BytesIO")

        return pd.read_csv(resp)

    @property
    def sankey(self):
        """sankey diagram"""
        return self.get_sankey()

    @functools.lru_cache(maxsize=1)
    def get_sankey(self):
        """get the sankey data"""

        # raise without scenario id
        self._validate_scenario_id()

        # make request
        url = f'scenarios/{self.scenario_id}/sankey'
        resp = self.session.get(url, decoder="BytesIO")

        # convert to frame
        cols = ['Group', 'Carrier', 'Category', 'Type']
        sankey = pd.read_csv(resp, index_col=cols)

        return sankey

    @property
    def scenario_parameters(self):
        """all user values including non-user defined parameters"""

        # get user and fillna with default
        uparams = self.user_parameters
        sparams = uparams.user.fillna(uparams.default)

        # set name of series
        sparams.name = 'scenario'

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

        # raise without scenario id
        self._validate_scenario_id()

        # make request
        url = f'scenarios/{self.scenario_id}/storage_parameters'
        resp = self.session.get(url, decoder="BytesIO")

        # convert to frame
        cols = ['Group', 'Carrier', 'Key', 'Parameter']
        parameters = pd.read_csv(resp, index_col=cols)

        return parameters

    @property
    def user_parameters(self):
        """user parameters"""
        return self.get_user_parameters()

    @user_parameters.setter
    def user_parameters(self, uparams):
        raise AttributeError('protected attribute; change user values instead.')

    def get_user_parameters(self):
        """get configuration information of all available user parameters"""

        # raise without scenario id
        self._validate_scenario_id()

        # drop disabled parameters
        ivalues = self.input_values
        uparams = ivalues[~ivalues.disabled]

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
        uvalues = self.user_parameters['user']
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
        url = f'scenarios/{self.scenario_id}'
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
            uvalues = pd.Series(uvalues, name='user', dtype='object')

        # subset series from df
        if isinstance(uvalues, pd.DataFrame):
            uvalues = uvalues.user

        return uvalues

    def _get_sharegroup(self, key):
        """return subset of parameters in share group"""

        # get user and scenario parameters
        uparams = self.user_parameters
        sparams = self.scenario_parameters

        return sparams[uparams.share_group == key]

    # @property
    # def _cvalues(self):
    #     """continous user values"""

    #     # get relevant parameters
    #     keys = self._dvalues.index
    #     cvalues = self.scenario_parameters

    #     # get continious parameters
    #     cvalues = cvalues[~cvalues.index.isin(keys)]

    #     return cvalues.astype('float64')

    # @property
    # def _dvalues(self):
    #     """discrete user values"""

    #     keys = [
    #         'heat_storage_enabled',
    #         'merit_order_subtype_of_energy_power_nuclear_uranium_oxide',
    #         'settings_enable_merit_order',
    #         'settings_enable_storage_optimisation_energy_flexibility_hv_opac_electricity',
    #         'settings_enable_storage_optimisation_energy_flexibility_pumped_storage_electricity',
    #         'settings_enable_storage_optimisation_energy_flexibility_mv_batteries_electricity',
    #         'settings_enable_storage_optimisation_energy_flexibility_flow_batteries_electricity',
    #         'settings_enable_storage_optimisation_transport_car_flexibility_p2p_electricity',
    #         'settings_weather_curve_set',
    #     ]

    #     # get discrete parameters
    #     dvalues = self.scenario_parameters
    #     dvalues = dvalues[dvalues.index.isin(keys)]

    #     return dvalues
