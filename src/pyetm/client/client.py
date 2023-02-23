"""client object"""
from __future__ import annotations

from pyetm.logger import get_modulelogger

import pandas as pd

from .base import BaseClient
from .curves import CurveMethods
from .customcurves import CustomCurveMethods
from .gqueries import GQueryMethods
from .interpolate import InterpolationMethods
from .meritorder import MeritOrderMethods
from .parameters import ParameterMethods
from .savedscenarios import SavedScenarioMethods
from .utils import UtilMethods

# get modulelogger
logger = get_modulelogger(__name__)


class Client(
    CurveMethods,
    CustomCurveMethods,
    GQueryMethods,
    InterpolationMethods,
    MeritOrderMethods,
    ParameterMethods,
    SavedScenarioMethods,
    UtilMethods,
    BaseClient,
):
    """Main client object"""

    @classmethod
    def from_scenario_parameters(cls, area_code: str, end_year: int,
        metadata: dict | None = None, keep_compatible: bool = False,
        read_only: bool = False, uvalues: dict | pd.Series | None = None,
        heat_network_order: list[str] | None = None,
        ccurves: pd.DataFrame | None = None, **kwargs) -> Client:
        """create new scenario from parameters on Client initalization"""

        # initialize new scenario
        client = cls(**kwargs)
        client.create_new_scenario(area_code, end_year, metadata=metadata,
            keep_compatible=keep_compatible, read_only=read_only)

        # set user values
        if uvalues is not None:
            client.user_values = uvalues

        # set ccurves
        if ccurves is not None:
            client.upload_custom_curves(ccurves)

        # set heat network order
        if heat_network_order is not None:
            client.heat_network_order = heat_network_order

        return client

    @classmethod
    def from_existing_scenario(cls, scenario_id: str | None = None,
        metadata: dict | None = None, keep_compatible: bool | None = None,
        read_only: bool = False, **kwargs) -> Client:
        """create new scenario as copy of existing scenario"""

        # initialize client
        client = cls(scenario_id=None, **kwargs)
        client.copy_scenario(scenario_id)

        # set scenario title
        if metadata is not None:
            client.metadata = metadata

        # set keep compatible parameter
        if keep_compatible is not None:
            client.keep_compatible = keep_compatible

        # set read only parameter
        client.read_only = read_only

        return client

    @classmethod
    def from_saved_scenario_id(cls,
        saved_scenario_id: str,
        copy: bool = True,
        metadata: dict | None = None,
        keep_compatible: bool | None = None,
        read_only: bool = False,
        **kwargs):
        """initialize client from saved scenario id

        Parameters
        ----------
        saved_scenario_id : int or str, default None
            The saved scenario id to which is connected.
        copy : bool, default True
            Connect to a copy of the latest scenario_id. Connects
            to the latest used scenario_id otherwise.
        metadata : dict, default None
            metadata passed to scenario.
        keep_compatible : bool, default None
            Keep scenario compatible with future
            versions of ETM. Defaults to settings
            in original scenario.
        read_only : bool, default False
            The scenario cannot be modified.

        **kwargs are passed to the default initialization
        procedure of the client.

        Return
        ------
        client : Client
            Returns initialized client object."""

        # initialize client
        client = cls(**kwargs)

        # connect to saved scenario
        client.connect_to_saved_scenario(
            saved_scenario_id=saved_scenario_id,
            copy=copy,
            metadata=metadata,
            keep_compatible=keep_compatible,
            read_only=read_only
        )

        return client

    def __repr__(self):
        """reproduction string"""

        # get initialization parameters
        params = {
            **{
                "scenario_id": self.scenario_id,
                "beta_engine": self.beta_engine,
                "token": self.token,
                "session": self.session
                },
            **self.__kwargs
            }

        # object environment
        env = ", ".join(f'{k}={v}' for k, v in params.items())

        return f"Client({env})"

    def __str__(self):
        """stringname"""

        # make stringname
        strname = (
            "Client("
                f"scenario_id={self.scenario_id}, "
                f"area_code={self.area_code}, "
                f"end_year={self.end_year})"
        )

        return strname

    def _reset_cache(self):
        """reset cached scenario properties"""

        # clear parameter caches
        self._get_scenario_header.cache_clear()
        self.get_input_values.cache_clear()

        # clear frame caches
        self.get_application_demands.cache_clear()
        self.get_energy_flows.cache_clear()
        self.get_heat_network_order.cache_clear()
        self.get_production_parameters.cache_clear()
        self.get_sankey.cache_clear()

        # reset gqueries
        self.get_gquery_results.cache_clear()

        # reset ccurves
        self.get_custom_curves.cache_clear()

        # reset curves
        self.get_hourly_electricity_curves.cache_clear()
        self.get_hourly_electricity_price_curve.cache_clear()
        self.get_hourly_heat_curves.cache_clear()
        self.get_hourly_household_curves.cache_clear()
        self.get_hourly_hydrogen_curves.cache_clear()
        self.get_hourly_methane_curves.cache_clear()
