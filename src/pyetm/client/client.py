"""client object"""
from __future__ import annotations

import pandas as pd

from pyetm.logger import get_modulelogger
from pyetm.sessions import RequestsSession, AIOHTTPSession

from .account import AccountMethods
from .curves import CurveMethods
from .customcurves import CustomCurveMethods
from .gqueries import GQueryMethods
from .meritorder import MeritOrderMethods
from .parameters import ParameterMethods
from .scenario import ScenarioMethods
from .utils import UtilMethods

# get modulelogger
logger = get_modulelogger(__name__)


class Client(
    AccountMethods,
    CurveMethods,
    CustomCurveMethods,
    GQueryMethods,
    MeritOrderMethods,
    ParameterMethods,
    ScenarioMethods,
    UtilMethods,
):
    """Main client object"""

    @classmethod
    def from_scenario_parameters(
        cls,
        area_code: str,
        end_year: int,
        metadata: dict | None = None,
        keep_compatible: bool = False,
        private: bool | None = None,
        uvalues: dict | pd.Series | None = None,
        forecast_storage_order: list[str] | None = None,
        heat_network_order: list[str] | None = None,
        ccurves: pd.DataFrame | None = None,
        **kwargs
    ):
        """Create a new scenario from parameters.

        Parameters
        ----------
        area_code : str
            Area code of the created scenario.
        end_year : int
            End year of the created scenario.
        metadata : dict, default None
            metadata passed to scenario.
        keep_compatible : bool, default None
            Keep scenario compatible with future
            versions of ETM. Defaults to settings
            in original scenario.
        private : bool, default None
            Make the scenario private.
        uvalues : dict, default None
            The user value configuration in the scenario.
        forecast_storage_order : list[str], default None
            The forecast storage order in the scenario.
        heat_network_order : list[str], default None
            The heat network order in the scenario.
        ccurves : pd.DataFrame, default None
            Custom curves to use in sceneario.

        **kwargs are passed to the default initialization
        procedure of the client.

        Return
        ------
        client : Client
            Returns initialized client object."""
        # initialize new scenario
        client = cls(**kwargs)
        client.create_new_scenario(area_code, end_year, metadata=metadata,
            keep_compatible=keep_compatible, private=private)

        # set user values
        if uvalues is not None:
            client.user_values = uvalues

        # set ccurves
        if ccurves is not None:
            client.upload_custom_curves(ccurves)

        # set forecast storage order
        if forecast_storage_order is not None:
            client.forecast_storage_order = forecast_storage_order

        # set heat network order
        if heat_network_order is not None:
            client.heat_network_order = heat_network_order

        return client

    @classmethod
    def from_existing_scenario(
        cls,
        scenario_id: str | None = None,
        metadata: dict | None = None,
        keep_compatible: bool | None = None,
        private: bool | None = None,
        **kwargs
    ):
        """create a new scenario as a copy of an existing scenario.

        Parameters
        ----------
        scenario_id : int or str, default None
            The scenario id from which to copy.
        metadata : dict, default None
            metadata passed to scenario.
        keep_compatible : bool, default None
            Keep scenario compatible with future
            versions of ETM. Defaults to settings
            in original scenario.
        private : bool, default None
            Make the scenario private. Inherits the privacy
            setting of copied scenario id by default.

        **kwargs are passed to the default initialization
        procedure of the client.

        Return
        ------
        client : Client
            Returns initialized client object."""

        # initialize client
        client = cls(scenario_id=None, **kwargs)

        # copy scenario id
        client.copy_scenario(
            scenario_id, metadata, keep_compatible, private, connect=True)

        return client

    @classmethod
    def from_saved_scenario_id(
        cls,
        saved_scenario_id: int,
        metadata: dict | None = None,
        keep_compatible: bool | None = None,
        private: bool | None = None,
        **kwargs
    ):
        """initialize client from saved scenario id

        Parameters
        ----------
        saved_scenario_id : int or str, default None
            The saved scenario id to which is connected.
        metadata : dict, default None
            metadata passed to scenario.
        keep_compatible : bool, default None
            Keep scenario compatible with future
            versions of ETM. Defaults to settings
            in original scenario.
        private : bool, default None
            Make the scenario private.

        **kwargs are passed to the default initialization
        procedure of the client.

        Return
        ------
        client : Client
            Returns initialized client object."""

        # initialize client
        client = cls(**kwargs)

        # make request
        url = f"saved_scenarios/{saved_scenario_id}"
        headers = {'content-type': 'application/json'}

        # connect to saved scenario and
        scenario_id = client.session.get(
            url, decoder='json', headers=headers)['scenario_id']

        return cls.from_existing_scenario(
            scenario_id, metadata, keep_compatible, private, **kwargs)

    def __init__(
        self,
        scenario_id: str | None = None,
        beta_engine: bool = False,
        reset: bool = False,
        token: str | None = None,
        session: RequestsSession | AIOHTTPSession | None = None,
        **kwargs
    ):
        """client object to process ETM requests via its public API

        Parameters
        ----------
        scenario_id : str, default None
            The api_session_id to which the client connects. Can only access
            a limited number of methods when scenario_id is set to None.
        beta_engine : bool, default False
            Connect to the beta-engine instead of the production-engine.
        reset : bool, default False
            Reset scenario on initalization.
        token : str, default None
            Personal access token to authenticate requests to your
            personal account and scenarios. Detects token automatically
            from environment when assigned to ETM_ACCESS_TOKEN when
            connected to production or ETM_BETA_ACCESS_TOKEN when
            connected to beta.
        session: object instance, default None
            session instance that handles requests to ETM's public API.
            Default to use a RequestsSession.

        All key-word arguments are passed directly to the init method
        of the default session instance. All key-word arguments are
        ignored when the session argument is not None.

        Returns
        -------
        self : Client
            Object that processes ETM requests via public API"""

        super().__init__()

        # default session
        if session is None:
            session = RequestsSession(**kwargs)

        # set session
        self.__kwargs = kwargs
        self._session = session

        # set engine and token
        self.beta_engine = beta_engine
        self.token = token

        # set scenario id
        self.scenario_id = scenario_id

        # set default gqueries
        self.gqueries = []

        # reset scenario on intialization
        if reset and (scenario_id is not None):
            self.reset_scenario()

        # make message
        msg = (
            "Initialised new Client: "
                f"'scenario_id={self.scenario_id}, "
                f"area_code={self.area_code}, "
                f"end_year={self.end_year}'"
        )

        logger.debug(msg)

    def __enter__(self):
        """enter conext manager"""

        # connect session
        self.session.connect()

        return self

    def __exit__(self, *args, **kwargs):
        """exit context manager"""

        # close session
        self.session.close()

    def __repr__(self):
        """reproduction string"""

        # get initialization parameters
        params = {
            **{
                "scenario_id": self.scenario_id,
                "beta_engine": self.beta_engine,
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
