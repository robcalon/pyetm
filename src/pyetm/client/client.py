"""client object"""
from __future__ import annotations
from typing import Any, Iterable

import pandas as pd

from pyetm.logger import get_modulelogger
from pyetm.sessions import AIOHTTPSession, RequestsSession
from pyetm.types import InterpolateOptions
from pyetm.utils.interpolation import interpolate

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
        keep_compatible: bool | None = None,
        private: bool | None = None,
        input_parameters: pd.Series[Any] | None = None,
        forecast_storage_order: list[str] | None = None,
        heat_network_order: list[str] | None = None,
        ccurves: pd.DataFrame | None = None,
        **kwargs,
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
        input_parameters : dict, default None
            The user specified input parameter value
            configuration of the scenario.
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
        client.create_new_scenario(
            area_code=area_code,
            end_year=end_year,
            metadata=metadata,
            keep_compatible=keep_compatible,
            private=private,
        )

        # set user values
        if input_parameters is not None:
            client.input_parameters = input_parameters

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
        scenario_id: int | None = None,
        metadata: dict | None = None,
        keep_compatible: bool | None = None,
        private: bool | None = None,
        **kwargs,
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
            scenario_id, metadata, keep_compatible, private, connect=True
        )

        return client

    @classmethod
    def from_saved_scenario_id(
        cls,
        saved_scenario_id: int,
        metadata: dict | None = None,
        keep_compatible: bool | None = None,
        private: bool | None = None,
        **kwargs,
    ):
        """initialize client from saved scenario id

        Parameters
        ----------
        saved_scenario_id : int, default None
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
        scenario_id = client._get_saved_scenario_id(saved_scenario_id)

        return cls.from_existing_scenario(
            scenario_id, metadata, keep_compatible, private, **kwargs
        )

    @classmethod
    def from_interpolation(
        cls,
        end_year: int,
        scenario_ids: Iterable[int],
        method: InterpolateOptions = "linear",
        saved_scenario_ids: bool = False,
        metadata: dict | None = None,
        keep_compatible: bool | None = None,
        private: bool | None = None,
        forecast_storage_order: list[str] | None = None,
        heat_network_order: list[str] | None = None,
        ccurves: pd.DataFrame | None = None,
        **kwargs,
    ):
        """Initialize from interpolation of existing scenarios. Note that
        the custom orders are always returned to the default values.

        Parameters
        ----------
        year: int
            The end year for which the interpolation is made.
        scenario_ids: int or iterable of int.
            The scenario ids that are used for interpolation.
        method : string, default 'linear'
            Method for filling continious user values
            for the passed target year(s).
        saved_scenario_ids : bool, default False
            Passed scenario ids are saved scenario ids.
        metadata : dict, default None
            metadata passed to scenario.
        keep_compatible : bool, default None
            Keep scenario compatible with future
            versions of ETM. Defaults to settings
            in original scenario.
        private : bool, default None
            Make the scenario private.
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

        # handle scenario ids:
        if saved_scenario_ids:
            # Only perform read operations on these sids
            # as saved scenario's history would otherwise be modified.
            client = Client(**kwargs)
            scenario_ids = [client._get_saved_scenario_id(sid) for sid in scenario_ids]

        clients = [Client(sid, **kwargs) for sid in scenario_ids]

        # initialize scenario ids and sort by end year
        clients = [Client(sid, **kwargs) for sid in scenario_ids]
        clients = sorted(clients, key=lambda cln: cln.end_year)

        # get interpolated input parameters
        interpolated = interpolate(target=end_year, clients=clients, method=method)
        input_parameters = interpolated[end_year]

        # get area code
        client = clients[-1]
        area_code = client.area_code

        return Client.from_scenario_parameters(
            area_code=area_code,
            end_year=end_year,
            metadata=metadata,
            private=private,
            keep_compatible=keep_compatible,
            input_parameters=input_parameters,
            forecast_storage_order=forecast_storage_order,
            heat_network_order=heat_network_order,
            ccurves=ccurves,
            **kwargs,
        )

    def __init__(
        self,
        scenario_id: int | None = None,
        engine_url: str | None = None,
        etm_url: str | None = None,
        token: str | None = None,
        session: RequestsSession | AIOHTTPSession | None = None,
        **kwargs,
    ):
        """client object to process ETM requests via its public API

        Parameters
        ----------
        scenario_id : str, default None
            The api_session_id to which the client connects. Can only access
            a limited number of methods when scenario_id is set to None.
        token : str, default None
            Personal access token to authenticate requests to your
            personal account and scenarios. Detects token automatically
            from environment when assigned to ETM_ACCESS_TOKEN.
        engine_url : str, default None
            Specify URL that points to ETM engine, default to public engine.
        etm_url : str, default None
            Specify URL that points to ETM model (pro), default to public
            energy transition model.
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
        self.session = session

        # set engine and token
        self.engine_url = engine_url
        self.etm_url = etm_url
        self.token = token

        # set scenario id
        self.scenario_id = scenario_id

        # set default gqueries
        self.gqueries = []

        logger.debug("Initialised new Client: %s", self)

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
                "engine_url": self.engine_url,
                "session": self.session,
            },
            **self.__kwargs,
        }

        # object environment
        env = ", ".join(f"{k}={v}" for k, v in params.items())

        return f"Client({env})"

    def __str__(self):
        """stringname"""

        # make stringname
        strname = (
            "Client("
            f"scenario_id={self.scenario_id}, "
            f"area_code={self.area_code if self.scenario_id else None}, "
            f"end_year={self.end_year if self.scenario_id else None})"
        )

        return strname

    def _reset_cache(self):
        """reset cached scenario properties"""

        # clear parameter caches
        self._get_scenario_header.cache_clear()
        self._get_input_parameters.cache_clear()

        # clear frame caches
        self.get_application_demands.cache_clear()
        self.get_energy_flows.cache_clear()
        self.get_production_parameters.cache_clear()
        self.get_sankey.cache_clear()
        self.get_storage_parameters.cache_clear()

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
