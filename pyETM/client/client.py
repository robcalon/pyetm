from __future__ import annotations

import re
import pandas as pd

from .curves import Curves
from .parameters import Parameters
from .ccurves import CustomCurves
from .gqueries import GQueries
from .header import Header
from .interpolate import Interpolate
from .scenario import Scenario
from .merit import MeritConfiguration
from .utils import Utils

from pyETM.logger import get_modulelogger
from pyETM.sessions import RequestsSession, AIOHTTPSession

# get modulelogger
logger = get_modulelogger(__name__)


class Client(Curves, Header, Parameters, Scenario, MeritConfiguration,
        CustomCurves, GQueries, Interpolate, Utils):

    @property
    def beta_engine(self) -> bool:
        """connects to beta-engine when False and to production-engine
        when True.""" 
        return self.__beta_engine
        
    @beta_engine.setter
    def beta_engine(self, boolean: bool) -> None:
        """set beta engine attribute"""
            
        # set boolean and reset session
        self.__beta_engine = bool(boolean)
        self.session.base_url = self.base_url

        self.reset_session()
        
    @property
    def base_url(self) -> str:
        """"base url for carbon transition model"""
        
        # return beta engine url
        if self.beta_engine:
            return "https://beta-engine.energytransitionmodel.com/api/v3/"
        
        # return production engine url
        return "https://engine.energytransitionmodel.com/api/v3/"

    @property
    def session(self):
        return self.__session

    @classmethod
    def from_scenario_parameters(cls, area_code: str, end_year: int, 
        metadata: dict | None = None, keep_compatible: bool = False, 
        read_only: bool = False, uvalues: dict | pd.Series | None = None, 
        heat_network_order: list[str] | None = None, 
        ccurves: pd.DataFrame | None = None, **kwargs) -> Client:
        """create new scenario from parameters on Client initalization"""
                
        # initialize new scenario
        client = cls(scenario_id=None, **kwargs)
        client.create_new_scenario(area_code, end_year, metadata=metadata, 
            keep_compatible=keep_compatible, read_only=read_only)
        
        # set user values
        if uvalues is not None:
            client.user_values = uvalues
            
        # set ccurves
        if ccurves is not None:
            client.ccurves = ccurves
            
        # set heat network order
        if heat_network_order is not None:
            client.heat_network_order = heat_network_order
            
        return client
                
    @classmethod
    def from_existing_scenario(cls, scenario_id: str | None = None, 
        metadata: dict | None = None, keep_compatible: bool = False, 
        read_only: bool = False, **kwargs) -> Client:
        """create new scenario as copy of existing scenario"""
        
        # initialize client
        client = cls(scenario_id=None, **kwargs)
        client.create_scenario_copy(scenario_id)

        # set scenario title
        if metadata is not None:
            client.metadata = metadata
            
        # set protection settings
        client.keep_compatible = keep_compatible
        client.read_only = read_only
                    
        return client
    
    @classmethod
    def from_saved_scenario_id(cls, 
        scenario_id: str | None = None, **kwargs) -> Client:
        """initialize a session with a saved scenario id"""
        
        # initialize client
        client = cls(scenario_id=None, **kwargs)
        session_id = client._get_session_id(scenario_id)
        
        return cls(session_id, **kwargs)
    
    def __init__(self, scenario_id: str | None = None, 
        beta_engine: bool = False, reset: bool = False, 
        Session: RequestsSession | AIOHTTPSession | None = None, 
        **kwargs) -> Client:
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
        Session: object, default None
            Session object that handles requests to ETM's public API.

        All key-word arguments are passed directly to the init function
        of the assign Session object.  

        Returns
        -------
        self : Client
            Object that processes ETM requests via public API"""

        super().__init__()

        # default session
        if Session is None:
            Session = RequestsSession

        # set session
        self.__session = Session(**kwargs)

        # set parmeters
        self.beta_engine = beta_engine
        self.scenario_id = scenario_id

        # set default gqueries
        self.gqueries = []
        
        # reset scenario on intialization
        if reset and (scenario_id != None):
            self.reset_scenario()

        # store client environment
        self.__env = {
            **{"scenario_id": scenario_id, "beta_engine": beta_engine,
            "reset": reset, "Session": str(self.session)}, **kwargs}

        # make message
        msg = ("\"scenario_id='%s', area_code='%s', end_year=%s\"" 
            %(self.scenario_id, self.area_code, self.end_year))
        logger.debug("Initialised new Client: %s", msg)

    def __repr__(self):
        """reproduction string"""

        # object environment
        env = ", ".join(f'{k}={v}' for k, v in 
            self.__env.items())

        return "Client(%s)" %env

    def __str__(self):
        """stringname"""
        return "Client(scenario_id=%s, area_code=%s, end_year=%s)" %(
            self.scenario_id, self.area_code, self.end_year)

    def __enter__(self):
        """enter conext manager"""

        # connect session
        self.session.connect()

        return self

    def __exit__(self, *args, **kwargs):
        """exit context manager"""
        
        # close session
        self.session.close()

    def reset_session(self):
        """reset stored scenario properties"""

        # clear parameter caches
        self.get_scenario_header.cache_clear()
        self.get_input_values.cache_clear()

        # clear frame caches
        self.get_heat_network_order.cache_clear()
        self.get_application_demands.cache_clear()
        self.get_energy_flows.cache_clear()
        self.get_production_parameters.cache_clear()
        
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

        logger.debug("cleared lru_cache")

    def _get_session_id(self, scenario_id: str, **kwargs) -> int:
        """get a session_id for a pro-environment scenario"""    

        # make pro url
        host = "https://pro.energytransitionmodel.com"
        url = f"{host}/saved_scenarios/{scenario_id}/load"

        # extract content from url
        content = self.session.request("get", url, decoder='text', **kwargs)
            
        # get session id from content
        pattern = '"api_session_id":([0-9]{6,7})'
        session_id = re.search(pattern, content)

        return int(session_id.group(1))
