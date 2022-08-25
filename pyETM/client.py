from __future__ import annotations

import re
import logging

from .core.curves import Curves
from .core.parameters import Parameters
from .core.ccurves import CustomCurves
from .core.gqueries import GQueries
from .core.header import Header
from .core.interpolate import Interpolate
from .core.scenario import Scenario
from .core.merit import MeritConfiguration
from .core.utils import Utils

from .sessions import RequestsSession

logger = logging.getLogger(__name__)


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

        self._reset_session()
        
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
    def from_scenario_parameters(cls, end_year, area_code, metadata=None,
                                 keep_compatible=False, read_only=False, 
                                 uvalues=None, heat_network_order=None, 
                                 ccurves=None, **kwargs):
        """create new scenario from parameters on Client initalization"""
                
        # initialize new scenario
        client = cls(scenario_id=None, **kwargs)
        client.create_new_scenario(end_year, area_code, metadata=metadata, 
                                   keep_compatible=keep_compatible, 
                                   read_only=read_only)
        
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
    def from_existing_scenario(cls, scenario_id, metadata=None, 
                               keep_compatible=False, read_only=False,
                               **kwargs):
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
    def from_saved_scenario_id(cls, scenario_id, **kwargs):
        """initialize a session with a saved scenario id"""
        
        # initialize client
        client = cls(scenario_id=None, **kwargs)
        session_id = client._get_session_id(scenario_id)
        
        return cls(session_id, **kwargs)
    
    def __init__(self, scenario_id: int = None, beta_engine: bool = False,
            reset: bool = False, Session = RequestsSession, **kwargs):
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
                
        # reset session?
        self._reset_session()

        logger.debug("initialised: '%s'", self)

    def __repr__(self):
        """reproduction string"""
        return f'Client({self.scenario_id})'

    def __str__(self):
        """stringname"""
        return repr(self)

    def __enter__(self):
        """enter conext manager"""

        # connect session
        self.session.connect()

        return self

    def __exit__(self, *args, **kwargs):
        """exit context manager"""
        
        # close session
        self.session.close()

    def _reset_session(self):
        """reset stored scenario properties"""
                
        # reset parameters
        self._input_values = None
        self._heat_network_order = None
        self._application_demands = None
        self._energy_flows = None
        self._production_parameters = None
        
        # reset gqueries
        self._gquery_results = None
        
        # reset ccurves
        self._ccurves = None
        
        # reset curves
        self._hourly_electricity_curves = None
        self._hourly_electricity_price_curve = None
        self._hourly_heat_curves = None
        self._hourly_household_curves = None
        self._hourly_hydrogen_curves = None
        self._hourly_methane_curves = None

    def _get_session_id(self, scenario_id: int, **kwargs) -> int:
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
