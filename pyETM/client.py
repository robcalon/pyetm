from .curves import Curves
from .engine import Engine
from .header import Header
from .httpclient import HTTPClient
from .parameters import Parameters
from .scenario import Scenario
from .ccurves import CustomCurves
from .gqueries import GQueries
from .interpolate import Interpolate
from .utils import Utils

class Client(Curves, Engine, Header, HTTPClient, Parameters, 
             Scenario, CustomCurves, GQueries, Interpolate, Utils):
       
    def __init__(self, scenario_id=None, beta_engine=False, 
                 reset=False, validate_ccurves=True, ipython='auto', 
                 proxy='auto'):
        """Client which connects to ETM
        
        Parameters
        ----------
        scenario_id : str, default None
            The api_session_id to which the client connects. Can only access
            a limited number of methods when scenario_id is set to None.
        beta_engine : bool, default False
            Connect to the beta-engine instead of the production-engine.
        validate_ccurves : bool, default True
            Validate the key of a passed custom curve. Can be set
            to False when attempting to upload internal curves. 
        reset : bool, default False
            Reset scenario on initalization.
        ipython : bool, default 'auto'
            Set to True When the client is called in an IPython environment 
            to nest the asyncio event loop. When set to 'auto', tries to
            detect if ipython is running in client.
        proxy : str, default auto
            Proxy address to pass to the aiohttp client. When set to 'auto',
            enviornment variabels are searched to detect a HTTP(S)_PROXY.
            
        Returns
        -------
        self :  Client
            Object that connects to ETM API.
        """
        
        # init self
        super().__init__()
        
        # set proxy server
        self.proxy = proxy
        
        # set class parameters
        self.beta_engine = beta_engine
        self._ipython = ipython
        self.scenario_id = scenario_id
        
        # set default gqueries
        self.gqueries = []
        
        # reset scenario on intialization
        if reset and (scenario_id != None):
            self.reset_scenario()
            
        # set validate ccurves key argument
        self.validate_ccurves = validate_ccurves
        
        # reset session?
        self._reset_session()
        
    @classmethod
    def from_scenario_parameters(cls, end_year, area_code, title=None, 
                        protected=False, uvalues=None, ccurves=None, 
                        heat_network_order=None, **kwargs):
        """create new scenario from parameters on Client initalization"""
        
        # initialize new scenario
        client = cls(scenario_id=None, **kwargs)
        client.create_new_scenario(end_year, area_code, title, protected)
        
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
    def from_existing_scenario(cls, scenario_id, title=None, 
                               protected=False, **kwargs):
        """create new scenario as copy of existing scenario"""
        
        # initialize client
        client = cls(scenario_id=None, **kwargs)
        client.create_scenario_copy(scenario_id)

        # set scenario title
        if title is not None:
            client.title = title
            
        # set protected
        if protected == True:
            client.protected = protected
        
        return client
        
    def __str__(self):
        return f'Client({self.scenario_id})'
    
    def __enter__(self):
        return self
    
    def __exit__(self, *args, **kwargs):
        return None
    
    def _reset_session(self):
        """reset stored scenario properties"""
        
        # reset header
        self._scenario_header = None
        
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
        self._hourly_heat_network_curves = None
        self._hourly_household_heat_curves = None
        self._hourly_hydrogen_curves = None
        self._hourly_network_gas_curves = None