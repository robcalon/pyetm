from .core.curves import Curves
from .core.custom import Custom
from .core.engine import Engine
from .core.header import Header
from .core.httpclient import HTTPClient
from .core.parameters import Parameters
from .core.scenario import Scenario
from .core.gqueries import GQueries
from .core.interpolate import Interpolate

class ETMClient(Curves, Custom, Engine, Header, HTTPClient, 
                Parameters, Scenario, GQueries, Interpolate):
    
    def __init__(self, scenario_id, beta_engine=False, 
                 context=None, proxy=None):
        
        # init self
        super().__init__()
        
        # set proxy server
        self.proxy = proxy
        
        # set class parameters
        self.beta_engine = beta_engine
        self._context = context
        self.scenario_id = scenario_id
        
        # set default gqueries
        self.gqueries = []
                
    def __str__(self):
        return f'BaseClient({self.scenario_id})'
    
    def __enter__(self):
        return self
    
    def __exit__(self, *args, **kwargs):
        return None
    
    def _reset_session(self):
        """reset stored scenario properties"""
        
        # reset header
        self._scenario_header = None
        
        # reset parameters
        self._user_parameters = None
        self._flexibility_order = None
        self._heat_network_order = None
        self._application_demands = None
        self._energy_flows = None
        self._production_parameters = None
        
        # reset gqueries
        self._gquery_results = None
        
        # reset curves
        self._hourly_electricity_curves = None
        self._hourly_electricity_price_curve = None
        self._hourly_heat_network_curves = None
        self._hourly_household_heat_curves = None
        self._hourly_hydrogen_curves = None
        self._hourly_network_gas_curves = None
        
        # reset customs
        self._reset_interconnectors()
        self._interconnector_prices = None