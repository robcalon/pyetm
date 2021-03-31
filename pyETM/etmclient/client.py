from .core.Curves import Curves
from .core.Custom import Custom
from .core.Engine import Engine
from .core.Header import Header
from .core.HTTPClient import HTTPClient
from .core.Parameters import Parameters
from .core.Scenario import Scenario


class ETMClient(Curves, Custom, Engine, Header, 
                 HTTPClient, Parameters, Scenario):
    
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