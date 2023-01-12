"""parameters object"""
from .application_demands import ApplicationDemands
from .energy_flows import EnergyFlows
from .heat_network_order import HeatNetworkOrder
from .production_parameters import ProductionParameters
from .sankey import Sankey
from .scenario_id import ScenarioID
from .scenario_parameters import ScenarioParameters
from .input_values import InputValues
from .user_parameters import UserParameters
from .user_values import UserValues
from .util_parameters import UtilParameters

class Parameters(ApplicationDemands, EnergyFlows, HeatNetworkOrder,
                 ProductionParameters, Sankey, ScenarioID, ScenarioParameters,
                 InputValues, UserParameters, UserValues, UtilParameters):
    """collector class for parameter objects"""
