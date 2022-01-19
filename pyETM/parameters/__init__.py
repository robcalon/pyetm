from .application_demands import ApplicationDemands
from .energy_flows import EnergyFlows
# from .flexibility_order import FlexibilityOrder
from .heat_network_order import HeatNetworkOrder
from .production_parameters import ProductionParameters
from .scenario_id import ScenarioID
from .user_parameters import UserParameters
from .user_values import UserValues


class Parameters(ApplicationDemands, EnergyFlows, HeatNetworkOrder, 
                 ProductionParameters, ScenarioID, UserParameters, UserValues):
    
    def __init__(self):
        super().__init__()