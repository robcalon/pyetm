# import property classes
from .parameters.application_demands import ApplicationDemands
from .parameters.energy_flows import EnergyFlows
from .parameters.flexibility_order import FlexibilityOrder
from .parameters.heat_network_order import HeatNetworkOrder
from .parameters.production_parameters import ProductionParameters
from .parameters.scenario_id import ScenarioID
from .parameters.user_parameters import UserParameters
from .parameters.user_values import UserValues


class Parameters(ApplicationDemands, EnergyFlows, FlexibilityOrder, 
                 HeatNetworkOrder, ProductionParameters, ScenarioID, 
                 UserParameters, UserValues):
    
    def __init__(self):
        super().__init__()