# import property classes
from .curves.hourly_electricity_curves import HourlyElectricityCurves
from .curves.hourly_electricity_price_curve import HourlyElectricityPriceCurve
from .curves.hourly_heat_network_curves import HourlyHeatNetworkCurves
from .curves.hourly_household_heat_curves import HourlyHouseholdHeatCurves
from .curves.hourly_hydrogen_curves import HourlyHydrogenCurves
from .curves.hourly_network_gas_curves import HourlyNetworkGasCurves


class Curves(HourlyElectricityCurves, HourlyElectricityPriceCurve,
             HourlyHeatNetworkCurves, HourlyHouseholdHeatCurves, 
             HourlyHydrogenCurves, HourlyNetworkGasCurves):
    
    def __init__(self):
        super().__init__()