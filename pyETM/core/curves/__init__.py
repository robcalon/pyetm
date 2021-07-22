from .hourly_electricity_curves import HourlyElectricityCurves
from .hourly_electricity_price_curve import HourlyElectricityPriceCurve
from .hourly_heat_network_curves import HourlyHeatNetworkCurves
from .hourly_household_heat_curves import HourlyHouseholdHeatCurves
from .hourly_hydrogen_curves import HourlyHydrogenCurves
from .hourly_network_gas_curves import HourlyNetworkGasCurves


class Curves(HourlyElectricityCurves, HourlyElectricityPriceCurve,
             HourlyHeatNetworkCurves, HourlyHouseholdHeatCurves, 
             HourlyHydrogenCurves, HourlyNetworkGasCurves):
    
    def __init__(self):
        super().__init__()