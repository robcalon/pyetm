from .hourly_electricity_curves import HourlyElectricityCurves
from .hourly_electricity_price_curve import HourlyElectricityPriceCurve
from .hourly_heat_curves import HourlyHeatCurves
from .hourly_household_curves import HourlyHouseholdCurves
from .hourly_hydrogen_curves import HourlyHydrogenCurves
from .hourly_methane_curves import HourlyMethaneCurves


class Curves(HourlyElectricityCurves, HourlyElectricityPriceCurve,
             HourlyHeatCurves, HourlyHouseholdCurves, 
             HourlyHydrogenCurves, HourlyMethaneCurves):
    
    def __init__(self):
        super().__init__()