import io
import pandas

class HourlyHouseholdHeatCurves:
    
    @property
    def hourly_household_heat_curves(self):
        
        # get hourly household heat curves
        if self._hourly_household_heat_curves is None:
            self.get_hourly_household_heat_curves()
        
        return self._hourly_household_heat_curves
            
    def get_hourly_household_heat_curves(self):
        """get the hourly household heat curves"""
        
        # raise without scenario id
        self._raise_scenario_id()
        
        # prepare post
        headers = {'Connection':'close'}
        post = f'/scenarios/{self.scenario_id}/curves/household_heat'
        
        # request response and convert to df
        resp = self.get(post, headers=headers)
        curves = pandas.read_csv(io.StringIO(resp))
        
        # set corresponsing parameter property
        self._hourly_household_heat_curves = curves
        
        return curves