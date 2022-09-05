import pandas as pd

class HourlyHouseholdCurves:
    
    @property
    def hourly_household_curves(self):
        
        # get hourly household heat curves
        if self._hourly_household_curves is None:
            self.get_hourly_household_curves()
        
        return self._hourly_household_curves
            
    def get_hourly_household_curves(self):
        """get the hourly household heat curves"""
        
        # raise without scenario id
        self._raise_scenario_id()
        
        # make request
        post = f'scenarios/{self.scenario_id}/curves/household_heat'
        resp = self.session.get(post, decoder="BytesIO")

        # convert to frame
        curves = pd.read_csv(resp)
        
        # set corresponsing parameter property
        self._hourly_household_curves = curves
        
        return curves