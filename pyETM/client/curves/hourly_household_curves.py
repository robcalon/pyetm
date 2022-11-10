import functools
import pandas as pd

class HourlyHouseholdCurves:
    
    @property
    def hourly_household_curves(self):
        return self.get_hourly_household_curves()

    @functools.lru_cache  
    def get_hourly_household_curves(self):
        """get the hourly household heat curves"""
        
        # raise without scenario id
        self._raise_scenario_id()
        
        # make request
        post = f'scenarios/{self.scenario_id}/curves/household_heat'
        resp = self.session.get(post, decoder="BytesIO")
                
        return pd.read_csv(resp)