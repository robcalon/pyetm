import functools
import pandas as pd

class HourlyElectricityCurves:
    
    @property
    def hourly_electricity_curves(self):
        return self.get_hourly_electricity_curves()

    @functools.lru_cache            
    def get_hourly_electricity_curves(self):
        """get the hourly electricity curves"""
        
        # raise without scenario id
        self._raise_scenario_id()
        
        # make request
        post = f'scenarios/{self.scenario_id}/curves/merit_order'
        resp = self.session.get(post, decoder="BytesIO")

        # convert to frame and reset index
        curves = pd.read_csv(resp, index_col='Time')
        curves = curves.reset_index(drop=True)

        return curves