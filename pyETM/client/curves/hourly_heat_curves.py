import functools
import pandas as pd

class HourlyHeatCurves:
    
    @property
    def hourly_heat_curves(self):
        return self.get_hourly_heat_curves()
                        
    @functools.lru_cache
    def get_hourly_heat_curves(self):
        """get the hourly heat network curves"""
        
        # raise without scenario id
        self._raise_scenario_id()
        
        # request response and convert to frame
        post = f'scenarios/{self.scenario_id}/curves/heat_network'
        resp = self.session.get(post, decoder="BytesIO")

        # convert to frame and reset index
        curves = pd.read_csv(resp, index_col='Time')
        curves = curves.reset_index(drop=True)
                        
        return curves