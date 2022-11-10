import functools
import pandas as pd

class HourlyElectricityPriceCurve:
    
    @property
    def hourly_electricity_price_curve(self):
        return self.get_hourly_electricity_price_curve()

    @functools.lru_cache        
    def get_hourly_electricity_price_curve(self):
        """get the hourly electricity price curve"""
        
        # raise without scenario id
        self._raise_scenario_id()
        
        # make request
        post = f'scenarios/{self.scenario_id}/curves/electricity_price'
        resp = self.session.get(post, decoder="BytesIO")    

        # convert to frame and reset index
        curves = pd.read_csv(resp, index_col='Time')
        curves = curves.squeeze('columns').reset_index(drop=True)
                
        return curves