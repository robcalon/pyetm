import pandas as pd

class HourlyElectricityPriceCurve:
    
    @property
    def hourly_electricity_price_curve(self):
        
        # get hourly electricity price curve
        if self._hourly_electricity_price_curve is None:
            self.get_hourly_electricity_price_curve()
        
        return self._hourly_electricity_price_curve
            
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
        
        # set corresponsing parameter property
        self._hourly_electricity_price_curve = curves
        
        return curves