import pandas as pd

class HourlyElectricityCurves:
    
    @property
    def hourly_electricity_curves(self):
        
        # get hourly electricity curves
        if self._hourly_electricity_curves is None:
            self.get_hourly_electricity_curves()
        
        return self._hourly_electricity_curves
            
    def get_hourly_electricity_curves(self):
        """get the hourly electricity curves"""
        
        # raise without scenario id
        self._raise_scenario_id()
        
        # prepare post
        headers = {'Connection':'close'}
        post = f'/scenarios/{self.scenario_id}/curves/merit_order'
        
        # request response and convert to frame
        resp = self.get(post, decoder="BytesIO", headers=headers)
        curves = pd.read_csv(resp, index_col='Time').reset_index(drop=True)
        
        # set corresponsing parameter property
        self._hourly_electricity_curves = curves
        
        return curves