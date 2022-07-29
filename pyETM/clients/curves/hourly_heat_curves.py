import pandas as pd

class HourlyHeatCurves:
    
    @property
    def hourly_heat_curves(self):
        
        # get hourly heat network curves
        if self._hourly_heat_curves is None:
            self.get_hourly_heat_curves()
            
        return self._hourly_heat_curves
            
    def get_hourly_heat_curves(self):
        """get the hourly heat network curves"""
        
        # raise without scenario id
        self._raise_scenario_id()
        
        # prepare post
        headers = {'Connection':'close'}
        post = f'/scenarios/{self.scenario_id}/curves/heat_network'
        
        # request response and convert to frame
        resp = self.get(post, decoder="BytesIO", headers=headers)
        curves = pd.read_csv(resp, index_col='Time').reset_index(drop=True)
                
        # set corresponsing parameter property
        self._hourly_heat_curves = curves
        
        return curves