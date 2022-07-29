import pandas as pd

class HourlyMethaneCurves:
    
    @property
    def hourly_methane_curves(self):
        
        # get hourly network gas curves
        if self._hourly_methane_curves is None:
            self.get_hourly_methane_curves()
        
        return self._hourly_methane_curves
            
    def get_hourly_methane_curves(self):
        """get the hourly methane curves"""
        
        # raise without scenario id
        self._raise_scenario_id()
        
        # prepare post
        headers = {'Connection':'close'}
        post = f'/scenarios/{self.scenario_id}/curves/network_gas'
        
        # request response and convert to frame
        resp = self.get(post, decoder="BytesIO", headers=headers)
        curves = pd.read_csv(resp, index_col='Time').reset_index(drop=True)
                
        # set corresponsing parameter property
        self._hourly_methane_curves = curves
        
        return curves