import pandas as pd

class HourlyHydrogenCurves:
    
    @property
    def hourly_hydrogen_curves(self):
        
        # get hourly hydrogen curves
        if self._hourly_hydrogen_curves is None:
            self.get_hourly_hydrogen_curves()
        
        return self._hourly_hydrogen_curves
            
    def get_hourly_hydrogen_curves(self):
        """get the hourly hydrogen curves"""
        
        # raise without scenario id
        self._raise_scenario_id()
        
        # prepare post
        headers = {'Connection':'close'}
        post = f'/scenarios/{self.scenario_id}/curves/hydrogen'
        
        # request response and convert to frame
        resp = self.get(post, decoder="BytesIO", headers=headers)
        curves = pd.read_csv(resp, index_col='Time').reset_index(drop=True)
        
        # set corresponsing parameter property
        self._hourly_hydrogen_curves = curves
        
        return curves