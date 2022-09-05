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
                
        # make request
        post = f'scenarios/{self.scenario_id}/curves/hydrogen'
        resp = self.session.get(post, decoder="BytesIO")

        # convert to frame and reset index
        curves = pd.read_csv(resp, index_col='Time')
        curves = curves.reset_index(drop=True)
        
        # set corresponsing parameter property
        self._hourly_hydrogen_curves = curves
        
        return curves