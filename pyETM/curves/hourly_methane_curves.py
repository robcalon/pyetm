import io
import pandas

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
        
        # request response and extract data
        resp = self.get(post, headers=headers)
        data = io.StringIO(resp)
        
        # convert data to dataframe and set DateTime
        curves = pandas.read_csv(data, index_col='Time', 
                                 parse_dates=True).asfreq('H')
        curves.index.name = 'DateTime'
        
        # set corresponsing parameter property
        self._hourly_methane_curves = curves
        
        return curves