import io
import pandas

class HourlyHeatNetworkCurves:
    
    @property
    def hourly_heat_network_curves(self):
        
        # get hourly heat network curves
        if self._hourly_heat_network_curves is None:
            self.get_hourly_heat_network_curves()
            
        return self._hourly_heat_network_curves
            
    def get_hourly_heat_network_curves(self):
        """get the hourly heat network curves"""
        
        # raise without scenario id
        self._raise_scenario_id()
        
        # prepare post
        headers = {'Connection':'close'}
        post = f'/scenarios/{self.scenario_id}/curves/heat_network'
        
        # request response and extract data
        resp = self.get(post, headers=headers)
        data = io.StringIO(resp)
        
        # convert data to dataframe and set DateTime
        curves = pandas.read_csv(data, index_col='Time', 
                                 parse_dates=True).asfreq('H')
        curves.index.name = 'DateTime'
        
        # set corresponsing parameter property
        self._hourly_heat_network_curves = curves
        
        return curves