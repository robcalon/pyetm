import io
import pandas

class HourlyNetworkGasCurves:
    
    @property
    def hourly_network_gas_curves(self):
        
        # get hourly network gas curves
        if self._hourly_network_gas_curves is None:
            self.get_hourly_network_gas_curves()
        
        return self._hourly_network_gas_curves
            
    def get_hourly_network_gas_curves(self):
        """get the hourly network gas curves"""
        
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
        self._hourly_network_gas_curves = curves
        
        return curves