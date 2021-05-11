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
        
        # request response and convert to df
        resp = self.get(post, headers=headers)
        curves = pandas.read_csv(io.StringIO(resp))
        
        # set corresponsing parameter property
        self._hourly_network_gas_curves = curves
        
        return curves