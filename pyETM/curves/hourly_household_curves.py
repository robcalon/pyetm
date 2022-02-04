import io
import pandas

class HourlyHouseholdCurves:
    
    @property
    def hourly_household_curves(self):
        
        # get hourly household heat curves
        if self._hourly_household_curves is None:
            self.get_hourly_household_curves()
        
        return self._hourly_household_curves
            
    def get_hourly_household_curves(self):
        """get the hourly household heat curves"""
        
        # raise without scenario id
        self._raise_scenario_id()
        
        # prepare post
        headers = {'Connection':'close'}
        post = f'/scenarios/{self.scenario_id}/curves/household_heat'
        
        # request response and extract data
        resp = self.get(post, headers=headers)
        data = io.StringIO(resp)
        
        # convert data to dataframe and set DateTime
        curves = pandas.read_csv(data)
        
        # set corresponsing parameter property
        self._hourly_household_curves = curves
        
        return curves