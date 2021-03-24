import io
import pandas

class ProductionParameters:
    
    @property
    def production_parameters(self):
        return self._production_parameters
    
    def get_production_parameters(self, **kwargs):
        """get the production parameters"""

        # raise without scenario id
        self._raise_scenario_id()
        
        # prepare post
        headers = {'Connection':'close'}
        post = f'/scenarios/{self.scenario_id}/production_parameters'
        
        # request response and convert to df
        resp = self.get(post, headers=headers, **kwargs)
        parameters = pandas.read_csv(io.StringIO(resp))
        
        # set corresponsing parameter property
        self._production_parameters = parameters

        return parameters