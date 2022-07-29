import pandas as pd

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
        url = f'/scenarios/{self.scenario_id}/production_parameters'
        
        # request response and convert to df
        resp = self.get(url, decoder="BytesIO", headers=headers, **kwargs)
        parameters = pd.read_csv(resp)
        
        # set corresponsing parameter property
        self._production_parameters = parameters

        return parameters