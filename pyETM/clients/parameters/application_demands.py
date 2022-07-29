import pandas as pd

class ApplicationDemands:
    
    @property
    def application_demands(self):
        
        # get application demands
        if self._application_demands is None:
            self.get_application_demands()
            
        return self._application_demands
    
    def get_application_demands(self, **kwargs):
        """get the application demands"""

        # raise without scenario id
        self._raise_scenario_id()
        
        # prepare post
        headers = {'Connection':'close'}
        url = f'/scenarios/{self.scenario_id}/application_demands'
        
        # request response and convert to df
        resp = self.get(url, decoder="BytesIO", headers=headers, **kwargs)
        demands = pd.read_csv(resp, index_col='key')
        
        # set corresponsing parameter property
        self._application_demands = demands

        return demands
