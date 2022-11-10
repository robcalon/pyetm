import pandas as pd
import functools

class ApplicationDemands:
    
    @property
    def application_demands(self):
        return self.get_application_demands()
    
    @functools.lru_cache
    def get_application_demands(self):
        """get the application demands"""

        # raise without scenario id
        self._raise_scenario_id()
        
        # make request
        url = f'scenarios/{self.scenario_id}/application_demands'
        resp = self.session.get(url, decoder="BytesIO")

        return pd.read_csv(resp, index_col='key')
