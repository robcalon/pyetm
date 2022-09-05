import functools
import pandas as pd

class ProductionParameters:
    
    @property
    def production_parameters(self):
        return self.get_production_parameters()
    
    @functools.lru_cache
    def get_production_parameters(self):
        """get the production parameters"""

        # raise without scenario id
        self._raise_scenario_id()
        
        # make request
        url = f'scenarios/{self.scenario_id}/production_parameters'
        resp = self.session.get(url, decoder="BytesIO")

        return pd.read_csv(resp)
