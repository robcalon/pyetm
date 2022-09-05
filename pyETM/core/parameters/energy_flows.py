import functools
import pandas as pd

class EnergyFlows:
    
    @property
    def energy_flows(self):
        return self.get_energy_flows()
    
    @functools.lru_cache
    def get_energy_flows(self):
        """get the energy flows"""

        # raise without scenario id
        self._raise_scenario_id()
                
        # make request
        url = f'scenarios/{self.scenario_id}/energy_flow'
        resp = self.session.get(url, decoder="BytesIO")

        # convert to frame
        flows = pd.read_csv(resp, index_col='key')
        
        return flows