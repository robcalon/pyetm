import pandas as pd

class EnergyFlows:
    
    @property
    def energy_flows(self):
        
        # get energy flows
        if self._energy_flows is None:
            self.get_energy_flows()
        
        return self._energy_flows
    
    def get_energy_flows(self):
        """get the energy flows"""

        # raise without scenario id
        self._raise_scenario_id()
                
        # make request
        url = f'scenarios/{self.scenario_id}/energy_flow'
        resp = self.session.get(url, decoder="BytesIO")

        # convert to frame
        flows = pd.read_csv(resp, index_col='key')
        
        # set corresponsing parameter property
        self._energy_flows = flows

        return flows