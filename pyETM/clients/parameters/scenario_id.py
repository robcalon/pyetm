import copy
import logging

logger = logging.getLogger(__name__)


class ScenarioID:
    
    def __init__(self):
        self._scenario_id = None
    
    @property
    def scenario_id(self):
        return self._scenario_id
    
    @scenario_id.setter
    def scenario_id(self, scenario_id):
        self.change_scenario_id(scenario_id)
                
    def change_scenario_id(self, scenario_id, **kwargs):
        """change the connected scenario."""
        
        # check if scenario id works
        scenario_id = self.check_scenario_id(scenario_id, **kwargs)
        
        # store previous scenario id
        previous = copy.deepcopy(self.scenario_id)
        
        # set new scenario id
        self._scenario_id = scenario_id
        
        # reinitialize scenario
        self._reset_session()
        
        # specify warning conditions
        c1 = previous is not None
        c2 = self.scenario_id is not None
        c3 = self.scenario_id != previous
        
        # warn about changed id
        if all((c1, c2, c3)) is True:
            logger.warning(f'scenario_id changed to {self.scenario_id}')
        
    def check_scenario_id(self, scenario_id, **kwargs):
        """check if scenario id responds"""
        
        # convert intiger to string
        if isinstance(scenario_id, int):
            scenario_id = str(scenario_id)
            
        # try accessing dict
        if isinstance(scenario_id, dict):
            scenario_id = scenario_id['id']
        
        # ignore None tests
        if scenario_id is None:
            return scenario_id
                
        # prepare validation request
        url = f'/scenarios/{scenario_id}'
        headers = {'Connection': 'close'}

        # make validation request
        self.get(url, headers=headers, **kwargs)
        
        return scenario_id
    
    def _raise_scenario_id(self):
        """raise error when scenario id is None"""

        if self.scenario_id is None:
            raise ValueError('scenario id is None')