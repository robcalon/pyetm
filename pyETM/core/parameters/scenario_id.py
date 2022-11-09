import copy

from pyETM.logger import get_modulelogger

# get modulelogger
logger = get_modulelogger(__name__)


class ScenarioID:
    
    def __init__(self):
        self._scenario_id = None
    
    @property
    def scenario_id(self):
        return self._scenario_id
    
    @scenario_id.setter
    def scenario_id(self, scenario_id):
        self.change_scenario_id(scenario_id)
                
    def change_scenario_id(self, scenario_id: str):
        """change the connected scenario."""

        # store previous and validate new scenario id
        previous = copy.deepcopy(self.scenario_id)

        # try accessing dict
        if isinstance(scenario_id, dict):
            scenario_id = scenario_id['id']

        # convert passed id to string        
        if not (isinstance(scenario_id, str) | (scenario_id is None)):
            scenario_id = str(scenario_id)

        # set new scenario id
        self._scenario_id = scenario_id

        # log changed scenario id
        if self.scenario_id != previous:
            logger.debug(f"Updated scenario_id: '{self.scenario_id}'")

        # reset session
        if self.scenario_id != previous:
            self.reset_session()

        # validate scenario id
        self.get_scenario_header()

    def _raise_scenario_id(self):
        """raise error when scenario id is None"""

        if self.scenario_id is None:
            raise ValueError('scenario id is None')