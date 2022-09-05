from pyETM.logger import get_modulelogger

# get modulelogger
logger = get_modulelogger(__name__)


class ScenarioParameters:
    
    @property
    def scenario_parameters(self):
        """all user values including non-user defined parameters"""
        
        # get user and fillna with default
        uparams = self.user_parameters
        sparams = uparams.user.fillna(uparams.default)
        
        # set name of series
        sparams.name = 'scenario'
        
        return sparams

    @scenario_parameters.setter
    def scenario_parameters(self, sparams):
        
        # check and set scenario parameters
        self._check_scenario_parameters(sparams)
        self.change_user_values(sparams)
        
    def _check_scenario_parameters(self, sparams=None):
        """Utility function to check the validity of the scenario 
        parameters that are set in the scenario."""
        
        # default sparams
        if sparams is None:
            sparams = self.scenario_parameters
        
        # check passed parameters as user values
        sparams = self._check_user_values(sparams)
        
        # ensure that they are complete
        passed = self.scenario_parameters.index.isin(sparams.index)
        if not passed.all():
            missing = self.scenario_parameters[~passed]
            
            # warn for each missing key
            for key in missing.index:
                logger.warning(f"'{key}' not in passed scenario parameters")
