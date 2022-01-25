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
        self.change_user_values(uvalues)
        
    def _check_scenario_parameters(self):
        """Utility function to check the validity of the scenario 
        parameters that are set in the scenario."""
        
        sparams = self.scenario_parameters
        self._check_user_values(sparams)