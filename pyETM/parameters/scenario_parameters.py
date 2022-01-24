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