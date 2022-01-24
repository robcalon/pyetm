class UserParameters:
    
    @property
    def user_parameters(self):
        return self.get_user_parameters()
    
    @user_parameters.setter
    def user_parameters(self, uparams):
        raise AttributeError('protected attribute; change user values instead.')        
    
    def get_user_parameters(self, **kwargs):
        """get configuration information of all available user parameters"""
        
        # raise without scenario id
        self._raise_scenario_id()
        
        # drop disabled parameters
        ivalues = self.input_values
        uparams = ivalues[ivalues.disabled == False]
        
        # set corresponsing parameter property
        self._user_parameters = uparams

        return uparams
