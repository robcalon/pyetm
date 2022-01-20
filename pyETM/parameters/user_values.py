import pandas

class UserValues:
    
    @property
    def user_values(self):        
        return self.get_user_values()
    
    @user_values.setter
    def user_values(self, uvalues):
        self.change_user_values(uvalues)
        
    def get_user_values(self):
        """get the parameters that are configued by the user"""
        
        # raise without scenario id
        self._raise_scenario_id()
        
        # subset values from user parameter df
        uvalues = self.user_parameters['user']
        uvalues = uvalues.dropna()
    
        # set uvalues
        self._user_values = uvalues
        
        return uvalues
    
    def change_user_values(self, uvalues):
        """change the passed user values in the ETM.

        parameters
        ----------
        uvalues : pandas.Series
            collection of key, value pairs of user values."""
        
        # raise without scenario id
        self._raise_scenario_id()
        
        # validate passed user values
        uvalues = self._check_user_values(uvalues)
        
        # convert uvalues to dict
        uvalues = uvalues.to_dict()
        
        # map values to correct scenario parameters
        data = {"scenario": {"user_values": uvalues}, "detailed": True}
        
        # prepare post
        headers = {'Connection':'close'}
        post = f'/scenarios/{self.scenario_id}'
        
        # evaluate post
        self.put(post, json=data, headers=headers)
        
        # reinitialize scenario
        self._reset_session()
        
    def _check_user_values(self, uvalues):
        """check if all user values can be passed to ETM."""
        
        # convert None to dict
        if uvalues is None:
            uvalues = {}
        
        # convert dict to series
        if isinstance(uvalues, dict):
            uvalues = pandas.Series(uvalues, name='user', dtype='object')
            
        # subset series from df  
        if isinstance(uvalues, pandas.DataFrame):
            uvalues = uvalues.user
            
        # check for invalid key and values
        uvalues = self._check_user_value_keys(uvalues)
        uvalues = self._check_user_value_values(uvalues)
        
        return uvalues
        
    def _check_user_value_keys(self, uvalues):
        """check if passed user values can be set in ETM."""
        
        parameters = self.user_parameters
        
        # check for unsupported names
        invalid = ~uvalues.index.isin(parameters.index)
        errors = uvalues[invalid]
        
        # raise errors
        for key in errors.index:
            raise KeyError(f'"{key}" is not a valid user value')
        
        # check for disabled parameters
        disabled = parameters.index[parameters.disabled == True]
        errors = uvalues[uvalues.index.isin(disabled)]
        
        # raise errors
        for key in errors.index:
            raise KeyError(f'"{key}" is a disabled user value')
        
        return uvalues
        
    def _check_user_value_values(self, uvalues):
        """check if passed user values are inside the ETM bounds."""
        
        # subset user parameters and update user values
        parameters = self.user_parameters.loc[uvalues.index]
        parameters.user = uvalues
        
        # check for values that exceed upper bound
        ubound = parameters[~parameters['max'].isna()]
        errors = ubound[ubound.user > ubound['max']]
        
        # raise errors
        for key in errors.index:
            raise ValueError(f'"{key}" value exceeds upper bound in ETM')
        
        # check for values that exceed lower bound
        lbound = parameters[~parameters['min'].isna()]
        errors = lbound[lbound.user < lbound['min']]
        
        # raise errors
        for key in errors.index:
            raise ValueError(f'"{key}" value exceeds lower bound in ETM')
        
        # subset categorical bound values
        cbound = parameters[~parameters['permitted_values'].isna()]
        
        # raise errors if
        for key, value in cbound.iterrows():
            if value.user not in value.permitted_values:
                raise ValueError(f'"{key}" value is not permitted in ETM')
                
        return uvalues
