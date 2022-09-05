import pandas

class UserValues:
    
    @property
    def user_values(self):
        """all user set values without non-user defined parameters"""
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
        
        """Check for parameters in your parameter settings that are disabled 
        by parameters that you are attempting to set
        
        This should not be done upon checking, as this is only relevant when
        attempting to change"""
        
        # convert uvalues to dict
        uvalues = uvalues.to_dict()
        
        # map values to correct scenario parameters
        data = {"scenario": {"user_values": uvalues}, "detailed": True}
        
        # evaluate request
        url = f'scenarios/{self.scenario_id}'
        self.session.put(url, json=data)
        
        # reinitialize scenario
        self.reset_session()
        
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
            
        # # check for invalid key and values
        # self._check_user_value_keys(uvalues)
        # self._check_user_value_values(uvalues)
        # self._check_sharegroups(uvalues)
        
        return uvalues
    
    # def _check_user_value_keys(self, uvalues):
    #     """check if passed user values can be set in ETM."""
        
    #     # use input values to include disabled 
    #     ivalues = self.input_values
        
    #     # check for unsupported names
    #     invalid = ~uvalues.index.isin(ivalues.index)
    #     errors = uvalues[invalid]
        
    #     # raise errors
    #     for key in errors.index:
    #         raise KeyError(f'"{key}" is not a valid user value')
        
    #     # check for disabled parameters
    #     disabled = ivalues.index[ivalues.disabled == True]
    #     errors = uvalues[uvalues.index.isin(disabled)]
        
    #     # raise errors
    #     for key in errors.index:
    #         raise KeyError(f'"{key}" is a (recently) disabled user value')
                            
    # def _check_user_value_values(self, uvalues):
    #     """check if passed user values are inside the ETM bounds."""
        
    #     # subset user parameters and update user values
    #     parameters = self.user_parameters.loc[uvalues.index]
    #     parameters.user = uvalues
        
    #     # check for values that exceed upper bound
    #     ubound = parameters[~parameters['max'].isna()]
    #     errors = ubound[ubound.user > ubound['max']]
        
    #     # raise errors
    #     for key in errors.index:
    #         raise ValueError(f'"{key}" value exceeds upper bound in ETM')
        
    #     # check for values that exceed lower bound
    #     lbound = parameters[~parameters['min'].isna()]
    #     errors = lbound[lbound.user < lbound['min']]
        
    #     # raise errors
    #     for key, values in errors.iterrows():
    #         mval, uval = values['min'], values['user']
    #         raise ValueError(f'"{key}" value exceeds lower bound in ETM, {uval} < {mval}')
        
    #     # subset categorical bound values
    #     cbound = parameters[~parameters['permitted_values'].isna()]
        
    #     # raise errors
    #     for key, value in cbound.iterrows():
    #         if value.user not in value.permitted_values:
    #             raise ValueError(f'"{key}" value is not permitted in ETM')
                
    # def _check_sharegroups(self, uvalues, debug=False):
    #     """check if share groups are balanced"""
            
    #     """TO DO
    #     Make this an optional check when a subset of the entire group is set. 
    #     ETEngine changes other parameters for which user should be warned."""

    #     # get grouper from user parameters
    #     groups = self.user_parameters.share_group
    #     percentage = uvalues.groupby(groups).sum()
        
    #     # set float and round for small errors
    #     percentage = percentage.astype('float64').round(3)

    #     # check unbalanced share groups
    #     if (percentage != 100).any():
    #         unbalanced = percentage[(percentage != 100)]
            
    #         if debug is True:
    #             return unbalanced
                                
    #         else:
    #             groups = list(unbalanced.index)
    #             raise ValueError(f'sharegroups "{groups}" do not ' + 
    #                              'add up to 100 percent')
        
    def _get_sharegroup(self, key):
        """return subset of parameters in share group"""

        # get user and scenario parameters
        uparams = self.user_parameters
        sparams = self.scenario_parameters
        
        return sparams[uparams.share_group == key]