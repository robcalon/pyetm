import json
import numpy
import pandas

class UserParameters:
    
    @property
    def user_parameters(self):
        
        # get user parameters
        if self._user_parameters is None:
            self.get_user_parameters()
        
        return self._user_parameters
    
    @user_parameters.setter
    def user_parameters(self, uparams):
        raise AttributeError('protected attribute; change user values instead.')
    
    def get_user_parameters(self, **kwargs):
        """get configuration information of all available user values"""

        # raise without scenario id
        self._raise_scenario_id()
        
        # prepare post
        headers = {'Connection':'close'}
        post = f'/scenarios/{self.scenario_id}/inputs'

        # request response and convert to df
        resp = self.get(post, headers=headers, **kwargs)
        uparams = pandas.DataFrame.from_dict(json.loads(resp), orient='index')
        
        # add user to column when absent
        if 'user' not in uparams.columns:
            uparams.insert(loc=5, column='user', value=numpy.nan)

        # convert user dtype to object
        uparams.user = uparams.user.astype('object')
        
        # set corresponsing parameter property
        self._user_parameters = uparams

        return uparams