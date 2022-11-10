import functools
import numpy as np
import pandas as pd


class InputValues:
    
    @property
    def input_values(self):
        return self.get_input_values()
            
    @input_values.setter
    def input_values(self, uparams):
        raise AttributeError('protected attribute; change user values instead.')
    
    @functools.lru_cache
    def get_input_values(self):
        """get configuration information of all available input parameters. 
        direct dump of inputs json from engine."""

        # raise without scenario id
        self._raise_scenario_id()
        
        # make request
        url = f'scenarios/{self.scenario_id}/inputs'
        resp = self.session.get(url)

        # convert to frame
        ivalues = pd.DataFrame.from_dict(resp, orient='index')
        
        # add user to column when absent
        if 'user' not in ivalues.columns:
            ivalues.insert(loc=5, column='user', value=np.nan)

        # convert user dtype to object and set disabled
        ivalues.user = ivalues.user.astype('object')
        ivalues.disabled = ivalues.disabled.fillna(False)
        
        return ivalues