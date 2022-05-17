import numpy
import pandas

class InputValues:
    
    @property
    def input_values(self):
        
        # get user parameters
        if self._input_values is None:
            self.get_input_values()
        
        return self._input_values
    
    @input_values.setter
    def input_values(self, uparams):
        raise AttributeError('protected attribute; change user values instead.')
    
    def get_input_values(self, **kwargs):
        """get configuration information of all available input parameters. 
        direct dump of inputs json from engine."""

        # raise without scenario id
        self._raise_scenario_id()
        
        # prepare post
        headers = {'Connection':'close'}
        url = f'/scenarios/{self.scenario_id}/inputs'

        # request response and convert to df
        resp = self.get(url, headers=headers, **kwargs)
        ivalues = pandas.DataFrame.from_dict(resp, orient='index')
        
        # add user to column when absent
        if 'user' not in ivalues.columns:
            ivalues.insert(loc=5, column='user', value=numpy.nan)

        # convert user dtype to object and set disabled
        ivalues.user = ivalues.user.astype('object')
        ivalues.disabled = ivalues.disabled.fillna(False)
        
        # set corresponsing parameter property
        self._input_values = ivalues

        return ivalues