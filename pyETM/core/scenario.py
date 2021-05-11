import json
import pandas

class Scenario:
    
#     @property
#     def scenario(self):
#         return self._scenario

#     @scenario.setter
#     def scenario(self, scenario):
#         self.set_scenario(scenario)
        
    def reset_scenario(self, **kwargs):
        """Resets user values, flexibility order and heat network order
        to their default configurations."""
        
        # set reset parameter
        data = {"reset": True}
        
        # prepare post
        headers = {'Connection':'close'}
        post = f'/scenarios/{self.scenario_id}'
        
        # request response and convert to df
        self.put(post, json=data, headers=headers, **kwargs)
        
        # reinitialize connected scenario
        self._reset_session()
        
    def get_scenario_templates(self, **kwargs):
        """get the available scenario templated within the ETM"""
        
        # prepare post
        headers = {'Connection':'close'}
        post = '/scenarios/templates'
        
        # request response and convert to df
        resp = self.get(post, headers=headers, **kwargs)
        templates = pandas.DataFrame.from_dict(json.loads(resp))
        
        return templates
    
    def create_scenario_copy(self, scenario_id, **kwargs):
        """Create a new scenario that is a copy of an existing scenario
        based on its id"""
        
        # make scenario
        scenario = {'scenario_id': str(scenario_id)}
                        
        # post scenario details
        data = {"scenario": scenario}
        headers = {'Connection':'close'}
        
        # request response
        resp = self.post('/scenarios', json=data, headers=headers, **kwargs)
        
        # update the scenario_id
        self.scenario_id = str(resp['id'])
        
    def create_new_scenario(self, scenario=None, **kwargs):
        """Create a new scenario on the ETM server. 
                
        Parameters
        ----------
        scenario : dict
            Dictonairy with scenario keys. The scenario must contain title,
            area_code, end_year and protected key, value pairs."""
        
        # default scenario
        if scenario is None:
            scenario = {}
        
        # prepare post
        data = {"scenario": scenario}
        headers = {'Connection':'close'}
        
        # post scenario details
        response = self.post('/scenarios', json=data, headers=headers, **kwargs)
        
        # update scenario_id
        self.scenario_id = str(response['id'])