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
        """Resets user values and heat network order
        to default settings."""
        
        # set reset parameter
        data = {"reset": True}
        
        # prepare post
        headers = {'Connection':'close'}
        post = f'/scenarios/{self.scenario_id}'
        
        # request response and convert to df
        self.put(post, json=data, headers=headers, **kwargs)
        
        # reinitialize connected scenario
        self._reset_session()
        
#     def get_scenario_templates(self, **kwargs):
#         """get the available scenario templated within the ETM"""
        
#         # prepare post
#         headers = {'Connection':'close'}
#         post = '/scenarios/templates'
        
#         # request response and convert to df
#         resp = self.get(post, headers=headers, **kwargs)
#         templates = pandas.DataFrame.from_dict(json.loads(resp))        
#         templates['id'] = templates['id'].astype(str)
        
#         return templates
    
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
        
    def create_new_scenario(self, end_year, area_code, 
                            title=None, protected=False, **kwargs):
        """Create a new scenario on the ETM server. 
                
        Parameters
        ----------
        end_year : int
            End year of the created scenario
        area_code : str
            Area code of the created scenario
        title : str, default None
            Title of the created scenario
        protected : boolean, default False
            Created a protected scenario (should prevent scenario
            from being able to be edited through API?)
        
        Kwargs are passed to underlying HTTP client."""
        
        # default scenario
        if isinstance(end_year, str):
            end_year = int(end_year)
        
        # make scenario dict based on args
        scenario = {'end_year': end_year, 'area_code' : area_code}
        
        # user specified title
        if title is not None:
            scenario['title'] = title
            
        # protected scenario
        if protected is True:
            scenario['protected'] = protected
            
        self._create_new_scenario(scenario=scenario, **kwargs)
        
    def _create_new_scenario(self, scenario=None, **kwargs):
        """Create a new scenario on the ETM server.
        
        Parameters
        ----------
        scenario : dict
            Dictonairy with scenario keys. The scenario must contain an
            area_code and a end_year key."""
        
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