import pandas

class Scenario:
            
    def reset_scenario(self, **kwargs):
        """Resets user values and heat network order
        to default settings."""
        
        # set reset parameter
        data = {"reset": True}
        
        # prepare request
        headers = {'Connection':'close'}
        url = f'/scenarios/{self.scenario_id}'
        
        # request response and convert to df
        self.put(url, json=data, headers=headers, **kwargs)
        
        # reinitialize connected scenario
        self._reset_session()
    
    def create_scenario_copy(self, scenario_id, **kwargs):
        """Create a new scenario that is a copy of an existing scenario
        based on its id"""
        
        # make scenario
        scenario = {'scenario_id': str(scenario_id)}
                        
        # request scenario details
        data = {"scenario": scenario}
        headers = {'Connection':'close'}
        
        # request response
        resp = self.post('/scenarios', json=data, headers=headers, **kwargs)
        
        # update the scenario_id
        self.scenario_id = str(resp['id'])
        
    def create_new_scenario(self, end_year, area_code, 
                            metadata=None, keep_compatible=False, 
                            read_only=False, **kwargs):
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
        
        # set metadata
        if metadata is not None:
            scenario['metadata'] = metadata
            
        # set protection settings
        scenario['keep_compatible'] = keep_compatible
        scenario['read_only'] = read_only
                        
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
        
        # prepare request
        data = {"scenario": scenario}
        headers = {'Connection':'close'}
        
        # post scenario details
        response = self.post('/scenarios', json=data, headers=headers, **kwargs)
        
        # update scenario_id
        self.scenario_id = str(response['id'])