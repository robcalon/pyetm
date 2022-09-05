import pandas

class Scenario:
            
    def reset_scenario(self):
        """Resets user values and heat network order
        to default settings."""
        
        # set reset parameter
        data = {"reset": True}
        
        
        # make request
        url = f'scenarios/{self.scenario_id}'
        self.session.put(url, json=data)
        
        # reinitialize connected scenario
        self._reset_session()
    
    def create_scenario_copy(self, scenario_id):
        """Create a new scenario that is a copy of an existing scenario
        based on its id"""
        
        # make and set scenario
        scenario = {'scenario_id': str(scenario_id)}
        data = {"scenario": scenario}

        # request response
        url = 'scenarios'
        resp = self.session.post(url, json=data)
        
        # update the scenario_id
        self.scenario_id = str(resp['id'])
        
    def create_new_scenario(self, end_year, area_code, 
                            metadata=None, keep_compatible=False, 
                            read_only=False):
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
            Created a protected scenario."""
        
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
                        
        self._create_new_scenario(scenario=scenario)
        
    def _create_new_scenario(self, scenario=None):
        """Create a new scenario on the ETM server.
        
        Parameters
        ----------
        scenario : dict
            Dictonairy with scenario keys. The scenario must contain an
            area_code and a end_year key."""
        
        # default scenario
        if scenario is None:
            scenario = {}
        
        # set scenario parameter
        data = {"scenario": scenario}

        # make request
        url = 'scenarios'
        response = self.session.post(url, json=data)
        
        # update scenario_id
        self.scenario_id = str(response['id'])