class DisplayGroup:
    
    @property
    def display_group(self):
        
        # get scenario header
        if self._scenario_header is None:
            self._get_scenario_header()
        
        return self._scenario_header['display_group']