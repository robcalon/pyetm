class StartYear:
    
    @property
    def start_year(self):
        
        # get scenario header
        if self._scenario_header is None:
            self._get_scenario_header()
        
        return self._scenario_header['start_year']