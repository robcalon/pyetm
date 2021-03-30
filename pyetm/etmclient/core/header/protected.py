class Protected:
    
    @property
    def protected(self):
        
        # get scenario header
        if self._scenario_header is None:
            self._get_scenario_header()
        
        return self._scenario_header['protected']
    
    @protected.setter
    def protected(self, boolean):
        
        # format header and update
        header = {'protected': boolean}
        self._change_scenario_header(header)