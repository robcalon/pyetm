class AreaCode:
        
    @property
    def area_code(self):
        
        # get scenario header
        if self._scenario_header is None:
            self._get_scenario_header()
        
        return self._scenario_header['area_code']