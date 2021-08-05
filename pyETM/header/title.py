class Title:
    
    @property
    def title(self):
        
        # get scenario header
        if self._scenario_header is None:
            self._get_scenario_header()
            
        return self._scenario_header['title']
    
    @title.setter
    def title(self, title):
        
        # format title and update
        header = {'title': title}
        self._change_scenario_header(header)