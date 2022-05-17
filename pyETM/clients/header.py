import pandas


class Header():
            
    @property
    def _scenario_header(self):
        return self._get_scenario_header()
            
    @property
    def area_code(self):
        
        # get scenario header
        if self._scenario_header is None:
            self._get_scenario_header()
        
        return self._scenario_header['area_code']
    
    @property
    def created_at(self):
        
        # get scenario header
        if self._scenario_header is None:
            self._get_scenario_header()
        
        # get created at
        datetime = self._scenario_header['created_at']
        datetime = pandas.to_datetime(datetime, utc=True)
        
        return datetime
    
    @property
    def display_group(self):
        
        # get scenario header
        if self._scenario_header is None:
            self._get_scenario_header()
        
        return self._scenario_header['display_group']

    @property
    def ordering(self):
        
        # get scenario header
        if self._scenario_header is None:
            self._get_scenario_header()
        
        return self._scenario_header['ordering']
    
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
        
    @property
    def scaling(self):
        
        # get scenario header
        if self._scenario_header is None:
            self._get_scenario_header()
        
        return self._scenario_header['scaling']
    
    @property
    def source(self):
        
        # get scenario header
        if self._scenario_header is None:
            self._get_scenario_header()
        
        return self._scenario_header['source']
    
    @property
    def start_year(self):
        
        # get scenario header
        if self._scenario_header is None:
            self._get_scenario_header()
        
        return self._scenario_header['start_year']
    
    @property
    def template(self):
        
        # get scenario header
        if self._scenario_header is None:
            self._get_scenario_header()
        
        return str(self._scenario_header['template'])
    
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
        
    @property
    def url(self):
        
        # get scenario header
        if self._scenario_header is None:
            self._get_scenario_header()
        
        return self._scenario_header['url']
    
    def _get_scenario_header(self, **kwargs):
        """get header of scenario"""
        
        # raise without scenario id
        self._raise_scenario_id()
                
        # prepare request
        headers = {'Connection':'close'}
        url = f'/scenarios/{self.scenario_id}'
        
        # request response and convert to dict
        return self.get(url, headers=headers, **kwargs)
        
    def _change_scenario_header(self, header, **kwargs):
        """change header of scenario"""

        # raise without scenario id
        self._raise_scenario_id()
        
        # set data
        data = {"scenario": header}

        # prepare request
        headers = {'Connection':'close'}
        url = f'/scenarios/{self.scenario_id}'
        
        # evaluate request
        self.put(url, json=data, **kwargs)
        
        # reinitialize scenario
        self._reset_session()
    