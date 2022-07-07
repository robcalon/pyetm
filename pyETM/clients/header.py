import pandas as pd


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
                
        # get created at
        datetime = self._scenario_header['created_at']
        datetime = pd.to_datetime(datetime, utc=True)
        
        return datetime
    
    @property
    def display_group(self):
        return self._scenario_header['display_group']

    @property
    def end_year(self):            
        return self._scenario_header['end_year']
    
    @property
    def esdl_exportable(self):
        return self._scenario_header['esdl_exportable']
    
    @property
    def keep_compatible(self):
        return self._scenario_header['keep_compatible']
    
    @keep_compatible.setter
    def keep_compatible(self, boolean):
        
        # format header and update
        header = {'keep_compatible': bool(boolean)}
        self._change_scenario_header(header)
    
    @property
    def metadata(self):
        return self._scenario_header.get('metadata')
    
    @metadata.setter
    def metadata(self, metadata):
        
        # format header and update
        header = {'metadata': dict(metadata)}
        self._change_scenario_header(header)
    
    @property
    def ordering(self):
        return self._scenario_header['ordering']
    
#     @property
#     def protected(self):
#         return self._scenario_header['protected']
    
#     @protected.setter
#     def protected(self, boolean):
        
#         # format header and update
#         header = {'protected': boolean}
#         self._change_scenario_header(header)
        
    @property
    def read_only(self):
        return self._scenario_header['read_only']
        
    @read_only.setter
    def read_only(self, boolean):
        
        # format header and update
        header = {'read_only': bool(boolean)}
        self._change_scenario_header(header)
        
    @property
    def scaling(self):
        return self._scenario_header['scaling']
    
    @property
    def source(self):
        return self._scenario_header['source']
    
    @property
    def start_year(self):
        return self._scenario_header['start_year']
    
    @property
    def template(self):
        return str(self._scenario_header['template'])
    
#     @property
#     def title(self):
#         return self._scenario_header['title']
    
#     @title.setter
#     def title(self, title):
        
#         # format title and update
#         header = {'title': title}
#         self._change_scenario_header(header)
        
    @property
    def updated_at(self):
                
        # get created at
        datetime = self._scenario_header['updated_at']
        datetime = pd.to_datetime(datetime, utc=True)
        
        return datetime
        
    @property
    def url(self):
        return self._scenario_header['url']

    @property
    def pro_url(self):
        
        # specify base url
        base = 'https://pro.energytransitionmodel.com'

        # update to beta server
        if self.beta_engine:
            base = base.replace('https://', 'https://beta-')

        return base + '/scenarios/%s/load' %self.scenario_id
    
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
        headers = {'detailed': 'true', 'Connection':'close'}
        url = f'/scenarios/{self.scenario_id}'
        
        # evaluate request
        self.put(url, json=data, **kwargs)
        
        # reinitialize scenario
        self._reset_session()
    