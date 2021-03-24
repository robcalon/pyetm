class Engine:
    
    @property
    def beta_engine(self):
        return self._beta_engine
        
    @beta_engine.setter
    def beta_engine(self, boolean):

        # check instance
        if not isinstance(boolean, bool):
            raise TypeError('beta_engine must be a boolean')
            
        # set boolean
        self._beta_engine = boolean
        
        # reset session
        self._reset_session()
        
    @property
    def _base_url(self):

        # default production engine url
        url = 'https://engine.energytransitionmodel.com/api/v3'
        
        # make beta engine
        if self.beta_engine is True:
            url = url.replace('engine', 'beta-engine')
            
        return url
    
    def _make_url(self, post):
        """Merge base url and post"""   
        return self._base_url + post