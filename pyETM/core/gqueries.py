import pandas


class GQueries:
    
    @property
    def gqueries(self):
        """returns a list of set gqueries"""
        return self._gqueries

    @gqueries.setter
    def gqueries(self, gqueries):
        """sets gqueries list"""

        # put string in list
        if isinstance(gqueries, str):
            gqueries = [gqueries]
        
        # set gqueries
        self._gqueries = gqueries
        
        # reset gquery results
        self._gquery_results = None
        
    @property
    def gquery_results(self):
        """returns results for all set gqueries"""
        
        # get gquery results
        if self._gquery_results is None:
            self._get_gquery_results()
            
        return self._gquery_results
    
    @property
    def gquery_curves(self):
        """return a subset of gquery results that are curves"""
        
        # subset curves from gquery_results
        gqueries = self.gquery_results
        gqueries = gqueries[gqueries.unit == 'curve']
        
        # subset future column and convert to series
        gqueries =  gqueries.future.apply(pandas.Series)
    
        return gqueries.T
        
    @property
    def gquery_deltas(self):
        """returns a subset of gquery_results that are not curves"""
        
        # subset deltas from gquery_results
        gqueries = self.gquery_results
        gqueries = gqueries[gqueries.unit != 'curve']
        
        return gqueries
            
    def get_gquery_results(self, gqueries):
        """Request gqueries from ETM"""
        
        # update gqueries
        self.gqueries = gqueries
        
        return self.gquery_results
        
    def _get_gquery_results(self, **kwargs):
        """get data for queried graphs from ETM"""
                
        # raise without scenario id
        self._raise_scenario_id()
                
        # create gquery request
        data = {'detailed': True, 'gqueries': self.gqueries}
        
        # prepare post
        headers = {'Connection': 'close'}
        post = f'/scenarios/{self.scenario_id}'
        
        # evaluate post
        response = self.put(post, json=data, headers=headers, **kwargs)
        gquery_results = pandas.DataFrame.from_dict(response['gqueries'], orient='index')
        
        # set gquery results
        self._gquery_results = gquery_results