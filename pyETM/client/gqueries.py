import functools
import pandas as pd


class GQueries:
    
    @property
    def gqueries(self) -> pd.Index:
        """returns a list of set gqueries"""
        return self._gqueries

    @gqueries.setter
    def gqueries(self, gqueries: list) -> None:
        """sets gqueries list"""

        # put string in list
        if isinstance(gqueries, str):
            gqueries = [gqueries]
            
        if not isinstance(gqueries, list):
            gqueries = list(gqueries)
        
        # set gqueries
        self._gqueries = gqueries
        
        # reset gquery results
        self.get_gquery_results.cache_clear()
        
    @property
    def gquery_results(self):
        """returns results for all set gqueries"""
        return self.get_gquery_results()
    
    @property
    def gquery_curves(self):
        """return a subset of gquery results that are curves"""
        
        # subset curves from gquery_results
        gqueries = self.gquery_results
        gqueries = gqueries[gqueries.unit == 'curve']
        
        # subset future column and convert to series
        gqueries =  gqueries.future.apply(pd.Series)
    
        return gqueries.T
        
    @property
    def gquery_deltas(self):
        """returns a subset of gquery_results that are not curves"""
        
        # subset deltas from gquery_results
        gqueries = self.gquery_results
        gqueries = gqueries[gqueries.unit != 'curve']
        
        return gqueries
            
    def get_gquery_results_for_gqueries(self, gqueries):
        """Request gqueries from ETM"""
        
        # update gqueries
        self.gqueries = gqueries
        
        return self.gquery_results
        
    @functools.lru_cache
    def get_gquery_results(self):
        """get data for queried graphs from ETM"""
                
        # raise without scenario id
        self._raise_scenario_id()
                
        # create gquery request
        data = {'gqueries': self.gqueries}
        url = f'scenarios/{self.scenario_id}'
        
        # evaluate post
        response = self.session.put(url, json=data)
        
        # transform into dataframe
        records = response['gqueries']
        gquery_results = pd.DataFrame.from_dict(records, orient='index')
        
        return gquery_results
