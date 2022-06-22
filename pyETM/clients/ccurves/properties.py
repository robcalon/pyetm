import logging
import pandas as pd

logger = logging.getLogger(__name__)


class Properties:
    
    def __overview(self, include_unattached: bool = False, 
            include_internal: bool = False) -> pd.DataFrame:
        """fetch custom curve descriptives"""
        
        # raise without scenario id
        self._raise_scenario_id()

        params = {}
        
        if include_unattached:
            params['include_unattached'] = include_unattached
        
        if include_internal:
            params['include_internal'] = include_internal
        
        # prepare request
        headers = {'Connection': 'close'}
        url = f'/scenarios/{self.scenario_id}/custom_curves'
        
        # request repsonse and convert to df
        resp = self.get(url, headers=headers, params=params)
        ccurves = pd.DataFrame.from_records(resp, index="key")

        # format datetime column
        if "date" in ccurves.columns:
            ccurves = self._format_datetime(ccurves)
            
        return ccurves
            
    def get_custom_curve_keys(self, include_unattached: bool = False, 
            include_internal: bool = False) -> list[str]:
        """get all custom curve keys"""
        
        # subset keys
        params = include_unattached, include_internal
        keys = self.__overview(*params).copy()

        return keys.index.to_list()
        
    def get_custom_curve_settings(self, include_unattached: bool = False, 
            include_internal: bool = False) -> pd.DataFrame:
        """show overview of custom curve settings"""
        
        """
        TO DO:
        ------
        Check what source scenario is and if it returns a scenarioID or
        templateID which can be used to reference the source_scenario.
        """
        
        # get relevant keys
        params = include_unattached, include_internal
        keys = self.get_custom_curve_keys(*params)

        # empty frame without returned keys
        if not keys:
            return pd.DataFrame()
        
        # reformat overrides
        ccurves = self.__overview(*params).copy()
        ccurves.overrides = ccurves.overrides.apply(len)        
        
        # drop messy stats column
        if 'stats' in ccurves.columns:
            ccurves = ccurves[ccurves.columns.drop('stats')]
                
        # drop unattached keys
        if not include_unattached:
            ccurves = ccurves.loc[ccurves.attached]
            ccurves = ccurves.drop(columns="attached")
            
        return ccurves
    
    def get_custom_curve_user_value_overrides(self, 
            include_unattached: bool = False, 
            include_internal: bool = False) -> pd.DataFrame:
        """get overrides of user value keys by custom curves"""
        
        # subset and explode overrides
        cols = ['overrides', 'attached']

        # get overview curves
        params = include_unattached, include_internal
        overview = self.__overview(*params).copy()

        # explode and drop na
        overrides = overview[cols].explode('overrides')
        overrides = overrides.dropna()
        
        # reset index
        overrides = overrides.reset_index()
        overrides.columns = ['overriden_by', 'user_value_key', 'active']
        
        # set index
        overrides = overrides.set_index('user_value_key')
        
        # subset active
        if not include_unattached:
            return overrides.overriden_by[overrides.active]
        
        return overrides
