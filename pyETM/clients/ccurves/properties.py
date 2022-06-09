import logging
import pandas as pd

logger = logging.getLogger(__name__)


class Properties:
    
    @property
    def __overview(self):
        """fetch custom curve descriptives"""
        
        # raise without scenario id
        self._raise_scenario_id()
        
        # prepare request
        headers = {'Connection': 'close'}
        params = {'include_unattached': 'true'}
        url = f'/scenarios/{self.scenario_id}/custom_curves'
        
        # request repsonse and convert to df
        resp = self.get(url, headers=headers, params=params)
        ccurves = pd.DataFrame.from_records(resp, index="key")

        # format datetime column
        if "date" in ccurves.columns:
            ccurves = self._format_datetime(ccurves)
            
        return ccurves
            
    def get_custom_curve_keys(self, include_unattached=False):
        """get all custom curve keys"""
        
        # subset keys
        keys = self.__overview.attached
        
        # drop unattached keys
        if not include_unattached:
            keys = keys[keys]
            
        return keys.index.to_list()
        
    def get_custom_curve_settings(self, include_unattached=False):
        """show overview of custom curve settings"""
        
        """
        TO DO:
        ------
        Check what source scenario is and if it returns a scenarioID or
        templateID which can be used to reference the source_scenario.
        """
        
        # return empty frame without ccurves
        keys = self.get_custom_curve_keys()
        if (not keys) & (not include_unattached):
            return pd.DataFrame()
        
        # reformat overrides
        ccurves = self.__overview.copy(deep=True)
        ccurves.overrides = ccurves.overrides.apply(len)        
        
        # drop messy stats column
        if 'stats' in ccurves.columns:
            ccurves = ccurves[ccurves.columns.drop('stats')]
                
        # drop unattached keys
        if not include_unattached:
            ccurves = ccurves.loc[ccurves.attached]
            ccurves = ccurves.drop(columns="attached")
            
        return ccurves
    
    def get_custom_curve_user_value_overrides(self, include_unattached=False):
        """get overrides of user value keys by custom curves"""
        
        # subset and explode overrides
        cols = ['overrides', 'attached']
        overrides = self.__overview[cols].explode('overrides')
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
