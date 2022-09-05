import pandas as pd

from pyETM.logger import get_modulelogger

# get modulelogger
logger = get_modulelogger(__name__)


class Properties:
    
    def __overview(self, include_unattached: bool = False, 
            include_internal: bool = False) -> pd.DataFrame:
        """fetch custom curve descriptives"""
        
        # raise without scenario id
        self._raise_scenario_id()

        # # lower case booleans for params
        # include_unattached = str(include_unattached).lower()
        # include_internal = str(include_internal).lower()
        
        params = {}
        
        if include_unattached:
            include_unattached = str(bool(include_unattached))
            params['include_unattached'] = include_unattached.lower()
        
        if include_internal:
            include_internal = str(bool(include_internal))
            params['include_internal'] = include_internal.lower()
        
        # request repsonse 
        url = f'scenarios/{self.scenario_id}/custom_curves'
        resp = self.session.get(url, params=params)

        # check for response
        if bool(resp) == True:

            # convert response to frame
            ccurves = pd.DataFrame.from_records(resp, index="key")

            # format datetime column
            if "date" in ccurves.columns:
                ccurves = self._format_datetime(ccurves)
            
        else:

            # return empty frame
            ccurves = pd.DataFrame()

        return ccurves
            
    def get_custom_curve_keys(self, include_unattached: bool = False, 
            include_internal: bool = False) -> pd.Index:
        """get all custom curve keys"""
        
        # subset keys
        params = include_unattached, include_internal
        keys = self.__overview(*params).copy()

        return pd.Index(keys.index, name='ccurve_keys')
        
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
        if keys.empty:
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
