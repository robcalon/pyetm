from __future__ import annotations

import logging
import pandas as pd

from collections.abc import Iterable

logger = logging.getLogger(__name__)


class Curves:
        
    @property
    def custom_curves(self) -> pd.DataFrame:
        """fetch custom curves"""
        
        # get custom curves
        if self._ccurves is None:
            self.get_custom_curves()
                    
        return self._ccurves
        
    @custom_curves.setter
    def custom_curves(self, ccurves: pd.DataFrame) -> None:
        """set custom curves without option to set a name"""

        # check for old custom curves
        keys = self.get_custom_curve_keys()
        if not keys.empty:
        
            # remove old custom curves
            self.delete_custom_curves()
        
        # set single custom curve
        if isinstance(ccurves, pd.Series):

            if ccurves.name is not None:
                self.upload_custom_curve(ccurves, ccurves.name)

            else:
                raise KeyError("passed custom curve has no name")

        elif isinstance(ccurves, pd.DataFrame):
            self.upload_custom_curves(ccurves)

        else:
            raise TypeError("custom curves must be a series, frame or None")
            
    def __delete_ccurve(self, key: str) -> None:
        """delete without raising or resetting"""
        
        # validate key
        key = self._validate_ccurve_key(key)
        
        # prepare request
        headers = {'Connection': 'close'}
        url = f'/scenarios/{self.scenario_id}/custom_curves/{key}'
        
        # make request
        self.delete(url, headers=headers)
        
    def _format_datetime(self, ccurves: pd.DataFrame) -> pd.DataFrame:
        """format datetime"""
        
        # format datetime
        dtype = 'datetime64[ns, UTC]'
        ccurves.date = ccurves.date.astype(dtype)

        # rename column
        cols = {'date': 'datetime'}
        ccurves = ccurves.rename(columns=cols)

        return ccurves  
    
    def __get_ccurve(self, key: str) -> pd.Series:
        """get custom curve"""
                
        # validate key
        key = self._validate_ccurve_key(key)
            
        # prepare request
        headers = {"Connection": "close"}
        url = f'/scenarios/{self.scenario_id}/custom_curves/{key}'
        
        # request response and convert to series 
        resp = self.get(url, headers=headers, decoder='BytesIO')
        curve = pd.read_csv(resp, header=None, names=[key]).squeeze('columns')
        
        return curve
        
    def __upload_ccurve(self, curve: pd.Series, key: str | None = None, 
            name: str | None = None) -> None:
        """upload without raising or resetting"""
        
        # resolve None
        if key is None:
            
            # check series object
            if isinstance(curve, pd.Series):
                
                # use series name
                if curve.name is not None:
                    key = curve.name
                        
        # validate key
        key = self._validate_ccurve_key(key)
    
        # delete specified ccurve
        if curve is None:
            self.delete_custom_curve(key)
        
        # check ccurve
        curve = self._check_ccurve(curve, key)
        
        # prepare request
        headers = {'Connection': 'close'}
        url = f'/scenarios/{self.scenario_id}/custom_curves/{key}'
        
        # make request
        self.upload_series(url, curve, name=name, headers=headers)
        
    def _check_ccurve(self, curve: pd.Series, key: str) -> pd.Series:
        """check if a ccurve is compatible"""
          
        # subset columns from frame
        if isinstance(curve, pd.DataFrame):
            curve = curve[key]
            
        # assume list-like
        if not isinstance(curve, pd.Series):
            curve = pd.Series(curve, name=key)
        
        # check length
        if not len(curve) == 8760:
            raise ValueError("curve must contain 8760 entries")
        
        return curve
    
    def _validate_ccurve_key(self, key: str) -> str:
        """check if key is valid ccurve"""
        
        # raise for None
        if key is None:
            raise KeyError("No key specified for custom curve")
        
        # check if key in ccurve index
        params = {'include_unattached': True, 'include_internal': True}
        if key not in self.get_custom_curve_keys(**params):
            raise KeyError(f"'{key}' is not a valid custom curve key")
            
        return key
        
    def delete_custom_curve(self, key: str) -> None:
        """delate an uploaded ccurve"""
        
        # raise without scenario id
        self._raise_scenario_id()
                
        # if key is attached
        if key in self.get_custom_curve_keys():
                        
            # delete ccurve and reset
            self.__delete_ccurve(key)
            self._reset_session()

        else:
            # warn user for attempt
            msg = (f"%s: attempted to remove '{key}', " +
                   "while curve already unattached") %self
            logger.warning(msg)
        
    def delete_custom_curves(self, 
            keys: Iterable[str] | None = None) -> None:
        """delete all custom curves"""
        
        # raise without scenario id
        self._raise_scenario_id()

        # default keys        
        if keys is None:
            keys = []

        # convert iterable
        if not isinstance(keys, pd.Index):
            keys = pd.Index(keys)

        # get keys that need deleting
        attached = self.get_custom_curve_keys()

        # default keys
        if keys.empty:
            keys = attached

        else:
            # subset attached keys
            keys = keys[keys.isin(attached)]

        # check validity
        if (not attached.empty) & (not keys.empty):
        
            # delete all ccurves
            function = self.__delete_ccurve
            [function(key) for key in keys]
            
            # reset session
            self._reset_session()
        
        else:
            # warn user for attempt
            msg = ("%s: attempted to remove custom curves, " +
                   "without any (specified) custom curves attached") %self
            logger.warning(msg)

    def get_custom_curve(self, key: str):
        """return specific custom curve"""
        key = self._validate_ccurve_key(key)
        return self.custom_curves[key]
        
    def get_custom_curves(self, 
            keys: Iterable[str] | None = None) -> pd.DataFrame:
        """return all attached curstom curves"""
        
        # raise without scenario id
        self._raise_scenario_id()
        
        # default keys        
        if keys is None:
            keys = []

        # convert iterable
        if not isinstance(keys, pd.Index):
            keys = pd.Index(keys)
            
        # get keys that need deleting
        attached = self.get_custom_curve_keys()

        # default keys
        if keys.empty:
            keys = attached

        else:
            # subset attached keys
            keys = keys[keys.isin(attached)]

        # check validity
        if (not attached.empty):
            
            # get attached curves
            func = self.__get_ccurve
            ccurves =  pd.concat([func(key) for key in attached], axis=1)
            
        else:
            
            # empty dataframe
            ccurves = pd.DataFrame()
        
        # assign ccurves
        self._ccurves = ccurves
        
        return ccurves[keys]
             
    def upload_custom_curve(self, curve: pd.Series, key: str | None = None, 
            name: str | None = None) -> None:
        """upload custom curve"""
                
        # raise without scenario id
        self._raise_scenario_id()
        
        # upload ccurve
        self.__upload_ccurve(curve, key, name)
                
        # reset session
        self._reset_session()
    
    def upload_custom_curves(self, ccurves: pd.DataFrame, 
            names: list[str] | None = None) -> None:
        """upload multiple ccurves at once"""
                
        # raise without scenario id
        self._raise_scenario_id()
        
        # delete all ccurves
        if ccurves is None:
            self.delete_custom_curves()
        
        # list of Nones
        if names is None:
            names = [None for _ in ccurves.columns]

        # convert single to list
        if isinstance(names, str):
            names = [names for _ in ccurves.columns]

        # raise for errors
        if len(names) != len(ccurves.columns):
            raise ValueError('number of names does not match number of curves')

        # upload all ccurves to ETM
        for nr, key in enumerate(ccurves.columns):
            self.__upload_ccurve(ccurves[key], key, name=names[nr])
                
        # reset session
        self._reset_session()
