import logging
import pandas as pd

logger = logging.getLogger(__name__)


class Curves:
        
    @property
    def custom_curves(self):
        """fetch custom curves"""
        
        # get custom curves
        if self._ccurves is None:
            self.get_custom_curves()
                    
        return self._ccurves
        
    @custom_curves.setter
    def custom_curves(self, ccurves):
        """set custom curves without option to set a name"""

        # check for old custom curves
        if self.get_custom_curve_keys():
        
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
    
    @property
    def validate_ccurves(self):
        """validate ccurves key before uploading"""
        return self._validate_ccurves
    
    @validate_ccurves.setter
    def validate_ccurves(self, boolean):
        """set validate ccurves boolean"""
        
        if not isinstance(boolean, bool):
            raise TypeError("'validate_ccurves' must be of type boolean")
            
        self._validate_ccurves = boolean
        
    def __delete_ccurve(self, key):
        """delete without raising or resetting"""
        
        # validate key
        key = self._validate_ccurve_key(key)
        
        # prepare request
        headers = {'Connection': 'close'}
        url = f'/scenarios/{self.scenario_id}/custom_curves/{key}'
        
        # make request
        self.delete(url, headers=headers)
        
    def _format_datetime(self, ccurves):
        """format datetime"""
        
        # format datetime
        dtype = 'datetime64[ns, UTC]'
        ccurves.date = ccurves.date.astype(dtype)

        # rename column
        cols = {'date': 'datetime'}
        ccurves = ccurves.rename(columns=cols)

        return ccurves  
    
    def __get_ccurve(self, key):
        """get custom curve"""
                
        # validate key
        key = self._validate_ccurve_key(key)
            
        # prepare request
        headers = {"Connection": "close"}
        url = f'/scenarios/{self.scenario_id}/custom_curves/{key}'
        
        # request response and convert to series 
        resp = self.get(url, headers=headers, decoder='bytes')
        curve = pd.read_csv(resp, header=None, names=[key]).squeeze('columns')
        
        return curve
        
    def __upload_ccurve(self, curve, key=None, name=None):
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
        
    def _check_ccurve(self, curve, key):
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
    
    def _validate_ccurve_key(self, key):
        """check if key is valid ccurve"""
        
        # raise for None
        if key is None:
            raise KeyError("No key specified for custom curve")
        
        # check if key in ccurve index
        if key not in self.get_custom_curve_keys(include_unattached=True):
            
            # only raise when validation is requested
            if self.validate_ccurves:
                raise KeyError(f"'{key}' is not a valid custom curve key")
            
        return key
        
    def delete_custom_curve(self, key):
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
        
    def delete_custom_curves(self, keys=None):
        """delete all custom curves"""
        
        # raise without scenario id
        self._raise_scenario_id()
        
        # get keys that need deleting
        attached = self.get_custom_curve_keys()
        
        # default keys
        if keys is None:
            keys = attached

        else:
            # subset attached keys
            keys = [key for key in keys if key in attached]

        if bool(attached) & bool(keys):
        
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

    def get_custom_curve(self, key):
        """return specific custom curve"""
        key = self._validate_ccurve_key(key)
        return self.custom_curves[key]
        
    def get_custom_curves(self):
        """return all attached curstom curves"""
        
        # raise without scenario id
        self._raise_scenario_id()
        
        # iterate over attached keys
        keys = self.get_custom_curve_keys()
        
        # check attachment
        if keys:
            
            # get attached curves
            func = self.__get_ccurve
            ccurves =  pd.concat([func(key) for key in keys], axis=1)
            
        else:
            
            # empty dataframe
            ccurves = pd.DataFrame()
        
        # assign ccurves
        self._ccurves = ccurves
        
        return ccurves
             
    def upload_custom_curve(self, curve, key=None, name=None):
        """upload custom curve"""
                
        # raise without scenario id
        self._raise_scenario_id()
        
        # upload ccurve
        self.__upload_ccurve(curve, key, name)
                
        # reset session
        self._reset_session()
    
    def upload_custom_curves(self, ccurves, name=None):
        """upload multiple ccurves at once"""
                
        # raise without scenario id
        self._raise_scenario_id()
        
        # delete all ccurves
        if ccurves is None:
            self.delete_custom_curves()
        
        # upload all ccurves to ETM
        function = self.__upload_ccurve
        [function(ccurves[key], key, name) for key in ccurves.columns]
                
        # reset session
        self._reset_session()
