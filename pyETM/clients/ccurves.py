import logging
import numpy
import pandas

logger = logging.getLogger(__name__)


class CCurves:
    """class object access ccurve attributes"""
    
    @property
    def info(self):
        """show overview on ccurves"""
        
        """
        TO DO:
        ------
        Check what source scenario is and if it returns a scenarioID or
        templateID which can be used to reference the source_scenario.
        """
        
        # reformat overrides
        ccurves = self.__ccurves.copy(deep=True)
        ccurves.overrides = ccurves.overrides.apply(len)        
        
        # drop messy stats column
        if 'stats' in ccurves.columns:
            ccurves = ccurves[ccurves.columns.drop('stats')]
                
        return ccurves        
        
    @property
    def stats(self):
        """show statistics of attached ccurves"""
        
        # subset active ccurves
        ccurves = self.__ccurves[self.__ccurves.attached]
        
        if ccurves.empty:
            return pandas.DataFrame()
        
        # get values and index from ccurves
        data, index = ccurves.stats, ccurves.index
        
        return pandas.DataFrame.from_records(data, index=index)
        
    @property
    def statistics(self):
        """alias for stats method"""
        return self.stats
        
    @property
    def overrides(self):
        """show mapping of sliders that are overriden by ccurves"""
        
        # subset and explode overrides
        cols = ['overrides', 'attached']
        overrides = self.__ccurves[cols].explode('overrides')
        overrides = overrides.dropna()
        
        # reset index
        overrides = overrides.reset_index()
        overrides.columns = ['ccurve', 'key', 'active']
        
        return overrides.set_index('key')
    
    @property
    def active_overrides(self):
        """show active overrides"""
        return self.overrides.ccurve[self.overrides.active]
    
    @property
    def attached(self):
        """show which ccurves are (un)attached"""    
        return self.info.attached
        
    def __init__(self, ccurves):
        """store formatted ccurves response"""
        
        # set ccurves
        self.__ccurves = ccurves

    def __call__(self):
        """return overview on call"""
        return self.info
        
        
class CustomCurves:
    
    @property
    def ccurves(self):
        """request ccurve object"""
        
        # get custom curves
        if self._ccurves is None:
            self.get_custom_curves()
            
        return self._ccurves
        
    @ccurves.setter
    def ccurves(self, ccurves):
        """set ccurves witout option to set a name"""
        
        if ccurves is None:
            self.delete_custom_curves()
        
        elif isinstance(ccurves, pandas.Series):
            
            if ccurves.name is not None:
                self.upload_custom_curve(ccurves, ccurves.name)
            
            else:
                msg = 'cannot set a ccurve from series without key as name'
                raise KeyError(msg)
                
        elif isinstance(ccurves, pandas.DataFrame):
            self.upload_custom_curves(ccurves)
            
        else:
            raise TypeError('ccurves must be series, dataframe or None')
    
    @property
    def validate_ccurves(self):
        """validate ccurves key before uploading"""
        return self._validate_ccurves
    
    @validate_ccurves.setter
    def validate_ccurves(self, boolean):
        """set validate ccurves boolean"""
        
        if not isinstance(boolean, bool):
            raise TypeError('"validate_ccurves" must be of type boolean')
            
        self._validate_ccurves = boolean
        
    def __delete_ccurve(self, key):
        """delete without raising or resetting"""
        
        # validate key
        key = self._validate_key(key)
        
        # prepare request
        headers = {'Connection': 'close'}
        url = f'/scenarios/{self.scenario_id}/custom_curves/{key}'
        
        # make request
        self.delete(url, headers=headers)
        
    def __format_datetime(self, ccurves):
        """format datetime"""
        
        # format datetime
        dtype = 'datetime64[ns, UTC]'
        ccurves.date = ccurves.date.astype(dtype)

        # rename column
        cols = {'date': 'datetime'}
        ccurves = ccurves.rename(columns=cols)

        return ccurves  
        
    def __upload_ccurve(self, curve, key=None, name=None):
        """upload without raising or resetting"""
        
        # default to series name
        if key is None:
            if isinstance(curve, pandas.Series):
                if curve.name is not None:
                    key = curve.name
                        
        # validate key
        key = self._validate_key(key)
    
        # delete specified ccurve
        if curve is None:
            self.delete_custom_curve(key)
        
        # check ccurve
        curve = self._check_ccurve(curve, key)
        
        # prepare request
        headers = {'Connection': 'close'}
        url = f'/scenarios/{self.scenario_id}/custom_curves/{key}'
        
        # make request
        self.put_series(url, curve, name=name, headers=headers)
        
    def _check_ccurve(self, curve, key):
        """check if a ccurve is compatible"""
                
        # list to series
        if isinstance(curve, list):
            curve = pandas.Series(curve, name=key)

        # ndarray to series
        if isinstance(curve, numpy.ndarray):
            curve = pandas.Series(curve, name=key)
            
        # dataframe to series
        if isinstance(curve, pandas.DataFrame):
            curve = curve[key]
        
        # check length
        if not len(curve) == 8760:
            raise ValueError('curve must contain 8760 entries')
        
        return curve
    
    def _validate_key(self, key):
        """check if key is valid ccurve"""
        
        # raise for None
        if key is None:
            raise KeyError('No key specified for ccurve')
        
        # check if key in ccurve index
        if key not in self.ccurves().index:
            
            # only raise when validation is requested
            if self.validate_ccurves is True:
                raise KeyError(f'"{key}" is not a valid ccurve key')
            
        return key
    
    def delete_custom_curve(self, key):
        """delate an uploaded ccurve"""
        
        # raise without scenario id
        self._raise_scenario_id()
        
        # check if key is attached
        ccurves = self.ccurves()
        attached = ccurves.attached[ccurves.attached]
        
        # if key is attached
        if attached.loc[key]:
            
            # delete ccurve and reset
            self.__delete_ccurve(key)
            self._reset_session()

        else:
            # warn user for attempt
            msg = (f'attempted to unattach "{key}", ' +
                   'while already unattached')
            logger.warning(msg)
        
    def delete_custom_curves(self):
        """delete all custom curves"""
        
        # raise without scenario id
        self._raise_scenario_id()
        
        # check if key is attached
        ccurves = self.ccurves()
        attached = ccurves.attached[ccurves.attached]
        
        # if key is attached
        if attached.shape[0] > 0:

            # delete all ccurves
            function = self.__delete_ccurve
            [function(key) for key in attached.index]
            
            # reset session
            self._reset_session()
        
        else:
            # warn user for attempt
            msg = ('attempted to unattach all ccurves, ' +
                   'while already unattached')
            logger.warning(msg)

    def get_custom_curve(self, key):
        """return specific custum_curve"""        
        return self.ccurves().loc[self._validate_key(key)]
            
    def get_custom_curves(self):
        """get ccurves overview"""
        
        # raise without scenario id
        self._raise_scenario_id()
                
        # prepare request
        headers = {'Connection': 'close'}
        params = {'include_unattached': 'true'}
        url = f'/scenarios/{self.scenario_id}/custom_curves'
        
        # request repsonse and convert to df
        resp = self.get(url, headers=headers, params=params)
        ccurves = pandas.DataFrame.from_records(resp, index="key")

        # format datetime column
        if "date" in ccurves.columns:
            ccurves = self.__format_datetime(ccurves)
        
        # set ccurves
        self._ccurves = CCurves(ccurves)
        
        return self.ccurves()    
            
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
