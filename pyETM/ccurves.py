import json
import numpy
import pandas
import aiohttp


class CCurves:
    """class object access ccurve attributes"""
    
    @property
    def info(self):
        """show info on ccurves"""
        
        # get info
        if self._info is None:
            self._format_for_info()
        
        return self._info
        
    @property
    def stats(self):
        """show statistics of attached ccurves"""
        
        # get stats
        if self._stats is None:
            self._format_for_stats()
        
        return self._stats
    
    @property
    def statistics(self):
        """alias for stats"""
        return self.stats
        
    @property
    def overrides(self):
        """show mapping of sliders that are overriden by ccurves"""
        
        # get overrides
        if self._overrides is None:
            self._format_for_overrides()
            
        return self._overrides
    
    @property
    def attached(self):
        """show which ccurves are (un)attached"""    
        return self.info.attached
    
    def __init__(self, ccurves):
        """store formatted ccurves response"""
        
        # set defaults
        self._info = None
        self._stats = None
        self._overrides = None
        
        # convert to dataframe and set index
        ccurves = pandas.DataFrame(ccurves)
        ccurves = ccurves.set_index('key')
        
        # set ccurves
        self._ccurves = ccurves
    
    def _format_datetime(self, ccurves):
        """format datetime"""
        
        # format datetime
        dtype = 'datetime64[ns, UTC]'
        ccurves.date = ccurves.date.astype(dtype)

        # rename column
        cols = {'date': 'datetime'}
        ccurves = ccurves.rename(columns=cols)

        return ccurves
    
    def _format_for_info(self):
        """create info format"""
        
        # reference unformatted ccurves
        ccurves = self._ccurves

        # reformat datetime
        if 'date' in ccurves.columns:
            ccurves = self._format_datetime(ccurves)

        # reformat overrides
        ccurves.overrides = ccurves.overrides.apply(len)
        
        """
        TO DO:
        ------
        Check what source scenario is and if it returns a scenarioID or
        templateID which can be used to reference the source_scenario.
        """
        
        # drop messy stats column
        if 'stats' in ccurves.columns:
            ccurves = ccurves[ccurves.columns.drop('stats')]
        
        # set info
        self._info = ccurves
        
        return ccurves        
    
    def _format_for_stats(self):
        """create stats format"""
        
        ccurves = self._ccurves

        stats = ccurves[ccurves.attached].get('stats', {})
        stats = pandas.DataFrame.from_dict(dict(stats), orient='index')
        
        # set stats
        self._stats = stats
        
        return stats
    
    def _format_for_overrides(self):
        """create overrides format"""
        
        # reference unformatted ccurves
        ccurves = self._ccurves
        
        # subset and explode overrides
        cols = ['overrides', 'attached']
        overrides = ccurves[cols].explode('overrides')
        overrides = overrides.dropna()
        
        # reset index
        overrides = overrides.reset_index()
        overrides.columns = ['ccurve', 'slider', 'active']
        
        # set overrides
        self._overrides = overrides
        
        return overrides.set_index('slider')


class CustomCurves:
    
    @property
    def ccurves(self):
        """request ccurve object"""
        
        # get custom curves
        if self._ccurves is None:
            self.get_ccurves()
            
        return self._ccurves
    
    @property
    def custom_curves(self):
        """alias for ccurves"""
        return self.ccurves
        
    @ccurves.setter
    def ccurves(self, ccurves):
        """set ccurves"""
        
        if ccurves is None:
            self.delete_ccurves()
        
        elif isinstance(ccurves, pandas.Series):
            if ccurves.name is not None:
                self.upload_ccurve(ccurves, ccurves.name)
            else:
                msg = 'cannot set a ccurve from series without key as name'
                raise KeyError(msg)
                
        elif isinstance(ccurves, pandas.DataFrame):
            self.upload_ccurves(ccurves)
            
        else:
            raise TypeError('ccurves must be series, dataframe or None')
            
    @custom_curves.setter
    def custom_curves(self, ccurves):
        """alias for ccurves setter"""
        self.ccurves = ccurves
    
    def get_ccurves(self):
        """get ccurves infomation"""
        
        # raise without scenario id
        self._raise_scenario_id()
                
        # prepare post
        headers = {'Connection': 'close'}
        params = {'include_unattached': 'true'}
        post = f'/scenarios/{self.scenario_id}/custom_curves'
        
        # request repsonse and convert to df
        resp = self.get(post, headers=headers, params=params)
        ccurves = json.loads(resp)
        
        # set ccurves
        self._ccurves = CCurves(ccurves)
        
        return self.ccurves.info    
    
    def get_custom_curves(self):
        """alias for get ccurves"""
        return self.get_ccurves()
    
    def get_ccurve(self, key):
        """return specific custum_curve"""
        
        # validate key
        key = self._validate_key(key)
        
        return self.ccurves.info.loc[key]
    
    def get_custom_curve(self, key):
        """alias for get ccurve"""
        return self.get_ccurve(key)
    
    def upload_ccurve(self, curve, key, name=None):
        """upload custom curve"""
                
        # raise without scenario id
        self._raise_scenario_id()
    
        # validate key
        key = self._validate_key(key)
    
        # delete specified ccurve
        if curve is None:
            self.delete_ccurve(key)
    
        # check ccurve
        curve = self._check_ccurve(curve, key)
        
        # set default name
        if name is None:
            name = curve.name
        
        # convert date to string
        curve = curve.to_string(index=False)

        # convert to form
        form = aiohttp.FormData()
        form.add_field('file', curve, filename=name)
        
        # prepare request
        headers = {'Connection': 'close'}
        post = f'/scenarios/{self.scenario_id}/custom_curves/{key}'
        
        # make request
        self.put(post, data=form, headers=headers)
        
        # reset session
        self._reset_session()
    
    def upload_custom_curve(self, curve, key, name=None):
        """alias for upload ccurve"""
        self.upload_ccurve(curve, key, name)
    
    def upload_ccurves(self, ccurves, names=None):
        """upload multiple ccurves at once"""
        
        # delete all ccurves
        if ccurves is None:
            self.delete_ccurves()
        
        # default to None
        if names is None:
            names = [None for _ in ccurves.columns]
        
        # upload ccurves
        for nr, key in enumerate(ccurves.columns):
            self.upload_ccurve(ccurves[key], key, names[nr])
    
    def upload_custom_curves(self, ccurves, names=None):
        """alias for upload ccurves"""
        self.upload_ccurves(ccurves, names)
    
    def delete_ccurve(self, key):
        """delate an uploaded ccurve"""
        
        # raise without scenario id
        self._raise_scenario_id()
        
        # validate key
        key = self._validate_key(key)
        
        # prepare request
        headers = {'Connection': 'close'}
        post = f'/scenarios/{self.scenario_id}/custom_curves/{key}'
        
        # make request
        self.delete(post, headers=headers)
        
        # reset session
        self._reset_session()
        
    def delete_custom_curve(self, key):
        """alias for delete ccurve"""
        self.delete_ccurve(key)
        
    def delete_ccurves(self):
        """delete all custom curves"""
        
        attached = self.ccurves.attached
        for key, active in attached.iteritems():
            if active is True:
                self.delete_ccurve(key)
                    
    def delete_custom_curves(self):
        """alias for delete ccurves"""
        self.delete_ccurves()
        
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
        
        # check if key in ccurve index
        if key not in self.ccurves.info.index:
            raise KeyError(f'"{key}" is not a valid ccurve key')
            
        return key