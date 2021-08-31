import json
import numpy
import pandas
import aiohttp
import warnings

class CCurves:
    """class object access ccurve attributes"""
    
    @property
    def overview(self):
        """show overview on ccurves"""
        
        # get overview
        if self._overview is None:
            self._format_for_overview()
        
        return self._overview
        
    @property
    def stats(self):
        """show statistics of attached ccurves"""
        
        # get stats
        if self._stats is None:
            self._format_for_stats()
        
        return self._stats
    
    @property
    def statistics(self):
        """alias for stats method"""
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
        return self.overview.attached
    
    def __call__(self):
        """return overview on call"""
        return self.overview
    
    def __init__(self, ccurves):
        """store formatted ccurves response"""
        
        # set defaults
        self._overview = None
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
    
    def _format_for_overview(self):
        """create overview format"""
        
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
        
        # set overview
        self._overview = ccurves
        
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
        
    @ccurves.setter
    def ccurves(self, ccurves):
        """set ccurves witout option to set a name"""
        
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
    
    def get_ccurves(self):
        """get ccurves overview"""
        
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
        
        return self.ccurves.overview    
    
    def get_ccurve(self, key):
        """return specific custum_curve"""
        
        # validate key
        key = self._validate_key(key)
        
        return self.ccurves.overview.loc[key]
    
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
            self.delete_ccurve(key)
    
        # set key as name
        if name is None:
            name = key
    
        # check ccurve
        curve = self._check_ccurve(curve, key)
                
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
    
    def upload_ccurve(self, curve, key=None, name=None):
        """upload custom curve"""
                
        # raise without scenario id
        self._raise_scenario_id()
        
        # upload ccurve
        self.__upload_ccurve(curve, key, name)
                
        # reset session
        self._reset_session()
    
    def upload_custom_curve(self, curve, key, name=None):
        """alias for upload ccurve"""
        self.upload_ccurve(curve, key, name)
    
    def upload_ccurves(self, ccurves, name=None):
        """upload multiple ccurves at once"""
                
        # raise without scenario id
        self._raise_scenario_id()
        
        # delete all ccurves
        if ccurves is None:
            self.delete_ccurves()
        
        """
        TO DO:
        ------
        Consider to use gather and make this async as well.
        """
        
        # upload all ccurves to ETM
        function = self.__upload_ccurve
        [function(ccurves[key], key, name) for key in ccurves.columns]
                
        # reset session
        self._reset_session()
    
    def __delete_ccurve(self, key):
        """delete without raising or resetting"""
        
        # validate key
        key = self._validate_key(key)
        
        # prepare request
        headers = {'Connection': 'close'}
        post = f'/scenarios/{self.scenario_id}/custom_curves/{key}'
        
        # make request
        self.delete(post, headers=headers)
        
    def delete_ccurve(self, key):
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
            warnings.warn(msg)
        
    def delete_ccurves(self):
        """delete all custom curves"""
        
        # raise without scenario id
        self._raise_scenario_id()
        
        # check if key is attached
        ccurves = self.ccurves()
        attached = ccurves.attached[ccurves.attached]
        
        """
        TO DO:
        ------
        Consider to use gather and make this async as well.
        """
        
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
            warnings.warn(msg)
                
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
        if key not in self.ccurves.overview.index:
            raise KeyError(f'"{key}" is not a valid ccurve key')
            
        return key