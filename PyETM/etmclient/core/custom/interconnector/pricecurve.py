import json
import numpy
import pandas
import aiohttp
import dateutil

class Pricecurve:
        
    @property
    def price(self):

        # get price
        if self._price is None:
            self.get_price()
        
        return self._price
    
    @price.setter
    def price(self, curve):
        
        if curve is None:
            self.delete_price()
        else:
            self.change_price(curve)
        
    @property
    def price_upload_date(self):
        
        # get upload date
        if self._price_upload_date is None:
            self.get_price_upload_date()
            
        return self._price_upload_date
    
    def get_price_upload_date(self):
        """get upload date"""
        
        # abbreviate parent
        parent = self._parent
        
        # get interconnector info when false
        if parent._custom_fetched is False:
            parent.get_interconnector_prices()
        
        # get upload date
        date = parent._interconnectors[self.name]['date']
    
        self._price_upload_date = date
        
        return date
    
    def get_price(self):
        
        # abbreviate parent
        parent = self._parent
        
        # get interconnector info when false
        if parent._custom_fetched is False:
            parent.get_interconnector_prices()
        
        # get price information
        key = self.name + '_price'
        price = parent.interconnector_prices[key]
        
        # set price
        self._price = price
        
        return price
            
    def change_price(self, curve):
        """change pricecurve"""        
      
        # abbreviate parent
        parent = self._parent
        
        # raise if scenario id is None
        parent._raise_scenario_id()
        
        # check price curve
        pricecurve = self._check_pricecurve(curve)
        
        # make form properties
        filename = self.name + '_price.csv'
        curve = curve.to_string(index=False)

        # convert to form-data
        form = aiohttp.FormData()
        form.add_field('file', curve, filename=filename)
            
        # prepare upload post
        ID = parent.scenario_id
        key = self.name + '_price'
        headers = {'Connection': 'close'}
        post = (f'/scenarios/{ID}/custom_curves/{key}')
        
        # upload post
        parent.put(post, data=form, headers=headers)
        
        # reset interconnector
        parent._reset_session()
        
    def delete_price(self):
        """delete an uploaded pricecurve"""
            
        # abbreviate parent
        parent = self._parent
        
        # raise if scenario id is None
        parent._raise_scenario_id()
    
        # check if there's something to delete
        if all(self.price.isna()):
            return None
        
        # build request
        ID = parent.scenario_id
        key = self.name + '_price'
        headers = {'Connection': 'close'}
        post = (f'/scenarios/{ID}/custom_curves/{key}')
        
        # post delete request
        parent.delete(post, headers=headers)
        
        # reset interconnector
        parent._reset_session()
        
    def _check_pricecurve(self, curve):
        """check if a pricecurve is compatible"""
        
        if isinstance(curve, list):
            curve = pandas.Series(curve)
        
        if isinstance(curve, numpy.ndarray):
            curve = pandas.Series(curve)
            
        if isinstance(curve, pandas.DataFrame):
            curve = curve[self.name]
        
        if not len(curve) == 8760:
            raise ValueError('curve must contain 8760 entries')
        
        return curve